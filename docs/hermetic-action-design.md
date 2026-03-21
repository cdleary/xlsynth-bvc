# Hermetic Action System Design

## Non-Legacy Policy (Hard Rule)

The active repository state must contain only canonical action variants.

- Do not keep mixed legacy/canonical lineages for the same conceptual workflow.
- If an action variant is superseded, prune the legacy closure and re-materialize from the canonical root.
- Treat historical/legacy variants as migration inputs only, not steady-state data.
- Prefer consistency over preservation: this repo is intentionally re-synthesizable.

## Goals

- Deterministic, reproducible actions keyed by content-addressed action IDs.
- Artifacts materialized inside the git working tree.
- Provenance captured with enough detail to rematerialize outputs.
- Queue-based execution so discovery systems can enqueue work (e.g. newly published xlsynth versions).
- Storage layout that scales to many artifacts.

## Principles

- **Version-resilient data recovery:** when older/newer runtimes produce different encodings for the same conceptual output, prefer a tagged/sum-type output model over failing the workflow.
- **Variant-aware downstream actions:** downstream actions should pattern-match the producer variant and use the correct extraction path for each case.
- **Canonical normalized leaves:** after variant-specific extraction, emit a canonical normalized artifact for queries/trends/diffs.
- **Typed over stringly:** represent variant kind as schema/enum data (not ad hoc string checks) so action IDs/provenance capture the distinction explicitly.
- **No legacy action variants in active state:** see **Non-Legacy Policy (Hard Rule)** above.
- **No URL backward-compatibility guarantees:** web routes are canonicalized to current names; when route naming changes, remove old aliases and update links.
- **Version labels must be explicit in docs/UI/output:** always write `crate:vA.B.C` or `dso:vX.Y.Z`; avoid bare `vX.Y.Z` in prose because it is ambiguous.
- **Queue is ephemeral:** queue directories are transient execution state. Durable terminal failure history lives in sled (`failed_by_action`), not in `queue/`.
- **Deduplicate at enqueue time, not just execution time:** when multiple suggested actions are semantically equivalent, canonicalize to one action ID before enqueue so the queue itself does not explode with duplicates.
- **IR↔AIG equivalence proof path must avoid `ir2g8r`:** equivalence checks must use `aig2ir -> ir-equiv` only. `aig2ir` is a projection into IR space, while `ir2g8r` is the complex lowering pipeline under test and must not appear in the proof path.
- **IR↔AIG equivalence actions are currently quarantined by default:** `DriverIrAigEquiv` suggestion generation/enqueue is disabled unless `BVC_ENABLE_DRIVER_IR_AIG_EQUIV=1` is explicitly set.

Example pattern:

- producer action output becomes a union like `AigOrG8rObj = AigerFile | G8rBinWithStats`.
- downstream stats action consumes either:
  - `AigerFile` via `aig-stats`, or
  - `G8rBinWithStats` via embedded/sidecar stats (and fallback recompute if needed).

## Action Model

Each action is represented as a typed `ActionSpec` and hashed to an `action_id`:

- Hash input: `{schema_version, action_spec}` JSON.
- Hash function: SHA-256.
- Determinism: identical action specs map to identical action IDs.

Current action types:

1. `DownloadAndExtractXlsynthReleaseStdlibTarball(dso:vX.Y.Z) -> DslxFileSubtree`
1. `DownloadAndExtractXlsynthSourceSubtree(dso:vX.Y.Z, subtree=xls/modules/add_dual_path) -> DslxFileSubtree`
1. `DriverDslxFnToIr(dslx_subtree_ref, dslx_file, dslx_fn_name, dso:vX.Y.Z, crate:vA.B.C) -> IrPackageFile`
1. `DriverIrToOpt(ir_ref, top_fn_name?, dso:vX.Y.Z, crate:vA.B.C) -> IrPackageFile`
1. `IrFnToKBoolConeCorpus(ir_ref, top_fn_name?, k, max_ir_ops?, dso:vX.Y.Z, crate:vA.B.C) -> IrPackageFile (multi-fn cone package + manifest)`
1. `DriverIrToDelayInfo(ir_ref, top_fn_name?, delay_model, dso:vX.Y.Z, crate:vA.B.C) -> IrDelayInfoFile (textproto)`
1. `DriverIrEquiv(lhs_ir_ref, rhs_ir_ref, top_fn_name?, dso:vX.Y.Z, crate:vA.B.C) -> EquivReportFile`
1. `DriverIrAigEquiv(ir_ref, aig_ref, top_fn_name?, dso:vX.Y.Z, crate:vA.B.C) -> EquivReportFile`
1. `DriverIrToG8rAig(ir_ref, top_fn_name?, fraig, dso:vX.Y.Z, crate:vA.B.C) -> AigFile`
1. `IrFnToCombinationalVerilog(ir_ref, top_fn_name?, use_system_verilog, dso:vX.Y.Z, crate:vA.B.C) -> VerilogFile`
1. `ComboVerilogToYosysAbcAig(verilog_ref, verilog_top_module_name?, yosys_script_ref) -> AigFile`
1. `DriverAigToStats(aig_ref, dso:vX.Y.Z, crate:vA.B.C) -> AigStatsFile`
1. `AigStatDiff(opt_ir_ref, g8r_aig_stats_ref, yosys_abc_aig_stats_ref) -> AigStatDiffFile`

For CLI flags: `--version` carries the DSO/tool release (`dso:vX.Y.Z`) and `--driver-version`
carries the crate release (`crate:vA.B.C`), or resolves from compatibility data when omitted.

## Artifact Storage Layout (Sled + Materialization Cache)

Root (default): `bvc-artifacts/`

- Durable artifacts/provenance (sled):
  - `provenance_by_action` tree
  - `action_file_bytes` tree
  - `failed_by_action` tree
  - `web_index_bytes` tree
- Materialized read cache (filesystem, disposable):
  - `bvc-artifacts/.materialized-actions/<aa>/<bb>/<action_id>/payload/...`
  - `bvc-artifacts/.materialized-actions/<aa>/<bb>/<action_id>/provenance.json`
- Queue:
  - `bvc-artifacts/queue/pending/<aa>/<bb>/<action_id>.json`
  - `bvc-artifacts/queue/running/<aa>/<bb>/<action_id>.json`
  - `bvc-artifacts/queue/done/<aa>/<bb>/<action_id>.json`
  - `bvc-artifacts/queue/canceled/<aa>/<bb>/<action_id>.json`
`<aa>` and `<bb>` are the leading bytes of `action_id` (hex), which prevents too many entries in one directory as the store grows.

## Provenance

Each materialized artifact directory includes `provenance.json` with:

- action ID
- action spec
- dependency artifact refs
- output artifact ref
- output file manifest (path, size, sha256)
- command traces (docker build/run invocations)
- action details (runtime settings plus explicit `dso_version_label` / `crate_version_label`; `DriverAigToStats` also records source-AIG vs stats-runtime labels separately)
- suggested next actions (derived successor actions + reason)

This data supports replay/rematerialization and auditability.

Suggestion generation currently includes:

- `DownloadAndExtractXlsynthReleaseStdlibTarball -> DriverDslxFnToIr` for each concrete function discovered via `xlsynth-driver dslx-list-fns` over extracted `.x` files
- `DownloadAndExtractXlsynthSourceSubtree -> DriverDslxFnToIr` for each concrete function discovered under the extracted source subtree
- `DriverDslxFnToIr -> DriverIrToOpt`
- `DriverIrToOpt -> DriverIrEquiv` (unoptimized IR vs optimized IR)
- `DriverIrToOpt -> DriverIrToDelayInfo` (canonical `delay_model=asap7`)
- `DriverIrToOpt -> DriverIrToG8rAig` (both `fraig=false` and `fraig=true` variants)
- `DriverIrToOpt -> DriverIrAigEquiv` (for both canonical g8r variants) only when runtime supports both `aig2ir` and `ir-equiv`
- `DriverIrToOpt -> IrFnToCombinationalVerilog` (`use_system_verilog=false` canonical variant)
- `DriverIrToOpt -> IrFnToKBoolConeCorpus(k=3, max_ir_ops=16)` only when the optimized IR output is structurally unique (`output_ir_fn_structural_hash`) among materialized `DriverIrToOpt` actions; the k3 action computes all cones then emits only cone functions with `ir_op_count <= 16`
- `IrFnToKBoolConeCorpus -> DriverIrToG8rAig` (`fraig=false`) for owned k-cone functions in the merged package
- `IrFnToKBoolConeCorpus -> IrFnToCombinationalVerilog` (`use_system_verilog=false`) for owned k-cone functions in the merged package
- `IrFnToCombinationalVerilog -> ComboVerilogToYosysAbcAig` using checked-in default script `flows/yosys_to_aig.ys`
- `DriverIrToG8rAig -> DriverAigToStats`
- `DriverIrToG8rAig -> DriverIrAigEquiv` only when runtime supports both `aig2ir` and `ir-equiv`
- `ComboVerilogToYosysAbcAig -> DriverAigToStats` (when driver context can be inferred from producing IR workflow)
- `DriverAigToStats -> AigStatDiff` by synthesizing the counterpart branch from provenance and joining both stats

For `enqueue-suggested`, `DriverAigToStats` suggestions are canonicalized to the current runtime
policy: use the latest known crate runtime from `generated_version_compat.json` (the "recent
default") whenever newer than the source runtime.

`DriverIrToG8rAig` has two output modes:

- modern runtimes emit AIGER (`--aiger-out`) and, when supported by `ir2gates`, also emit residual PIR after `prep_for_gatify` at `payload/prep_for_gatify.ir` (`ir2gates --prepared-ir-out`)
- modern runtimes: `DriverAigToStats` runs `xlsynth-driver aig-stats` using the canonicalized runtime
- legacy runtimes (`ir2g8r --bin-out`) also emit a sidecar stats JSON (`payload/result.g8r_stats.json`); `DriverAigToStats` consumes that sidecar (or recomputes via `ir2g8r --stats-out` if sidecar is missing)

Semantic reuse currently includes:

- `DriverIrToOpt`, `DriverIrToDelayInfo`, `DriverIrToG8rAig`, and `IrFnToCombinationalVerilog` compute `ir-fn-structural-hash` for the input IR and can reuse a prior payload when:
- `IrFnToKBoolConeCorpus` also computes `ir-fn-structural-hash` for the input IR and can reuse a prior payload when:
  - structural hash matches
  - action parameters match (same effective top/settings)
  - driver runtime/version match
- Reuse events are recorded in provenance details as `semantic_cache_hit`.

## Hermetic Runtime

Driver-based actions run in Docker using Ubuntu 24.04 by default.

Image build (`docker/xlsynth-driver.Dockerfile`):

- installs Rust and a pinned nightly toolchain (`nightly-2026-02-12`) for `xlsynth-driver` build compatibility
- installs `xlsynth-driver` via `cargo +nightly ... install --features with-bitwuzla-system,with-easy-smt`
- installs pinned bitwuzla DSOs from `xlsynth/boolector-build` release tag `bitwuzla-binaries-b29041fbbe6318cb4c19a6e11c7616efc4cb4d32`
- includes vendored `download_release.py` pinned from `xlsynth-crate` tag `v0.29.0`

Before queue draining, the executor preflights pending work:

- builds required driver/yosys images
- prepares a per-version/per-platform release cache at `bvc-artifacts/driver-release-cache/<dso_version>/<platform>/` using vendored `download_release.py`
- fetches delay-info decode protos into that same cache

Cache setup uses a lock file (`.setup.lock`) and ready marker (`.ready.json`) so parallel workers do not stampede the same GitHub assets.

At action execution time, driver containers mount that cache read-only at `/cache`, stage tools into `/tmp/xlsynth-release`, and run with:

- `docker run --pull never`
- `docker run --network none`

This keeps action execution offline after setup and avoids repeated release-asset pulls from GitHub.
Driver action creation validates `crate_version <-> dso_version` compatibility against `third_party/xlsynth-crate/generated_version_compat.json`.
There is no hardcoded default driver crate version in Rust code: actions either use an explicit `--driver-version` or resolve to the latest compatible crate version from `generated_version_compat.json` at action creation time.
Driver action provenance records both labels explicitly as `crate:vX.Y.Z` and `dso:vX.Y.Z`.
Because compatibility is modeled as crate->dso, the DSO label is derivable from a crate label,
but many crate versions may map to the same DSO release; storing both labels keeps provenance and
queries explicit.
`download-stdlib` uses that same compatibility map to resolve a runtime for `dslx-list-fns`-based suggestion generation; failures are recorded in provenance details without failing the stdlib extraction artifact.
`ir-to-delay-info` uses `delay_info_main --proto_out` and decodes the emitted binary `xls.DelayInfoProto` with `protoc` into canonical textual output (`delay_info.textproto`) using schema files cached during setup from the matching `xlsynth/xlsynth` tag for the requested version.

Yosys/ABC actions use a dedicated image (`docker/yosys-abc.Dockerfile`) and take a script reference that is captured as `(path, sha256)` in the action spec; rematerialization re-validates that hash.

The vendored third-party metadata is documented in `third_party/xlsynth-crate/VENDORED.md`.

## Queue/Enqueue Flow

- `enqueue`: writes queue item in `pending/` by action ID (dedup by action ID), with optional explicit `priority` (default `0`).
- `drain-queue`: first preflights runtime dependencies (image builds + release/proto cache fill), then workers claim only dependency-ready items (all dependency action IDs already completed) with an atomic hard-link-based claim from `pending` into `running`, attach a lease record (`lease_owner`, `lease_expires_utc`), execute, then write `done` and remove `running`.
- claim order is: higher explicit queue priority first, then action-kind scheduler priority, then enqueue time.
- suggested descendants do not inherit root priority verbatim anymore: enqueue uses the parent item's explicit priority as a base and adds a small stage bonus derived from action-kind scheduler priority, so lineages closer to `DriverAigToStats` / `AigStatDiff` naturally outrank older upstream work from the same root priority.
- compatible `DriverIrToG8rAig` micro-batching is intentionally conservative: extra claims stay within the same explicit queue priority band and are capped at four total actions per worker so newly-ready stats/diff work is less likely to wait behind already-leased G8r batches.
- expired running leases are reclaimed back to pending before draining (unless disabled).
- each action execution has a default 300-second timeout; timeout failures are recorded as persisted failed-action records (`TIMEOUT(300)`).
- on failure: running item is removed from `running/`, failure details are persisted in sled (`failed_by_action`), and queued downstream actions that depend on that failed action are recursively moved from `pending/` to `canceled/`.

This enables external discovery workflows:

1. poll GitHub releases
1. generate new `ActionSpec`s
1. enqueue unseen action IDs
1. run `drain-queue` worker

The built-in `discover-releases` command does this polling directly and supports `--after vX.Y.Z` so scans can stop once older tags are reached.

## CLI Overview

- `run ...` executes one action immediately (cached by action ID).
- `--artifacts-via-sled <path>` is required and points at the sled-backed artifact store. Queue state is transient filesystem state under `bvc-artifacts/queue`, durable failed-action history is in sled (`failed_by_action`), and provenance/payload bytes are persisted in sled and materialized on demand under `bvc-artifacts/.materialized-actions/`.
- In CLI text output/prose we still label versions explicitly as `crate:...` or `dso:...`; command flags themselves accept raw values (for example `--version v0.35.0`, `--driver-version 0.31.0`).
- For driver-backed `run` commands, `--version` is the DSO release (`dso:vX.Y.Z`) and `--driver-version` is the crate release (`crate:vA.B.C`).
- `run download-source-subtree --version vX.Y.Z --subtree xls/modules/add_dual_path` downloads the tagged `xlsynth/xlsynth` source archive and extracts only that subtree.
- `run ir-to-delay-info --ir-action-id ... [--top-fn-name ...] [--delay-model asap7] --version vX.Y.Z` computes textual delay info for an IR package.
- `run ir-equiv --lhs-ir-action-id ... --rhs-ir-action-id ... [--top-fn-name ...] --version vX.Y.Z` checks IR equivalence and emits an equivalence JSON report.
- `run ir-to-combo-verilog --ir-action-id ... [--top-fn-name ...] [--use-system-verilog] --version vX.Y.Z` emits combinational Verilog text from IR via `ir2combo`.
- `run ir-to-k-bool-cone-corpus --ir-action-id ... [--top-fn-name ...] [--k 3] --version vX.Y.Z` runs `ir-bool-cones`, merges emitted cones into one deterministic IR package, and writes a companion manifest.
- `run combo-verilog-to-yosys-abc-aig --verilog-action-id ... [--verilog-top-module-name ...] --yosys-script path/to/flow.ys` runs a Yosys+ABC flow script and emits `result.aig`.
- `run aig-to-stats --aig-action-id ... --version vX.Y.Z` computes JSON metrics for an AIG artifact.
- `run aig-stat-diff --opt-ir-action-id ... --g8r-aig-stats-action-id ... --yosys-abc-aig-stats-action-id ...` joins and diffs both metric snapshots.
- `enqueue [--priority N] ...` puts one action in queue; larger `N` runs sooner.
- `enqueue-crate-version --crate-version vA.B.C [--priority N]` seeds canonical roots (stdlib + module subtrees). Descendants reuse that value as a base, then pick up stage bonuses as they move toward leaf comparison actions.
- `discover-releases --after vX.Y.Z [--max-pages N]` finds new releases with stdlib assets and enqueues download actions.
- `drain-queue [--limit N] [--worker-id ID] [--lease-seconds N] [--no-reclaim-expired]` processes queued actions with lease-based claiming.
- `show-suggested <action_id> [--recursive] [--max-depth N]` reports suggestion completion state.
- `enqueue-suggested <action_id> [--recursive] [--max-depth N] [--priority N]` idempotently enqueues missing suggested actions from provenance, promoting already-pending descendants when the stage bonus would make them more urgent.
- `repair-queue [--apply] [--reenqueue-missing-suggested]` scans queue records for malformed/corrupt JSON (for example after abrupt process termination); dry-run reports offenders, `--apply` removes corrupt queue files, and optional `--reenqueue-missing-suggested` refills missing suggested actions idempotently.
- `ingest-legacy-failed-records [--dry-run] [--keep-legacy-files]` one-shot migration that ingests legacy filesystem failure records (`failed-action-records/` and `queue/failed/`) into sled `failed_by_action`; with defaults it prunes those legacy directories after successful parse.
- `audit-suggested [--include-completed]` scans all completed actions and reports suggestion completion gaps.
- `find-aig-stat-diffs --opt-ir-action-id <id>` answers whether any completed `AigStatDiff` joins exist for a given optimized IR action.
- `populate-ir-fn-corpus-structural [--recompute-missing-hashes]` materializes a derived index of IR functions grouped by `ir-fn-structural-hash` into sled web-index keys (`ir-fn-corpus-structural.v1/...`), not filesystem files under the repo checkout.

## JSON-RPC API

Endpoint: `POST /api/jsonrpc` with `{"jsonrpc":"2.0","id":...,"method":...,"params":...}`.

Supported methods include:

- `query.ir_fn_corpus_g8r_vs_yosys_abc_samples`
  - params: `crate_version`, `yosys_levels_lt`, `ir_node_count_lt`, `sort_key`, optional `limit`
  - returns: matched samples plus `associated_ir` payloads
- `query.ir_fn_corpus_g8r_abc_vs_codegen_yosys_abc_samples`
  - params: `crate_version`, optional `max_ir_nodes`, optional `losses_only`, optional `strict_loss_only`, `sort_key`, optional `limit`, optional `cursor`
  - returns: matched samples plus `associated_ir`, `loss_summary`, and pagination (`cursor`, `next_cursor`)
- `query.ir_fn_sample_explain`
  - params: `g8r_stats_action_id`, `yosys_abc_stats_action_id`, optional `max_upstream_actions_per_branch`
  - returns: branch-local upstream provenance rows (including command traces/details), shared upstream action IDs, optional associated IR, and optional loss summary

Sort keys:

- `g8r_product_over_yosys_product_desc`
- `g8r_product_loss_desc`
- `yosys_levels_asc_ir_nodes_asc`
  - includes `DriverIrToOpt` members (one function per action)
  - includes `IrFnToKBoolConeCorpus` members by traversing the action’s companion cone-manifest metadata (many functions per action/package)
  - uses `(ir_action_id, ir_top)` identity for referential integrity when one IR package action contains multiple functions.
- `enqueue-structural-opt-ir-g8r --crate-version vA.B.C --fraig {true|false} [--recompute-missing-hashes] [--dry-run]` enqueues at most one canonical `DriverIrToG8rAig` action per unique optimized-IR structural hash for that crate/dso/fraig target, skipping hashes already represented in done/pending/running/failed/canceled states.
- `backfill-k-bool-cone-suggestions [--enqueue] [--dry-run]` repairs historical `IrFnToKBoolConeCorpus` provenances by reconstructing owned k-cone follow-up suggestions from manifests (global ownership by first-seen `(created_utc, action_id)` per cone structural hash); with `--enqueue` it also enqueues missing immediate follow-ups.
- `refresh-version-compat` updates only `third_party/xlsynth-crate/generated_version_compat.json` from upstream `main`.
- `scripts/sync-version-compat.sh [--check]` is the repo-level update/check script for `third_party/xlsynth-crate/generated_version_compat.json`.
- `dslx-to-mangled-ir-fn-name --dslx-module-name M --dslx-fn-name F` computes a typical mangled IR function name for planning.
- `show-provenance <action_id>` prints provenance.
- `resolve <action_id>` prints output path.
- `rematerialize <action_id>` replays stored action spec.

## Web Caching

- `serve-web` keeps an in-memory page cache and bounded dataset caches for the two heaviest comparison views (`/dslx-fns-g8r-vs-yosys-abc/` and `/ir-fn-corpus-g8r-vs-yosys-abc/`).
- In `--no-runner` mode, heavy datasets are prewarmed in a background thread at startup to reduce first interactive load latency.
- The store also caches provenance/failed-record list snapshots in memory (`BVC_STORE_LIST_CACHE_TTL_SECS`, default `10`) to avoid repeated full sled scans during request bursts.
- Runner-enabled web instances use short cache TTLs to keep dashboards near-live; no-runner instances use longer TTLs for maximum read performance.

## Web Architecture Posture

- Treat the web/service behavior as two architectural postures, not formal user-facing runtime modes:
  - **Read-heavy posture** (typically `--no-runner`): prioritize query latency and interactive UX; prefer aggressive prewarm and longer-lived caches/index snapshots.
  - **Write-heavy posture** (runner active): prioritize action throughput, index correctness, and bounded memory while new artifacts are materializing; accept slower heavy analytical page loads.
- This split is a software architecture principle, not a hard API contract. We may infer posture from whether embedded runners are active, but the action/data model remains identical in both cases.
- Operational guidance:
  - Use write-heavy posture while draining/enqueueing.
  - Use read-heavy posture for dashboards/analysis and reproducible reporting.

## Current Limitations / Next Steps

- `ir2g8r --aiger-out` is used for AIG output in the current implementation.
- Delay info currently executes the release `delay_info_main` binary directly; `xlsynth-driver ir2delayinfo` is not used until implemented upstream.
- Consider optional global CAS blobs for dedup across action payloads.
- As of March 14, 2026, the queue uses stage-bumped descendant priorities plus capped same-priority G8r micro-batching to improve "time to first comparison point" for new crate versions such as `crate:v0.39.0`. If snapshot views still lag after long drains, the next follow-up should be ready-aware batch suppression and/or priority-banded pending shards so sparse high-value leaf work is discovered faster than the current rotating pending scan.

## Root Rules

Per crate version (`crate:vA.B.C`), enqueueing processing currently seeds two canonical root actions using the compat-derived `dso:vX.Y.Z`:

1. `DownloadAndExtractXlsynthReleaseStdlibTarball`
1. `DownloadAndExtractXlsynthSourceSubtree` for `xls/modules/add_dual_path`

Both roots run `dslx-list-fns` discovery and enqueue concrete `DriverDslxFnToIr` suggestions.

## GC / Re-synthesis

The artifact directory is intentionally disposable: artifacts are canonicalized by action IDs and can be regenerated.
It is acceptable to garbage-collect old action directories (or wipe `bvc-artifacts/`) and re-run queue processing to re-materialize required outputs.
When migrating away from a legacy action-identity variant, delete the legacy action closures and re-enqueue from canonical roots so views/queries do not mix duplicated lineages.

## Schema Evolution Notes

- Action identity is `sha256({schema_version, action_spec})`. If action semantics or key fields change, increment `ACTION_SCHEMA_VERSION`.
- Queue entries whose dependencies are absent from all queue states and absent from artifacts are stale/orphaned; deleting those pending entries is acceptable.
- Suggested-action IDs may change across schema/spec evolution; regenerate by re-running enqueue/discovery from current provenance instead of preserving old pending IDs.
- Favor simplicity for this repo: stale queue data can be deleted, and required outputs can be re-materialized from current action specs.
