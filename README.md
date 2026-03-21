# xlsynth-bvc

Hermetic, reproducible action execution for xlsynth artifact pipelines.

See `docs/hermetic-action-design.md` for the architecture.

Artifacts are stored in sharded CAS-style paths, e.g.:

`bvc-artifacts/artifacts/<aa>/<bb>/<action_id>/...`

## Build

```bash
cargo build
```

## Run Actions

```bash
# Download and extract stdlib from a release
cargo run --bin xlsynth_bvc -- run download-stdlib --version v0.37.0

# Convert DSLX function to IR using xlsynth-driver in Docker
cargo run --bin xlsynth_bvc -- run dslx-fn-to-ir \
  --dslx-subtree-action-id <stdlib_action_id> \
  --dslx-file xls/dslx/stdlib/math.x \
  --dslx-fn-name add \
  --version v0.37.0

# Optimize IR
cargo run --bin xlsynth_bvc -- run ir-to-opt \
  --ir-action-id <ir_action_id> \
  --top-fn-name foo \
  --version v0.37.0

# Compute delay info for an IR package (uses release delay_info_main tool)
cargo run --bin xlsynth_bvc -- run ir-to-delay-info \
  --ir-action-id <opt_ir_action_id> \
  --top-fn-name foo \
  --delay-model asap7 \
  --version v0.37.0

# Convert IR to AIG
cargo run --bin xlsynth_bvc -- run ir-to-g8r-aig \
  --ir-action-id <opt_ir_action_id> \
  --top-fn-name foo \
  --fraig \
  --version v0.37.0

# Check unoptimized vs optimized IR equivalence
cargo run --bin xlsynth_bvc -- run ir-equiv \
  --lhs-ir-action-id <unopt_ir_action_id> \
  --rhs-ir-action-id <opt_ir_action_id> \
  --top-fn-name foo \
  --version v0.37.0

# Convert IR function to combinational Verilog/SystemVerilog
cargo run --bin xlsynth_bvc -- run ir-to-combo-verilog \
  --ir-action-id <opt_ir_action_id> \
  --top-fn-name foo \
  --use-system-verilog \
  --version v0.37.0

# Convert combinational Verilog to AIG via yosys+abc and a checked-in flow script
cargo run --bin xlsynth_bvc -- run combo-verilog-to-yosys-abc-aig \
  --verilog-action-id <verilog_action_id> \
  --verilog-top-module-name foo \
  --yosys-script flows/yosys_to_aig.ys

# Compute AIG stats from either g8r or yosys/abc AIG output
cargo run --bin xlsynth_bvc -- run aig-to-stats \
  --aig-action-id <aig_action_id> \
  --version v0.37.0

# Join two AIG stats artifacts and produce a metric diff
cargo run --bin xlsynth_bvc -- run aig-stat-diff \
  --opt-ir-action-id <opt_ir_action_id> \
  --g8r-aig-stats-action-id <g8r_stats_action_id> \
  --yosys-abc-aig-stats-action-id <yosys_stats_action_id>
```

## Queue Mode

```bash
# Enqueue work
cargo run --bin xlsynth_bvc -- enqueue download-stdlib --version v0.37.0

# Discover new releases after a floor tag and enqueue them
cargo run --bin xlsynth_bvc -- discover-releases --after v0.37.0

# Drain queue with lease-based worker claiming
cargo run --bin xlsynth_bvc -- drain-queue --worker-id worker-a --lease-seconds 1800
```

`drain-queue` now records failed actions under `queue/failed/` and recursively
cancels queued downstream dependents under `queue/canceled/` based on action
dependency edges.
It also claims only dependency-ready actions, so items do not run out of
dependency order.
Each action execution has a default 300-second timeout; timed-out actions are
recorded as failed with an error prefix `TIMEOUT(300)`.
Before claiming work, `drain-queue` preflights runtime dependencies for pending
actions (builds required Docker images and fills `bvc-artifacts/driver-release-cache/<dso>/<platform>/`).
After setup, action containers run with `--pull never --network none` and consume
the cached release assets/protos, so workers do not repeatedly hit GitHub during execution.

Optional queue policy toggle:
- `BVC_QUEUE_ONLY_PREVIOUS_LOSS_K_CONES=1`: only enqueue suggested `IrFnToKBoolConeCorpus` actions when the source opt-IR structural hash has previously produced a positive k-cone loss sample. Suggestions are still recorded in provenance; this only filters queue insertion. The policy is honored by `drain-queue`, the embedded web runner, `enqueue-crate-version` recursive suggestion enqueue, and queue repair/reenqueue paths.

## Web UI

```bash
# Start local web UI (defaults to 127.0.0.1:3000)
cargo run --bin xlsynth_bvc -- serve-web

# Custom bind address
cargo run --bin xlsynth_bvc -- serve-web --bind 127.0.0.1:3900

# Serve read-only from a static snapshot directory (no live sled scans)
cargo run --bin xlsynth_bvc -- serve-web --no-runner --snapshot-dir /path/to/snapshot
```

The web UI currently has:
- `/versions/`: groups actions by crate version and shows per-version failure summaries.
- `/stdlib-fns-g8r/`: interactive Plotly time-series for stdlib function g8r AIG stats across crate versions (`metric=and_nodes|depth`, `fraig=true|false`).

Snapshot mode notes:
- Runner/queue mutation endpoints are disabled.
- `/action/*`, `/api/dslx-sample-details`, `/api/jsonrpc`, `/db-size/`, and `/dslx-file-action-graph/` are intentionally unavailable in snapshot mode.
- Indexed read-only views (`/versions/`, `/dslx-fns*`, `/ir-fn-corpus*`, `/ir-fn-corpus-structural/`) read from snapshot `web_index/` files.
- Snapshot builds intentionally omit `stdlib-file-action-graph.v1.json` to keep snapshot size tractable.

## Static Snapshot Build/Verify

```bash
# Build snapshot from current web indices (rebuild indices first by default)
cargo run --bin xlsynth_bvc -- build-static-snapshot --out-dir /path/to/snapshot --overwrite

# Optional fast path when indices are already fresh
cargo run --bin xlsynth_bvc -- build-static-snapshot --out-dir /path/to/snapshot --overwrite --skip-rebuild-web-indices

# Verify manifest + dataset checksums
cargo run --bin xlsynth_bvc -- verify-static-snapshot --snapshot-dir /path/to/snapshot
```

Make targets:
- `make build-static-snapshot`
- `make verify-static-snapshot`
- `make test`

Useful toggles:
- `LOCAL_SNAPSHOT_SKIP_REBUILD_WEB_INDICES=1` (skip rebuild during snapshot build)

Deployment automation and infrastructure configuration intentionally live outside
this repository.

## Release Discovery

```bash
# Enqueue all releases newer than the given floor tag
cargo run --bin xlsynth_bvc -- discover-releases --after v0.37.0

# Inspect what would be enqueued without mutating queue state
cargo run --bin xlsynth_bvc -- discover-releases --after v0.37.0 --dry-run
```

## Compatibility Map Refresh

```bash
# Refresh only the crate<->xlsynth compatibility JSON from upstream main
cargo run --bin xlsynth_bvc -- refresh-version-compat

# Equivalent script form (default: update if needed)
scripts/sync-version-compat.sh

# CI/check mode: exit non-zero if out of date
scripts/sync-version-compat.sh --check
```

## IR Corpus Structural Index

```bash
# Refresh the sled-backed structural corpus index
cargo run --bin xlsynth_bvc -- populate-ir-fn-corpus-structural
```

## Sled Space Analysis

```bash
# Scan the sled DB and print per-tree/category space usage as JSON
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  analyze-sled-space --top 25 --sample 40
```

Notes:
- This command scans all sled rows and can take time on large DBs.
- Sled uses an exclusive lock. If the DB is in use by another process,
  stop the service first or analyze a copied snapshot directory.
- For `action_file_bytes`, the JSON includes raw aggregations by relpath root,
  extension, filename, and relpath prefixes (depth 2/3).

## Sled Action File Compression

For the sled backend, action-file rows are zstd-compressed on write and
decompressed on materialization. Compression is transparent to callers.

Defaults:
- compress rows >= `4 KiB` at level `3`
- compress rows >= `1 MiB` at level `12`

Tunable environment variables:
- `BVC_SLED_ACTION_FILE_COMPRESS_MIN_BYTES`
- `BVC_SLED_ACTION_FILE_COMPRESS_LEVEL`
- `BVC_SLED_ACTION_FILE_COMPRESS_LARGE_MIN_BYTES`
- `BVC_SLED_ACTION_FILE_COMPRESS_LEVEL_LARGE`

Backfill existing sled rows:

```bash
# Rewrite action_file_bytes rows using current compression policy
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  backfill-sled-action-file-compression

# High-impact targeted pass: only rows for a given relpath
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  backfill-sled-action-file-compression --target-relpath prep_for_gatify.ir

# Prune actions whose relpath payload is above a size threshold, with downstream dependents
# (default relpath is payload/prep_for_gatify.ir)
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  prune-sled-actions-by-relpath-size --min-bytes $((1024*1024))

# Preview only
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  prune-sled-actions-by-relpath-size --min-bytes $((1024*1024)) --dry-run
```

## Sled DB Compaction

After large rewrites/backfills, sled can keep stale segments on disk. Compaction
copies live rows into a fresh DB and (optionally) atomically swaps it in.

```bash
# Write compacted copy beside the source DB
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  compact-sled-db

# Choose destination explicitly
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  compact-sled-db --output-path /path/to/artifacts.compacted.sled

# Swap compacted DB into source path (source is renamed to *.precompact-<ts>.bak)
cargo run --bin xlsynth_bvc -- \
  --artifacts-via-sled /path/to/artifacts.sled \
  compact-sled-db --replace-source
```

Notes:
- Stop the service first; sled requires an exclusive lock.
- Compaction needs additional free disk temporarily (source + compacted copy,
  until swap completes).

## Structural-Hash G8r Enqueue

```bash
# Enqueue one ir2g8r action per unique optimized-IR structural hash for a target crate runtime.
# Skips hashes that already have a matching g8r action (done/pending/running/failed/canceled)
# for the same crate version, mapped dso version, and fraig mode.
cargo run --bin xlsynth_bvc -- enqueue-structural-opt-ir-g8r \
  --crate-version v0.31.0 \
  --fraig true

# Preview without mutating queue state
cargo run --bin xlsynth_bvc -- enqueue-structural-opt-ir-g8r \
  --crate-version v0.31.0 \
  --fraig false \
  --dry-run
```

## Page Load Benchmark

```bash
# Benchmark common UI routes on a running server
cargo run --release --bin page_load_bench -- \
  --base-url http://127.0.0.1:3000 \
  --warmup 1 \
  --samples 3

# Benchmark specific routes only
cargo run --release --bin page_load_bench -- \
  --base-url http://127.0.0.1:3000 \
  --route /versions/ \
  --route /ir-fn-corpus-g8r-vs-yosys-abc/?losses_only=false
```

Driver-backed actions validate `--driver-version` against
`third_party/xlsynth-crate/generated_version_compat.json` and fail early on mismatches.
If `--driver-version` is omitted, the latest compatible crate version from that file is selected.
For driver-backed actions, `--version` names the xlsynth DSO/tool release (`dso:vX.Y.Z`) and
`--driver-version` names the `xlsynth-driver` crate release (`crate:vX.Y.Z`).
Provenance details for these actions include both labels via
`details.dso_version_label` and `details.crate_version_label`.
For `driver_aig_to_stats`, provenance also records the producing AIG lineage
(`details.source_aig_crate_version_label` / `details.source_aig_dso_version_label`)
separately from the stats-runtime labels
(`details.stats_runtime_crate_version_label` / `details.stats_runtime_dso_version_label`).
The compatibility map is keyed by crate version and yields a DSO version, so `dso:v...`
is derivable from `crate:v...`; however multiple crate releases can map to the same DSO,
so both labels are stored explicitly.
The driver image installs `xlsynth-driver` with `--features with-bitwuzla-system,with-easy-smt` and pulls pinned
bitwuzla DSOs from `xlsynth/boolector-build` (release tag pinned in `docker/xlsynth-driver.Dockerfile`),
so `ir-equiv` and `aig-equiv` can use bitwuzla.
`download-stdlib` also uses the latest compatible driver runtime to run `dslx-list-fns` across
all extracted `.x` files and suggest `dslx-fn-to-ir` actions for concrete functions.
Yosys-script-driven actions capture the script as `(path, sha256)` in the action spec, and
execution re-checks that hash before running.
IR-consuming driver actions (`ir-to-opt`, `ir-to-delay-info`, `ir-to-g8r-aig`, `ir-to-combo-verilog`) now compute
`ir-fn-structural-hash` and can semantically reuse a prior action payload when the structural hash
and action parameters match (same driver runtime/version and same effective top/settings). Provenance
records this via `details.semantic_cache_hit`.
`aig-to-stats` uses the `xlsynth-driver` runtime selected in the action spec.
When enqueuing historical suggested actions, stale `aig-to-stats` specs from
legacy workflows are canonicalized to the current runtime policy.
For legacy `driver_ir_to_g8r_aig` producers (`crate < v0.24.0`) that emit
`.g8rbin` via `--bin-out` (non-AIGER), `aig-to-stats` falls back to
`ir2g8r --stats-out` on the producer IR and normalizes
`live_nodes/deepest_path -> and_nodes/depth` for downstream trend queries.

`ir-to-delay-info` emits `payload/delay_info.textproto` by running `delay_info_main --proto_out`
and decoding `xls.DelayInfoProto` using schema files cached during setup from the
matching `xlsynth/xlsynth` release tag.

## Suggested Next Actions

```bash
# Show suggested next actions for one completed action
cargo run --bin xlsynth_bvc -- show-suggested <action_id>

# Walk suggestions recursively and show completion status
cargo run --bin xlsynth_bvc -- show-suggested <action_id> --recursive --max-depth 4

# Idempotently enqueue missing suggested actions from a root
cargo run --bin xlsynth_bvc -- enqueue-suggested <action_id> --recursive --max-depth 8

# Audit all completed actions and list missing suggested successors
cargo run --bin xlsynth_bvc -- audit-suggested
```

`ir-to-opt` suggestions now include:
- `ir-equiv` (unoptimized vs optimized IR)
- `ir-to-delay-info`
- `ir-to-g8r-aig` (`fraig=false` and `fraig=true`)
- `ir-to-combo-verilog`

`aig-to-stats` suggestions can synthesize the counterpart flow and propose `aig-stat-diff`.

## Queries

```bash
# Find all completed AIG stat diffs derived from an optimized IR action
cargo run --bin xlsynth_bvc -- find-aig-stat-diffs --opt-ir-action-id <opt_ir_action_id>
```

## Utility

```bash
cargo run --bin xlsynth_bvc -- dslx-to-mangled-ir-fn-name \
  --dslx-module-name my_mod \
  --dslx-fn-name f
```

See `third_party/xlsynth-crate/VENDORED.md` for pinned vendored asset sources.

## Inspect

```bash
cargo run --bin xlsynth_bvc -- show-provenance <action_id>
cargo run --bin xlsynth_bvc -- resolve <action_id>
cargo run --bin xlsynth_bvc -- rematerialize <action_id>
```

## GC / Rebuild

The artifact store is intentionally rebuildable from action specs and provenance.
It is valid to delete old action data (or all of `bvc-artifacts/`) and re-synthesize
artifacts by re-running `enqueue`/`drain-queue` or direct `run` commands.
