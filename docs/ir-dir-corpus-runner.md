# IR Directory Corpus Recipe Runner

## Implemented V1

`xlsynth-bvc` now has a first batch bridge from a local IR directory into the existing action
system:

- `run-ir-dir-corpus`
- output-dir-local workspace under `OUTPUT_DIR/.bvc/`
- imported local IR roots via `ImportIrPackageFile`
- fixed recipe expansion into existing downstream actions
- manifest/export writing into the user-visible output directory

The implementation goal is simple UX without inventing a second store model:

- the internal DB/queue layout stays normal
- the workspace is scoped by location instead of by a special in-memory mode
- the user can still leverage the existing mini build system and queue commands directly

## Current Command Shape

Command:

```bash
cargo run --bin xlsynth_bvc -- \
  run-ir-dir-corpus \
  --input-dir /tmp/mcmc-ir \
  --output-dir /tmp/mcmc-ir-g8r-vs-yabc \
  --execution-mode enqueue \
  --recipe-preset g8r-vs-yabc-aig-diff \
  --top-fn-policy infer-single-package \
  --version v0.39.0 \
  --driver-version 0.34.0
```

Current supported flags:

- `--input-dir <dir>`
- `--output-dir <dir>`
- `--execution-mode enqueue|run`
- `--recipe-preset g8r-vs-yabc-aig-diff|g8r-vs-yabc-no-fraig-aig-diff`
- `--top-fn-policy infer-single-package|explicit|from-filename`
- `--top-fn-name <name>` when policy is `explicit`
- `--fraig`
- `--version <dso:vX.Y.Z input via existing --version flag>`
- `--driver-version <crate:vA.B.C input via existing --driver-version flag>`
- `--yosys-script <path>` when explicitly provided must match the selected preset's canonical
  script
- `--priority <n>` when enqueueing

It recursively scans `INPUT_DIR` for files ending in `.ir`.

## Workspace Layout

The command creates a normal self-contained workspace under the requested output directory:

- `OUTPUT_DIR/.bvc/artifacts.sled`
- `OUTPUT_DIR/.bvc/bvc-artifacts/queue/...`
- `OUTPUT_DIR/.bvc/bvc-artifacts/.materialized-actions/...`

Public outputs are written directly into `OUTPUT_DIR/`:

- `manifest.json`
- `samples.jsonl`
- `summary.json`
- `joined/g8r-vs-yabc-aig-diff.csv`
- `joined/g8r-vs-yabc-aig-diff.jsonl`
- `joined/g8r-vs-yabc-no-fraig-aig-diff.csv`
- `joined/g8r-vs-yabc-no-fraig-aig-diff.jsonl`
- `artifacts/<sample_id>/...` for copied leaf outputs when available

The summary JSON includes the exact `--store-dir` and `--artifacts-via-sled` paths for the
internal workspace.

## Why Import First

The hard problem is not durability. It is external IR ingestion.

Existing `ActionSpec` values only compose from already-stored upstream artifacts. A queue
worker cannot safely depend on a caller-local path like `/tmp/foo/sample.ir`.

So the first new concept should be a synthetic imported root:

- `ImportIrPackageFile`

Conceptually:

1. the corpus runner reads bytes from the caller's input directory
1. it writes those bytes into the artifact store immediately
1. it records provenance for a stable import action ID
1. all downstream actions reference that imported IR action ID

That keeps downstream execution hermetic and queue-safe.

## Implemented Root Action

Implemented action:

1. `ImportIrPackageFile(source_sha256, top_fn_name?) -> IrPackageFile`

Important constraints:

- action identity must not depend on absolute host paths
- the action spec uses content hash plus optional top function name
- sample-relative path and logical name stay in the corpus manifest rather than the action spec
- import is performed directly by `run-ir-dir-corpus` rather than by queue workers

This is intentionally a seed/import action, not a dockerized transform.

## Implemented Preset

Current presets:

- `g8r-vs-yabc-aig-diff`
- `g8r-vs-yabc-no-fraig-aig-diff`

Expansion per sample:

1. `ImportIrPackageFile`
1. `DriverIrToG8rAig`
1. `DriverAigToStats`
1. `IrFnToCombinationalVerilog`
1. `ComboVerilogToYosysAbcAig`
1. `DriverAigToStats`
1. `AigStatDiff`

The two presets intentionally use the same action graph shape and differ only in the Yosys/ABC
script used by `ComboVerilogToYosysAbcAig`:

- `g8r-vs-yabc-aig-diff` uses `flows/yosys_to_aig.ys`
- `g8r-vs-yabc-no-fraig-aig-diff` uses `flows/abc_ablate_no_fraig.ys`

For symmetry with the rest of the repo, the preset should write `yosys/abc` in data keys
and docs, but `yabc` is a reasonable shorthand in CLI preset names and output filenames.

Example no-fraig run on a directory of `.ir` files:

```bash
cargo run --bin xlsynth_bvc -- \
  run-ir-dir-corpus \
  --input-dir /tmp/mcmc-ir \
  --output-dir /tmp/mcmc-ir-g8r-vs-yabc-no-fraig \
  --execution-mode enqueue \
  --recipe-preset g8r-vs-yabc-no-fraig-aig-diff \
  --top-fn-policy infer-single-package \
  --version v0.39.0 \
  --driver-version 0.34.0
```

## Queue-Backed Workflow

The enqueue workflow is deliberately explicit:

1. first `run-ir-dir-corpus --execution-mode enqueue` seeds the self-contained workspace, imports
   local IR roots, expands the fixed recipe, enqueues missing actions, and writes initial exports
   with current statuses
2. `drain-queue` targets `OUTPUT_DIR/.bvc/` using the normal worker command
3. rerunning the same `run-ir-dir-corpus` command with the same input/output/config tuple refreshes
   `manifest.json`, `samples.jsonl`, `summary.json`, `joined/*`, and copied leaf artifacts from the
   completed action graph

There is no separate `--refresh-only` command yet. Rerunning the corpus command is the refresh
mechanism.

Initial submission:

```bash
cargo run --bin xlsynth_bvc -- \
  run-ir-dir-corpus \
  --input-dir /tmp/mcmc-ir \
  --output-dir /tmp/mcmc-ir-g8r-vs-yabc \
  --execution-mode enqueue \
  --top-fn-policy infer-single-package \
  --version v0.39.0 \
  --driver-version 0.34.0
```

Drain the internal queue with the normal command-line worker:

```bash
cargo run --bin xlsynth_bvc -- \
  --store-dir /tmp/mcmc-ir-g8r-vs-yabc/.bvc/bvc-artifacts \
  --artifacts-via-sled /tmp/mcmc-ir-g8r-vs-yabc/.bvc/artifacts.sled \
  drain-queue --worker-id corpus-a --lease-seconds 1800
```

The corpus command is idempotent for a stable input/output/config tuple, so the rerun refresh step
does not invent a second store model or a separate export path.

## Inline Workflow

For an immediate one-shot batch run without queue workers:

```bash
cargo run --bin xlsynth_bvc -- \
  run-ir-dir-corpus \
  --input-dir /tmp/mcmc-ir \
  --output-dir /tmp/mcmc-ir-inline \
  --execution-mode run \
  --top-fn-policy explicit \
  --top-fn-name foo \
  --version v0.39.0 \
  --driver-version 0.34.0
```

## Output Directory Contract

`manifest.json` stores command configuration, workspace paths, runtimes, and one sample row per IR.

The top-level manifest also makes two corpus-specific semantics explicit:

- `refresh_semantics = rerun_same_command_refreshes_exports`
- `sample_id_scheme = basename-plus-normalized-source-relpath-sha256-12-v1`

Each sample row includes:

- `sample_id`
- `logical_name`
- `source_relpath`
- `source_sha256`
- `top_fn_policy`
- `top_fn_name`
- `fraig`
- exact `dso_version`
- exact driver crate versions for both the main driver flow and stats flow
- `yosys_script` path
- `yosys_script_sha256`
- `preset`
- all action IDs in the fixed recipe
- per-action status strings
- final sample `status`
- summarized `error`

`summary.json` aggregates total/completed counts, status counts, queue-vs-run mode, and the
workspace paths needed to target the same store with normal queue/provenance commands.

The joined diff tables denormalize:

- `source_relpath`
- `source_sha256`
- `top_fn_policy`
- `top_fn_name`
- both branch action IDs plus the import and diff action IDs
- `g8r_and_nodes`
- `g8r_depth`
- `g8r_product`
- `yosys_abc_and_nodes`
- `yosys_abc_depth`
- `yosys_abc_product`
- `g8r_product_loss`
- `delta_and_nodes_yosys_minus_g8r`
- `delta_depth_yosys_minus_g8r`

This is meant to make the public joined tables practical for science/analysis use without forcing
the caller to reopen internal action provenance just to recover the basic run configuration.

## Sample Identity

`sample_id` is intentionally path-stable, not content-addressed.

Current rule:

- sanitize the sample basename
- append the first 12 hex characters of `sha256(normalized_source_relpath)`

That means:

- rerunning the same corpus layout produces the same `sample_id`
- changing file contents without changing relative path keeps the same `sample_id`
- content identity is still visible as `source_sha256`

## Top Function Policy

Batch import needs one consistent answer to "what is the top function for this IR file?"

Implemented policies:

- `infer_single_package`: parse the IR package and require exactly one unambiguous top
  function/proc candidate
- `from_filename`: strip extension and use basename
- `explicit`: use one shared `--top-fn-name`

The resolved `top_fn_name` is persisted in both the import action and the sample manifest.
`infer_single_package` is useful for narrow corpora but not for general multi-function packages.

## Execution Modes

The same batch description should support both:

- `--execution-mode enqueue`
- `--execution-mode run`

`enqueue` mode:

- imports root IR artifacts immediately
- writes manifest rows with action IDs
- enqueues downstream actions
- leaves final status as `pending` / `running` / `done` / `failed` based on current store state

`run` mode:

- imports root IR artifacts immediately
- executes the expanded action chain inline using existing execution helpers
- materializes final copied outputs into `--output-dir/artifacts/<sample_id>/`
- writes final joined tables before exit

This avoids inventing two separate corpus APIs for the same recipe graph.

## Follow-Ups

- sidecar per-sample metadata/input manifest support
- more presets, including k-cone corpus recipes
- a dedicated refresh-only command if the rerun workflow becomes too implicit
- optional web/query surfaces over corpus-run manifests

## Non-Goals

- do not make the queue depend on caller-local source paths
- do not invent a second provenance format outside `ActionSpec` + `Provenance`
- do not bypass existing `DriverIrToG8rAig`, `IrFnToCombinationalVerilog`,
  `ComboVerilogToYosysAbcAig`, `DriverAigToStats`, or `AigStatDiff` actions

## Short Version

The missing layer is:

- a synthetic imported-IR root action
- a vectorized preset expander over existing actions
- a stable per-sample manifest/export bundle

That is now the bridge from "directory of IR files" to "batch g8r vs yabc diff stats".
