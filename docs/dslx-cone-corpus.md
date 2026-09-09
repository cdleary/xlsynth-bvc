<!-- SPDX-License-Identifier: Apache-2.0 -->

# Comparing a DSLX cone corpus

This workflow has three commands. `dslx-corpus-ingest` scans a local `.x` tree, discovers concrete
functions, converts each with `xlsynth-driver dslx2ir --opt true`, strips source position
metadata with `xlsynth-driver ir-strip-pos-data`, and uses `xlsynth-driver`
to extract MFFCs and optional k-cones from the optimized IR. It does **no synthesis or ABC work**.
The existing `run-ir-dir-corpus` runner compares the resulting cone IR using a matched ABC
recipe. `render-dslx-corpus-report` joins the two outputs into a standalone static page.

Run these commands from the repository root, using an installed `xlsynth-driver` on `PATH`:

```bash
mkdir -p ../bvc-corpus-work

cargo run --bin xlsynth_bvc -- dslx-corpus-ingest \
  --input-dir ./fixtures/dslx \
  --output-dir ../bvc-corpus-work/ingested \
  --max-files 50 --max-functions 500 --max-mffcs 200 \
  --k 3 --max-k-cones 200 --max-k-ir-ops 16

cargo run --bin xlsynth_bvc -- run-ir-dir-corpus \
  --input-dir ../bvc-corpus-work/ingested/cones \
  --output-dir ../bvc-corpus-work/compared \
  --recipe-preset g8r-abc-vs-yabc-aig-diff \
  --execution-mode enqueue \
  --top-fn-policy infer-single-package \
  --version "<installed-libxls-version>" \
  --driver-version "<installed-xlsynth-driver-crate-version>"

cargo run --bin xlsynth_bvc -- \
  --store-dir ../bvc-corpus-work/compared/.bvc/bvc-artifacts \
  --artifacts-via-sled ../bvc-corpus-work/compared/.bvc/artifacts.sled \
  run-workers --workers 4 --exit-when-idle

cargo run --bin xlsynth_bvc -- refresh-corpus-status --output-dir ../bvc-corpus-work/compared

cargo run --bin xlsynth_bvc -- render-dslx-corpus-report \
  --ingest-dir ../bvc-corpus-work/ingested \
  --comparison-dir ../bvc-corpus-work/compared \
  --output-dir ../bvc-corpus-work/report
```

The comparison runner uses its standard runtime and container setup (see
[the IR directory runner](ir-dir-corpus-runner.md)). In `run` mode it executes immediately; in
`enqueue` mode refresh its exports after workers finish. The matched recipe lowers G8r with the
frontend mode, skips driver FRAIG, then applies `flows/yosys_to_aig.ys` to its AIG. The other
branch converts IR to combinational Verilog and applies that **same** Yosys/ABC script. The
joined row includes AIG stats, action IDs, the script digest, and runtime versions for each pair.
`--fraig` is rejected for this recipe.

Ingest outputs include `sources/` snapshots, `optimized/` packages, raw extractor files and
logs, one single-top `.ir` per unique cone in `cones/`, and `manifest.json`. Each occurrence
in the manifest identifies its relative source path and function, extraction kind, cone digest,
optional MFFC rank, and source IR digest. Identical cone content is saved once, while all its
occurrences are retained. Parametric functions are skipped until instantiated; failed concrete
functions and extraction steps are recorded separately. Work bounds and driver version are
recorded in the manifest. Discovery and conversion search the source file's directory first,
then the snapshot root, then any additional import roots. `--dslx-path` may be repeated for those roots, and
`--dslx-stdlib-path`, `--driver`, and `--toolchain` select external tooling. Start with an empty
output directory outside the input tree and resource checkout; both commands reject overlapping
outputs before writing. Results stay in those user-selected output directories.

The report contains `index.html`, `data.json`, and linked exact cone IR under `ir/`. Open the
HTML directly or serve the directory with any local static HTTP server. The page shows coverage,
paired nodes and depth charts, extraction and size filters, all source occurrences, failure
summaries, and action evidence. It checks that the manifest, sample statuses, and joined rows
describe the same comparison run, with one joined row per completed sample. If exports disagree,
run `refresh-corpus-status` again before rendering. Source derived strings are inserted as text; no external assets
or backend are required. The generated outputs can contain copies of input source and IR, so
choose where to store and share them accordingly.
