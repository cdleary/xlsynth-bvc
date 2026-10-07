# Complete-corpus website

`build-static-site --corpus-run-dir` builds a standalone corpus explorer directly from completed
release workspaces. It needs neither a global artifact-store merge nor a snapshot. Repeat the
flag for every release to include; omit unfinished releases until they finish. No inputs are
sampled or silently omitted to fit a hosting limit.

## One build command

Once each worker has exited, finalize its exports using the same pinned runner and original
`run-ir-dir-corpus` invocation. Check that every planned sample is done and that no samples are
failed or missing. Then use the current website binary:

```bash
cargo run --release --bin xlsynth_bvc -- build-static-site \
  --corpus-run-dir runs/release-newer \
  --corpus-run-dir runs/release-older \
  --out-dir ../bvc-site
```

The command validates, converts the operational ingress to typed protobuf evidence, generates
the browser projection, verifies the entire output, and atomically installs `../bvc-site`. It does not
enqueue work, refresh exports, migrate the evaluation store, or deploy. Stores must be idle:
their canonical sled databases cannot be opened while a worker owns them. Use `--overwrite` to
atomically replace an earlier site; a rejected build leaves the earlier site intact.

Run these examples from the checkout. Output and publication directories must be outside the
checkout, input workspaces, and private stores (and must not be their ancestors). The sibling
directories used here keep generated public files separate from source and evaluation data.

Inputs must use `g8r-abc-vs-yabc-aig-diff`, with no fraiging or Git-candidate override. All samples
must have complete import, lowering, ABC, codegen, reference, raw/post-ABC stats, and diff actions.
The builder checks every action's identity and provenance, stats export byte digests, and input
IR content hashes. All included releases must contain exactly the same relative paths, source
bytes, and top functions, and use the same stats estimator and Yosys/ABC runtime/script. Each
generation has one lowering runtime; its own DSO is recorded and bound to its action graph.

## All-versions dashboard

Supply a historical snapshot **and** the completed full-corpus runs in the same command to build
the project dashboard. Include the registered fixed-cohort runs for the historical progression:

```bash
cargo run --release --bin xlsynth_bvc -- build-static-site \
  --snapshot-dir snapshots/current \
  --progression-run-dir runs/fixed-cohort-older \
  --progression-run-dir runs/fixed-cohort-newer \
  --corpus-run-dir runs/full-corpus-older \
  --corpus-run-dir runs/full-corpus-newer \
  --out-dir ../bvc-site
```

The root dashboard shows the latest evaluated version, all versions with historical synthesis
measurements or fixed-cohort results, and a large selectable fixed-population progression chart.
`/` is the only overview: `/history/` and `/history/index.html` redirect to it. Previous-version
comparisons, datasets, diagnostics, and progression (including Git candidates) remain detail views
under `history/`; `corpus/` contains the completed full-corpus comparison view. Both sets of detail
pages share Results, Latest, All versions, and Progression navigation. “Results” returns directly
to the dashboard, including from nested campaign pages. These directories are evidence-layout
boundaries, not separate sites. Dashboard release trends exclude Git
candidates and incomplete cohorts; those remain available in detailed progression.

Each trend is the change in summed post-ABC AND-nodes × depth versus its first complete release.
Partial historical populations are explicitly labeled and never mixed into that sum. Raw G8r
historical comparisons remain accessible but are not treated as post-ABC measurements. Counts
across full-corpus, historical pairs, and fixed cohorts may overlap and must not be added together.

This is native composition, not a merge of operational JSON: `dashboard.pb` binds the two verified
child manifests. That composition also determines the detail-page navigation and the old overview
redirect; standalone builders retain their standalone presentation. Verification checks both
children, exact file closure and templates (including navigation and redirects), and regenerates
`dashboard.json` from verified evidence. The entire composition installs atomically and shares one
hosting budget. Only the small summary loads on the splash page, not every release's sample shards.

Snapshot-only and corpus-only builds remain available. `--progression-run-dir` requires a snapshot.
Use a current snapshot when release metadata, recipes, or cohort definitions change; the dashboard
does not discover run directories or backfill missing evaluations automatically.

## Preview and inspect

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory ../bvc-site
```

Open `http://127.0.0.1:8000/`. Choose the release, compare its G8r+ABC result with either its
matched codegen+Yosys/ABC reference or another release's G8r+ABC result, and filter by input kind,
IR size, or positive product loss. Click a point/outlier to load its source IR. The cards include
zero-cost inputs; logarithmic plots explicitly omit zero-valued points. Undefined graph logical
effort is omitted only from that plot. Input-kind labels follow corpus top-function conventions.
The leading full-width **Product loss vs IR size** plot puts small, high-loss inputs in the upper
left. Both axes are logarithmic, and it includes every positive product-cost delta under the
selected comparison (including node/depth tradeoffs). Nonpositive losses or IR sizes are omitted
only from that plot; summaries still include them. Clicking a loss point loads the verified IR.

For an explicit independent audit or after copying the site:

```bash
xlsynth_bvc verify-static-site --site-dir ../bvc-site
xlsynth_bvc smoke-static-site --site-dir ../bvc-site
```

The build already performs the complete static verification. Browser smoke is separate and
requires Chrome/Chromium. `--base-url /prefix/` supports subpath hosting, and assets use relative
URLs so the existing immutable publication wrapper also works.

The optional integration test checks every included release's plot population, paired deltas,
rapid filter changes, source-IR selection, and mobile sizing. With a local server running:

```bash
node scripts/test_corpus_site_browser.cjs http://127.0.0.1:8000/ ../bvc-screenshots
```

It uses a disposable headless Chrome profile (`BVC_CHROME` can select the executable).
For a composed site, use the `corpus/` URL for this test and run
`node scripts/test_dashboard_browser.cjs http://127.0.0.1:8000/ ../bvc-screenshots`
to check the dashboard's cohorts, all-version links, and mobile layout.

## Automatic size bounds

The default `--corpus-shard-bytes 2097152` targets at most 2 MiB per evidence, metrics, or IR
file. Splitting is deterministic and lossless. The option accepts 1 through 16 MiB; a single input
larger than the target fails with an actionable error instead of dropping or truncating it.
Metric shards are loaded four requests at a time for the selected comparison; only its one or
two releases are retained after rendering. IR shards are fetched only on selection.

Verification enforces a 25 MiB per-file budget and a 19,990-file site budget, reserving space
under a 20,000-file deployment envelope for publication wrappers. These are conservative build
budgets, not automatic detection of a hosting account's plan. They also apply to snapshot-based
sites, which fail verification if an unsharded dataset exceeds the budget. Reusing a publication
root containing many older sites can exceed the deployment envelope in aggregate: validate the
actual deploy directory or stage only the intended publication before deployment.

`corpus-site.pb` identifies releases, the exact input cohort, and every bounded shard.
`data/<version>/<ordinal>.pb` retains typed source/action/metric evidence; adjacent `.json`
and `.ir.json` are only the final web projection. Verification reconstructs the action graphs,
rechecks complete cohort identity, and regenerates both JSON projections byte-for-byte.
Publication-only messages are compiled separately, preserving the evaluation-store descriptor
fingerprint; adding this website format does not make a pinned backfill binary incompatible.

## Publish separately

```bash
xlsynth_bvc publish-static-site --site-dir ../bvc-site --publish-root ../bvc-publication
xlsynth_bvc verify-published-site --publish-root ../bvc-publication
```

These commands stage an immutable local publication and pointer; uploading it remains an
explicit deployment step. Rebuilding or previewing a site never changes the production pointer.
