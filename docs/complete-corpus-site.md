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

When evaluating another release against an existing corpus, pass the existing estimator's
release as `run-ir-dir-corpus --stats-driver-version VERSION`. This pins all AIG statistics
actions independently of `--driver-version`; it does not change lowering or code generation.
Repeat the same flag when finalizing or resuming. An existing workspace rejects a different
resolved statistics runtime. Git candidates retain their captured baseline estimator and do
not accept this override.

### Faster repeated evaluation

`run-ir-dir-corpus --prepare-threads 4` imports inputs and enqueues independent
sample graphs concurrently. The default is four preparation threads; use `1` for
serial preparation. Each imported artifact and queue record is still committed
durably before preparation returns. Duplicate-content inputs are serialized, sample
order is deterministic, and rerunning after interruption fills in missing work.
This does not start evaluators concurrently with the preparation process.

To reuse downstream computations across release workspaces, seed an opt-in cache
from a completed canonical store, then pass that cache to the new workers:

```sh
xlsynth_bvc --store-dir PREVIOUS/.bvc/bvc-artifacts \
  --artifacts-via-sled PREVIOUS/.bvc/artifacts.sled \
  seed-downstream-cache --cache-dir SHARED_CACHE

env BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE=1 \
  xlsynth_bvc --store-dir CURRENT/.bvc/bvc-artifacts \
  --artifacts-via-sled CURRENT/.bvc/artifacts.sled \
  --downstream-cache-dir SHARED_CACHE \
  run-workers --workers 8 --exit-when-idle
```

Seeding requires exclusive access to the source sled database; use a completed,
idle workspace. Repeat `--action-id ID` to seed only selected completed actions.
For a fully enqueued fixed corpus, disable automatic suggested-action expansion
as above. Otherwise suggestions can add measurements using the latest estimator
in addition to the explicitly pinned estimator, doing work outside the corpus plan.
Successful new computations populate the cache automatically when it is enabled.
The new crate's lowering and code generation always run normally. Only
Verilog-to-Yosys/ABC, AIG-to-Yosys/ABC, and modern AIG statistics are eligible for
reuse. Keys bind exact input bytes, tool/runtime identity, flow script, frontend,
top module, language mode, and an execution-recipe revision. Legacy sidecar/IR
statistics are excluded. Different upstream action IDs alone do not prevent reuse.

Cache entries are atomic, self-contained protobufs with checked input/source
provenance and output length/SHA-256. Corrupt or incompatible entries are misses.
Cache writes intentionally do not add a per-entry durability flush; a crash can
lose cache entries, while canonical action commits retain their durability.
Hits retain the current action identity and dependencies, have no fabricated tool
command, and add `payload/downstream-reuse.pb` containing the original computation's
provenance. Original computations, not reuse chains, are seeded into the cache.
The cache can be discarded without losing canonical results; it must not be used
as a substitute for retaining completed stores. Its schema is compiled separately
and does not change the artifact-store descriptor fingerprint.

## Reuse completed release evidence

After the first build, reuse its typed publication evidence instead of reopening every completed
evaluation store. Add only newly finalized releases as raw run inputs:

```bash
cargo run --release --bin xlsynth_bvc -- build-static-site \
  --corpus-evidence-dir ../previous-site \
  --corpus-run-dir runs/newly-completed-release \
  --out-dir ../next-site
```

For an earlier all-versions dashboard, use `../previous-site/corpus` as the evidence directory.
The flag is repeatable and works without any `--corpus-run-dir`, including in the dashboard
command below. To create a reusable bundle for just one release, build a standalone corpus site
from that release once. Every release in each evidence directory is included; duplicate release
versions, unequal input populations, and incompatible measurement runtimes are rejected.

This is an explicit frozen publication input, not an automatic cache of a mutable run directory.
The reader consumes `site_manifest.v1.pb`, `corpus-site.pb`, and their bound `data/**/*.pb`
evidence shards. It checks canonical encoding, identities, checksums, paths, sample counts, and
cohort identity without accessing the original run or its sled database. Old HTML, JavaScript,
catalog JSON, and metric/IR JSON are neither read nor copied: current templates and projections
are regenerated, with the requested shard size and base URL. The completed output still receives
the full semantic, comparability, hosting-budget, and integrity verification before installation.

The source evidence is sufficient even after the original evaluation store has been archived.
Conversely, changes to that store do **not** update an existing publication artifact: explicitly
rebuild that release from `--corpus-run-dir` to include reevaluated results. There is no mtime-based
cache hit or implicit preference between duplicate releases. Keep evidence inputs and output
directories disjoint, and retain the source until the new build succeeds. This avoids millions of
small provenance/export reads, but does not skip validation of the resulting website.

Without a snapshot, existing function aliases and their provenance bindings are retained. With
`--snapshot-dir`, those bindings are validated and then refreshed from the supplied snapshot,
just as for run-directory inputs. Measurement evidence remains frozen in either case.

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

The chart defaults to pre-ABC g8r, with FRAIG disabled. Its stage selector also offers post-ABC
measurements; `?stage=raw` and `?stage=abc` preserve that selection in links. Each trend is the
change in summed AND-nodes × depth versus the first complete release at the selected stage.
Folding and structural hashing remain enabled; carry-out and priority-encode preparation rewrites
are disabled by the evaluation recipe. The latest-change card and chart links follow the stage.

Every plotted release uses the same complete input population. A missing measurement excludes
the entire release from that stage and creates a labeled gap, rather than reducing the sample
count or inserting zero cost. Partial historical populations remain accessible in detail views.
Counts across full-corpus, historical pairs, and fixed cohorts may overlap and must not be added
together. Fixed-cohort raw points open the pre-ABC graph-LE/node vector comparison; full-corpus
points open the release comparison with both sides at the selected stage.

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

Open `http://127.0.0.1:8000/`. On the dashboard, select a cohort and AIG stage, then click a release
point. In the full-corpus explorer, compare pre-ABC g8r with another release at the same stage,
or compare g8r+ABC with another release or its matched codegen+Yosys/ABC reference. Filter by input
kind, IR size, or positive product loss. Click a point/outlier to load its source IR. The cards include
zero-cost inputs; logarithmic plots explicitly omit zero-valued points. Undefined graph logical
effort is omitted only from that plot. In dashboard builds, the verified snapshot's structural
index supplies original function names and input-kind membership. The join first matches the
exact selected function bytes in the snapshot's IR evidence; directory names alone are not
evidence of structural identity. These aliases are persisted in typed publication protobufs.
Deduplicated inputs may be both whole functions and MFFCs/cones, so kind filters can overlap,
but “All inputs” still counts each evaluated input once. For example, a whole f32 adder whose
representative has an MFFC name remains visible under Whole functions and searchable by its
original name. The inspected IR and evaluated top remain unchanged.

When byte-identical functions have multiple historical structural hashes, the evidence retains
all of those identities and aliases; it does not arbitrarily pick one hash.
Standalone builds without a snapshot, and inputs not matched in a supplied snapshot, retain
explicitly labeled name-only MFFC/cone classification. Other names are unclassified, not claimed
to be whole-function provenance. Search accepts original names, evaluated names, or input paths.
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
