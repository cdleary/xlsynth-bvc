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
  --out-dir site
```

The command validates, converts the operational ingress to typed protobuf evidence, generates
the browser projection, verifies the entire output, and atomically installs `site`. It does not
enqueue work, refresh exports, migrate the evaluation store, or deploy. Stores must be idle:
their canonical sled databases cannot be opened while a worker owns them. Use `--overwrite` to
atomically replace an earlier site; a rejected build leaves the earlier site intact.

Inputs must use `g8r-abc-vs-yabc-aig-diff`, with no fraiging or Git-candidate override. All samples
must have complete import, lowering, ABC, codegen, reference, raw/post-ABC stats, and diff actions.
The builder checks every action's identity and provenance, stats export byte digests, and input
IR content hashes. All included releases must contain exactly the same relative paths, source
bytes, and top functions, and use the same stats estimator and Yosys/ABC runtime/script. Each
generation has one lowering runtime; its own DSO is recorded and bound to its action graph.

`--snapshot-dir`, `--progression-run-dir`, and `--candidate-run-dir` cannot be combined with this
mode. The normal snapshot-based site remains available separately.

## Preview and inspect

```bash
python3 -m http.server 8000 --bind 127.0.0.1 --directory site
```

Open `http://127.0.0.1:8000/`. Choose the release, compare its G8r+ABC result with either its
matched codegen+Yosys/ABC reference or another release's G8r+ABC result, and filter by input kind,
IR size, or positive product loss. Click a point/outlier to load its source IR. The cards include
zero-cost inputs; logarithmic plots explicitly omit zero-valued points. Undefined graph logical
effort is omitted only from that plot. Input-kind labels follow corpus top-function conventions.

For an explicit independent audit or after copying the site:

```bash
xlsynth_bvc verify-static-site --site-dir site
xlsynth_bvc smoke-static-site --site-dir site
```

The build already performs the complete static verification. Browser smoke is separate and
requires Chrome/Chromium. `--base-url /prefix/` supports subpath hosting, and assets use relative
URLs so the existing immutable publication wrapper also works.

The optional integration test checks every included release's plot population, paired deltas,
rapid filter changes, source-IR selection, and mobile sizing. With a local server running:

```bash
node scripts/test_corpus_site_browser.cjs http://127.0.0.1:8000/ screenshots
```

It uses a disposable headless Chrome profile (`BVC_CHROME` can select the executable).

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
xlsynth_bvc publish-static-site --site-dir site --publish-root publication
xlsynth_bvc verify-published-site --publish-root publication
```

These commands stage an immutable local publication and pointer; uploading it remains an
explicit deployment step. Rebuilding or previewing a site never changes the production pointer.
