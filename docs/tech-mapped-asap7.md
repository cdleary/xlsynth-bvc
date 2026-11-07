# Tech mapped ASAP7 comparisons

`run-techmap-corpus` supplements a completed frozen IR corpus export with standard-cell area
and nominal combinational delay. It supports the full corpus, whole functions, and MFFC cones.
The results appear in the dashboard's **Tech mapped ASAP7** page.

The two implementation paths are:

- XLS IR → default g8r lowering → native ASAP7 mapping, with separate area and delay goals.
- XLS IR → XLS combinational Verilog → Yosys/ABC AIG → ABC ASAP7 mapping, with area-only
  and timing-oriented policies.

ABC's area-only policy uses `map -S 10 -a`; its timing-oriented policy uses `map -S 10`.
The default mapper first seeks a minimum-delay mapping and then recovers area under the
resulting timing requirement. We do not supply an explicit `-D` delay target. Thus the two ABC
points represent area-only and default timing-oriented mapping, rather than independent pure
area and pure delay optimizers. The `-S 10` setting controls library characterization slew.

Native driver 0.75.0 supports the area goal. Native driver 0.76.0 and later support both goals.
ABC supplies both reference policies for every release, including 0.75.0. Native lowering
uses `--fraig=false`; it does not reuse the frontend-disabled AIG from historical corpus exports.
For the ABC path, the runner verifies retained codegen/ABC provenance and reuses the AIG when
the XLS DSO version matches. It regenerates codegen for a different DSO, then reuses the retained
baseline only when the fresh Verilog bytes match its canonical digest and the ABC runtime, top,
and flow match. Otherwise it regenerates the baseline AIG as well.
Inputs remain frozen across releases, including the selected top function.

All mapped netlists use one pinned evaluator image, its verified release-cache inputs, and
`gv-stats` with a 10 ps primary-input transition and a 1 fF output load. The same deterministic
ASAP7 RVT SS library is used for both paths. Area is reported in library units and delay in ps;
these are nominal cell-level estimates without placement or routing. The small protobuf library
summary, license, and pinned source information live in [`third_party/asap7`](../third_party/asap7/VENDORED.md).

## Evaluate a release

Run the command with the deployed checkout or installed resource bundle as its working
directory. The runner reads its preparation helpers and Yosys flow from that resource root.
Keep output and cache directories outside the resource root. The first mapping run fetches the
pinned ASAP7 source libraries and reconstructs the verified bundle in the shared cache. Later
runs reuse it offline. Static-site rendering uses the measured protobuf results and never needs
to fetch or read the full library.

For offline mapping, preseed `$TECHMAP_CACHE/inputs/asap7/<archive-sha256>/bundle.tar.gz`
with the verified archive identified by `third_party/asap7/summary.textproto`. The runner checks
its bytes and derived metadata before reuse.

Start with a completed version-5 `run-ir-dir-corpus` export retaining its `.bvc` provenance.
Use an immutable driver image for the release under test and the same ABC and evaluator images
for every release in a comparison. Both release-cache directories must be verified cache leaves
containing `.ready.pb`, rather than their parent cache directories.

```bash
target/release/xlsynth_bvc run-techmap-corpus \
  --source-run-dir "$SOURCE_RUN" \
  --output-dir "$TECHMAP_RUN" \
  --cohort whole-functions-v1 \
  --version 0.79.0 --dso-version 0.61.0 \
  --driver-image "$DRIVER_IMAGE" --abc-image "$ABC_IMAGE" \
  --release-cache-dir "$RELEASE_CACHE" \
  --evaluator-image "$EVALUATOR_IMAGE" \
  --evaluator-version 0.79.0 --evaluator-dso-version 0.61.0 \
  --evaluator-release-cache-dir "$EVALUATOR_RELEASE_CACHE" \
  --shared-cache-dir "$TECHMAP_CACHE" \
  --workers "$WORKERS" --timeout-seconds 900
```

Use `--cohort full-corpus` or `--cohort mffc-v1` with the corresponding source export. The frozen
populations contain 32,806, 187, and 904 inputs respectively. `--limit` runs a pilot while retaining
the full expected population in the coverage metadata.

Runs hold an exclusive output lock and checkpoint each input to protobuf. Rerunning the same
command resumes completed inputs and retries failures. Shared content-addressed caches reuse
lowering, mapped netlists, and common evaluation across inputs and releases. `manifest.pb` is the
canonical execution/publication handoff; `summary.json` is an operational progress projection,
refreshed once a minute. Local logs retain detailed tool failures. A run with failed inputs exits
unsuccessfully and keeps its checkpoints for inspection and retry.

External commands write into unique `.attempt-*` directories beneath each cache entry. The host
validates successful outputs and publishes their canonical files and protobuf checkpoints while
holding the cache lock. A command left running after interruption can only change its abandoned
attempt; retries use new directories. Attempt directories retain operation logs for diagnosis.
On restart, the runner removes containers labeled for the same output directory before starting
replacements, and commands address immutable container IDs so delayed commands cannot attach to
a replacement.

## Render the comparison

Pass each run directory to a composed dashboard build alongside its existing historical inputs:

```bash
target/release/xlsynth_bvc build-static-site \
  --snapshot-dir "$SNAPSHOT" --out-dir "$SITE" --base-url / \
  --corpus-evidence-dir "$PREVIOUS_SITE/corpus" \
  --progression-run-dir "$WHOLE_FUNCTION_HISTORY" \
  --progression-run-dir "$MFFC_HISTORY" \
  --techmap-run-dir "$TECHMAP_RUN"
target/release/xlsynth_bvc verify-static-site --site-dir "$SITE"
target/release/xlsynth_bvc smoke-static-site \
  --site-dir "$SITE" --browser "$BROWSER" --timeout-seconds 60
```

Repeat `--techmap-run-dir` for each release/cohort. The renderer validates the typed evidence,
rejects mixed libraries or evaluators, and preserves partial coverage explicitly. Published
evidence and browser metric shards are bounded to 2 MiB each; the browser loads only the selected
release and corpus. Published protobufs use a separate public projection that omits local mapped
netlist paths and recomputes sample record digests. It retains an opaque SHA-256 commitment to the
original execution manifest, while the publication binds the complete public projection. Local
execution evidence and scientific browser measurements are unchanged.

The comparison embeds Plotly's pinned strict bundle so full-corpus WebGL plots work under the
page's content security policy. Browsers without WebGL show a deterministic overview of up to
2,000 inputs; summaries, search, and the paginated table retain the complete selected population.

The chart shows area ratio and delay ratio against a selectable same-input ABC reference. An
input's native area and delay results remain distinct endpoints; the joining line does not imply
that intermediate designs were measured. The per-input view shows the absolute measurements for
both native goals and both ABC mapping policies. Zero-area or zero-delay references remain
available in the table and absolute view when a ratio is undefined.

## Isolate the technology mapper

The **Mapper isolation** view separates the input AIG from the technology mapper. A supplemental
experiment maps the retained Yosys/ABC AIG with both g8r objectives and joins those results to the
original native-g8r and ABC measurements. The retained AIG already includes ABC Boolean
optimization; this comparison does not represent an ABC-free Yosys frontend.

Pass the experiment's canonical protobuf manifest alongside its exact baseline run:

```bash
target/release/xlsynth_bvc build-static-site \
  --snapshot-dir "$SNAPSHOT" --out-dir "$SITE" --base-url / \
  --corpus-evidence-dir "$PREVIOUS_SITE/corpus" \
  --techmap-run-dir "$BASELINE_TECHMAP_RUN" \
  --techmap-crossmap-run "$CROSSMAP_MANIFEST"
```

The importer validates the run, specification, and sample digests; exact baseline measurements
and AIG commitments; shared library and evaluator; and completed equivalence-proof commitments.
It publishes a separate protobuf projection without local attempt or netlist paths. Site
verification reconstructs the browser data from that evidence. The supplement preserves the
original run and its four measured endpoints.

Two interactive plots compare mappers on the same AIG and input AIGs under the same g8r objective.
Selecting a point or searching for a function reveals all six absolute measurements and their
measured Pareto frontier. Zero measurements remain visible in the table and absolute plot;
logarithmic overviews explicitly account for excluded zero metrics and undefined references.
Coverage describes the supplemental experiment only, independently of the broader release and
corpus coverage on the main comparison page.
