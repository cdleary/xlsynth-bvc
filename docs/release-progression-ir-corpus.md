<!-- SPDX-License-Identifier: Apache-2.0 -->

# Fixed IR progression cohorts

The progression chart uses named, pinned benchmark cohorts rather than the functions enumerated by each xlsynth release. Every release and evaluated Git revision becomes a generation inside a cohort, and any two generations in that cohort can be compared with the same aggregate and per-artifact views.

The initial cohorts are:

- `whole-functions-v1`: 187 sorted, unique whole-function structural hashes in `src/site_assets/release_progression_ir_hashes.txt`.
- `mffc-v1`: 904 sorted, unique MFFC structural hashes in `src/site_assets/mffc_progression_ir_hashes.txt`.

Each companion `*_artifacts.tsv` binds a structural hash to the SHA-256 of the exact canonical IR bytes. The validator hashes each sorted pair with the domain prefix `xlsynth-bvc/ir-dir-corpus-artifact-manifest/v1\0`; a renamed, truncated, or replaced IR file is rejected before enqueue or publication. `src/site.rs` also pins each member count and domain-separated cohort digest, so changing either population is an explicit versioned decision.

The MFFC cohort additionally retains its extraction lineage in `src/site_assets/mffc_progression_sources.jsonl`: source function/action identities, source structural hash, extracted top, originating crate versions, and occurrence count.

## Scheduling policy

Cohort identity stays separate from operational scheduling. Both policies validate the complete exact-byte artifact manifest before enqueue:

```text
--scheduling-policy release-progression-ir-v1  # whole-functions-v1
--scheduling-policy mffc-progression-ir-v1     # mffc-v1
```

The whole-function policy also assigns a queue-priority boost to known persistent Yosys/ABC stragglers. The MFFC policy currently changes no relative priorities; it provides the same fail-closed cohort validation and candidate-run identity for the larger suite.

Policies are accepted only with `--execution-mode enqueue`. The run records the policy name, semantic version, compiled-config digest, tiers, and reasons in `manifest.json`. Queue priority is operational metadata rather than action identity, so selecting a policy does not change cache keys or QoR results.

## Whole-function cohort origin

The cohort was captured on 2026-09-03 from a production static-site snapshot whose `catalog.json` SHA-256 is `5a99a8efcc222687995a12ef0fe1a7f70dfd5ab87812b3c895df3c5943aaeeb2`. The snapshot's `ir-fn-corpus-ir.v1.json` descriptor SHA-256 is `d08e478594386e648347757ac9bef9d5d652d71a9752c479848b54ebc3bc0aba`; its paired `ir-fn-corpus-g8r-abc-vs-codegen-yosys-abc.v1.json` descriptor SHA-256 is `cd778dec73387f41ddf137616a3ad2fced7290f488fb86a215b93a3633e59246`.

It is the common set of exact whole-function structural hashes with paired G8r and Yosys/ABC measurements in both of these complete generations:

- xlsynth crate `0.66.0`, DSO `0.54.7`: 187 artifacts
- xlsynth crate `0.68.0`, DSO `0.54.7`: 187 artifacts

The two sets were identical. Requiring each indexed IR action to equal its source IR action excludes generated k3 cones and MFFCs. The corresponding canonical IR packages were materialized one function per file, named `<structural-hash>.ir`, and are the inputs to historical backtests. The checked-in manifest's raw SHA-256 is `bd6a384406f764baebe95089b792fe483ef84f0a8eda3ee8ea8047767a77ce38`; its code-pinned domain-separated digest is `a70a2e38b978d07b8bfc642f7a7cd6806a35bfa4de52f8c9919cd880057e2f77`.

## MFFC cohort origin

`mffc-v1` was captured on 2026-09-04 from the structurally deduplicated canonical A/B corpus at `/tmp/xlsynth-reassoc-reuse-backtest.3IMB2v/canonical-corpus-manifest.json`. The source snapshot contained 34,076 unique structural functions: 33,172 whole-function/k=3 entries and 904 MFFCs. Only the 904 records whose manifest kind is `mffc` enter this cohort.

The checked-in source-lineage JSONL has raw SHA-256 `6b2f583fb147ad893dd0bedb778b8207ad49f3f550f4f73850dfea4cb0f36641`. The cohort identity is `f80befb2248a9757b7512068a4818308ecff1e77bcd78701b1a67338f979e5f8`; its exact-byte artifact-manifest identity is `cfc36afcd8b178687690a03d6c8b555e9517e7606ae8062d502452cf29e8861c`.

To materialize a runnable directory from that retained corpus:

```bash
source_root=/tmp/xlsynth-reassoc-reuse-backtest.3IMB2v/canonical-corpus
corpus_out=/tmp/xlsynth-bvc-mffc-progression-v1
mkdir -p "$corpus_out"
while IFS=$'\t' read -r structural_hash source_sha256; do
  source_file="$source_root/$structural_hash.ir"
  test "$(sha256sum "$source_file" | cut -d' ' -f1)" = "$source_sha256"
  cp "$source_file" "$corpus_out/$structural_hash.ir"
done < src/site_assets/mffc_progression_ir_artifacts.tsv
```

Then evaluate releases with `--recipe-preset g8r-abc-vs-yabc-aig-diff` and exact Git
revisions with `--recipe-preset g8r-abc-stats`. Both use
`--top-fn-policy infer-single-package` and `--scheduling-policy mffc-progression-ir-v1`.
The release recipe is intentionally matched: its G8r result passes through the same Yosys/ABC
runtime and script as the Git path. Raw `g8r-vs-yabc-aig-diff` release results are not comparable
and publication rejects them. Completed actions are cache-addressed and runs are resumable.

## Publication workflow

Treat computation, export finalization, static-site construction, and browser inspection as
separate stages:

1. Enqueue the exact fixed cohort and start workers against `OUTPUT_DIR/.bvc/`. Use
   `show-corpus-progress --output-dir OUTPUT_DIR` to check counts, throughput, and failures; do not
   infer compute progress from a site build.
2. After workers are idle, rerun the exact `run-ir-dir-corpus` command once to refresh the public
   manifest and exported stats. Require the full cohort to be `done` with zero failed, missing, or
   extra samples. Do not merge or hand-edit JSON/JSONL files.
3. Pass every completed release or Git output directory directly to `build-static-site` with a
   repeated `--progression-run-dir DIR`. The renderer validates the operational manifest, fixed
   cohort, complete action graph, and provenance-backed stats, then writes one typed
   `data/progression-runs/<generation-id>/evidence.pb` record per generation. JSON is created only
   as the final browser catalog/projection.
4. Inspect the progression page with the intended cohort and explicit baseline/current generation
   IDs. Confirm both labels, completeness, summed-product delta, distribution plot, and largest
   per-artifact changes.

`build-static-site` verifies its staged output before installing it. Do not immediately run a
second full `verify-static-site` or `smoke-static-site` unless an independent audit is requested or
the installed bytes changed; use a targeted browser check for the selected comparison instead.
Report stage-specific ETAs: queue drain time is distinct from export refresh, site build, and
browser validation.

Example publication command:

```bash
cargo run --bin xlsynth_bvc -- \
  build-static-site \
  --snapshot-dir /path/to/current/snapshot \
  --out-dir /tmp/xlsynth-bvc-progression-site \
  --progression-run-dir /tmp/mffc/releases/0.70.0-g8r-abc \
  --progression-run-dir /tmp/mffc/candidates/HEAD
```

## Reproducing the manifest

Given the verified snapshot site directory, this command reproduces the manifest byte-for-byte:

```bash
snapshot_site=/path/to/verified-snapshot/site
manifest_out=/tmp/release_progression_ir_hashes.txt
jq -s -r '
  [.[].entries[]
   | select(.crate_version == "0.66.0" or .crate_version == "0.68.0")
   | select(.g8r.source_structural_hash == .yosys_abc.source_structural_hash)
   | select(.g8r.ir_action_id == .g8r.source_ir_action_id)
   | select(.yosys_abc.ir_action_id == .yosys_abc.source_ir_action_id)
   | {crate_version, hash: .g8r.source_structural_hash}]
  | group_by(.hash)
  | map(select(([.[].crate_version] | unique | length) == 2))
  | .[] | .[0].hash
' "$snapshot_site"/data/ir-fn-corpus-ir.v1/by-hash-prefix/*.json \
  | sort -u > "$manifest_out"
test "$(wc -l < "$manifest_out")" -eq 187
sha256sum "$manifest_out"
```

This command then materializes the exact single-top packages from the `0.68.0` copy of each identical whole-function entry:

```bash
corpus_out=/tmp/release-progression-ir-corpus
mkdir -p "$corpus_out"
jq -s -r --rawfile manifest "$manifest_out" '
  ($manifest | split("\n") | map(select(length == 64))) as $wanted
  | [.[].entries[]
     | select(.crate_version == "0.68.0")
     | select(.g8r.ir_action_id == .g8r.source_ir_action_id)
     | select(.g8r.source_structural_hash as $hash | $wanted | index($hash))]
  | unique_by(.g8r.source_structural_hash)
  | .[]
  | {hash: .g8r.source_structural_hash, ir_text: .g8r.ir_text}
  | @base64
' "$snapshot_site"/data/ir-fn-corpus-ir.v1/by-hash-prefix/*.json \
  | while IFS= read -r row; do
      hash=$(printf '%s' "$row" | base64 --decode | jq -r .hash)
      ir_text=$(printf '%s' "$row" | base64 --decode | jq -r .ir_text)
      printf 'package fixed_%s\n\n%s\n' "$hash" "$ir_text" > "$corpus_out/$hash.ir"
    done
```

The materialized directory contains 187 files. Hashing each file with its hash-only filename in sorted order produces `983fab9ccbfc6d6cb3ce112215730203b0c7731514281ebd4610a61cd82fc6a6`. Run the `g8r-abc-vs-yabc-aig-diff` corpus recipe against that directory for every historical
crate/DSO pair that will be compared with direct G8r+ABC Git evaluations.

Each checked-in named manifest is the benchmark identity. Do not replace it merely because a newer release adds or removes input functions; create and review a new cohort version when intentionally changing the benchmark.
