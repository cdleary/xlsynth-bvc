---
name: refresh-fixed-corpus-progression
description: Evaluate new xlsynth-bvc crate releases across full-corpus, whole-functions-v1, and mffc-v1; compute, resume, publish, or diagnose release and Git progression comparisons, ETAs, and local website refreshes.
---

# Refresh fixed-corpus progression

Keep computation, export finalization, site construction, and browser validation as explicit
stages. Report status and ETA for each stage separately.

## New-release coverage contract

When asked to process or evaluate a new crate release without narrowing the cohort, include all
three evaluation populations:

| Site view | Input population | Site-build input |
| --- | --- | --- |
| `full-corpus` | Frozen full-corpus replay inputs | `--corpus-run-dir` |
| `whole-functions-v1` | Registered `release-progression-ir-v1` manifest | `--progression-run-dir` |
| `mffc-v1` | Registered `mffc-progression-ir-v1` manifest | `--progression-run-dir` |

These are independently frozen populations. The name "full corpus" does not mean it contains
the exact inputs of the other two, and those views are not filters of the full-corpus run. Derive
expected counts and identities from each population's pinned manifest; do not infer coverage
from function names, matching counts, or completion of another population. Reuse work only
through verified content-bound action identities or the verified downstream cache.

Before enqueueing, record each population's workspace, pinned input identity, expected count,
and completion state. Resume existing compatible work and enqueue missing work for all three.
Report progress per population. Call the release's evaluation complete only after all three
have finalized exports, every expected sample and planned action is done, and no failed or
missing samples/artifacts remain. If only one finishes, name that population explicitly.

Respect an explicitly narrower request. Status, diagnosis, and render-only requests do not
authorize new evaluations: report any missing populations instead of silently starting them.
Evaluation completion does not imply the site was refreshed or authorize deployment.

## Choose a comparable recipe

- Release generations that will be compared with Git revisions must use
  `g8r-abc-vs-yabc-aig-diff`.
- Exact Git revisions use `g8r-abc-stats` with `--driver-git-commit`.
- Use `release-progression-ir-v1` for `whole-functions-v1` and
  `mffc-progression-ir-v1` for `mffc-v1`.
- Keep DSO, Yosys/ABC runtime, Yosys script, and released stats-driver compatible across a pair.
- Never treat `g8r-vs-yabc-aig-diff` g8r metrics as comparable to the post-ABC Git path.

Before enqueue, verify the manifest policy's cohort count and artifact-manifest digest. Use a new
output directory for a different release, commit, DSO, runtime, or immutable candidate identity.
Completed actions are cache-addressed, so resume the existing workspace when the identity is the
same. If correcting only the recipe, preserve the original output and seed a new workspace from
its `.bvc` store with a same-filesystem reflink/copy when practical.

## Compute and monitor

Run `run-ir-dir-corpus` once to enqueue. Run long workers in `tmux` using a
worker/persistent-runner configuration measured for the current host. Keep operator- and
host-specific tuning outside this repository.

Use this command for truthful progress:

```bash
xlsynth_bvc show-corpus-progress --output-dir OUTPUT_DIR --throughput-window-seconds 300
```

Check total/done/pending/running/failed sample counts and planned/done/failed action counts. Do not
call a run complete because a stale `summary.json` exists. Do not report a single ETA that mixes
queue drain time with export or rendering time.

Pin the same built binary across enqueue, worker drain, and the final refresh; do not rebuild it
while a corpus workspace is active. The artifact-store marker fingerprints the complete protobuf
descriptor set, so even adding publication-only messages changes that fingerprint. Prefer
finalizing active runs before a protobuf rebuild. If an interrupted run must cross a provably
wire-compatible additive schema change, add only the exact prior descriptor digest to the
checked-in compatibility allowlist, cover the atomic marker upgrade with a test, and let the binary
upgrade it. Never hand-edit or delete `store-format.pb`.

## Finalize once

After the worker pane is idle, rerun the exact `run-ir-dir-corpus` command once. This refreshes
`manifest.json`, `samples.jsonl`, exported stats, and joined projections from the canonical store.
Require the pinned cohort count, every sample `done`, and zero failed/missing/extra samples. Do not
hand-stitch JSON or run a separate merge script.

Operational JSON is an ingress adapter. The site builder must validate it and persist each admitted
fixed-cohort generation as `FixedCorpusProgressionRunEvidence` protobuf, and full-corpus releases
as `CorpusSite` and `EvidenceShard` protobufs. Keep protobuf through the publication pipeline;
emit JSON only for the final browser catalog and data projections.

## Build the site

When checked-in release metadata, cohort definitions, or publication recipes changed, first build
a fresh static snapshot from the canonical artifact store. The current repository definitions
determine the release universe. Prior snapshots and derived web indices are caches only: never
overlay current release metadata onto an old snapshot in the site renderer. Reuse canonical store
results through their content-bound action identities, but rebuild derived publication indices.
Do not use `--skip-rebuild-web-indices` after release metadata, cohort definitions, or recipes
change.

For an all-versions dashboard refresh, explicitly include earlier releases as well as the new
release's completed full-corpus and both fixed-cohort workspaces. Builds do not inherit releases
from the existing output directory or the snapshot. Reuse the previous dashboard's verified
`corpus/` evidence to retain all its full-corpus generations:

```bash
xlsynth_bvc build-static-site \
  --snapshot-dir SNAPSHOT_DIR \
  --out-dir SITE_DIR \
  --corpus-evidence-dir PREVIOUS_SITE_DIR/corpus \
  --corpus-run-dir NEW_FULL_CORPUS_RUN_DIR \
  --progression-run-dir PRIOR_WHOLE_FUNCTION_RUN_DIR \
  --progression-run-dir PRIOR_MFFC_RUN_DIR \
  --progression-run-dir NEW_WHOLE_FUNCTION_RUN_DIR \
  --progression-run-dir NEW_MFFC_RUN_DIR
```

Repeat the prior fixed-cohort arguments for every completed workspace to retain, not just
one earlier release. All three input flags are repeatable. If no previous evidence is available
(including the first build), omit `--corpus-evidence-dir` and repeat `--corpus-run-dir` for every
earlier completed full-corpus release as well as the new release. Evidence and run inputs must
cover disjoint release sets; supplying a run already in the evidence is a duplicate, not an
override. Keep source evidence and output directories disjoint.

Add completed Git workspaces with `--progression-run-dir` when requested. Do not use the hidden
legacy `--candidate-run-dir` alias in new workflows.
The builder validates and verifies its staged output before installation. Do not immediately repeat
a full `verify-static-site` or `smoke-static-site` unless an independent audit is requested or the
installed bytes changed.

## Validate the requested comparison

For a new-release dashboard refresh, verify that the release appears as complete in all three
cohort selectors/trends and that each uses its own expected population. Check the generated
catalog and the rendered views, and compare retained release sets with the prior publication
so that earlier releases cannot silently disappear. Successful full-corpus rendering alone is
not sufficient. If a render-only request encounters unevaluated cohorts, explicitly identify
their missing coverage and last evaluated release in the handoff rather than describing the
site as fully up to date.

Open `progression.html` with the intended `cohort`, `baseline`, and `current` generation IDs. Check:

- release and Git/release labels identify the exact two generations;
- both are cohort-complete and have the expected artifact count;
- summed-product and distribution views use the same paired fixed-artifact population;
- the largest-change heading states the baseline and current generations explicitly; and
- every added generation has `data/progression-runs/<generation-id>/evidence.pb`, while
  `catalog.json` is only the verified browser projection.
