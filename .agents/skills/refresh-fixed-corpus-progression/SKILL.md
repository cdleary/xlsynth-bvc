---
name: refresh-fixed-corpus-progression
description: Compute, resume, publish, or diagnose xlsynth-bvc fixed-corpus progression comparisons for release and Git generations. Use for whole-functions-v1 or mffc-v1 backtests, release-vs-release or release-vs-HEAD plots, progression ETAs, and local progression website refreshes.
---

# Refresh fixed-corpus progression

Keep computation, export finalization, site construction, and browser validation as explicit
stages. Report status and ETA for each stage separately.

## Choose a comparable recipe

- Release generations that will be compared with Git revisions must use
  `g8r-abc-vs-yabc-aig-diff`.
- Exact Git revisions use `g8r-abc-stats` with `--driver-git-commit`.
- Use `release-progression-ir-v1` for `whole-functions-v1` and
  `mffc-progression-ir-v1` for `mffc-v1`.
- Keep DSO, Yosys/ABC runtime, Yosys script, and released stats-driver compatible across a pair.
- Never treat `g8r-vs-yabc-aig-diff` G8r metrics as comparable to the post-ABC Git path.

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
generation as typed `FixedCorpusProgressionRunEvidence` protobuf. Keep protobuf through the
publication pipeline; emit JSON only for the final browser catalog and data projections.

## Build the site

When checked-in release metadata, cohort definitions, or publication recipes changed, first build
a fresh static snapshot from the canonical artifact store. The current repository definitions
determine the release universe. Prior snapshots and derived web indices are caches only: never
overlay current release metadata onto an old snapshot in the site renderer. The optional
`--skip-rebuild-web-indices` path is appropriate only when the cached indices validate against the
current metadata and resolved recipe identities.

Pass every completed release or Git workspace directly to one site build:

```bash
xlsynth_bvc build-static-site \
  --snapshot-dir SNAPSHOT_DIR \
  --out-dir SITE_DIR \
  --progression-run-dir RELEASE_RUN_DIR \
  --progression-run-dir GIT_RUN_DIR
```

The flag is repeatable. Do not use the hidden legacy `--candidate-run-dir` alias in new workflows.
The builder validates and verifies its staged output before installation. Do not immediately repeat
a full `verify-static-site` or `smoke-static-site` unless an independent audit is requested or the
installed bytes changed.

## Validate the requested comparison

Open `progression.html` with the intended `cohort`, `baseline`, and `current` generation IDs. Check:

- release and Git/release labels identify the exact two generations;
- both are cohort-complete and have the expected artifact count;
- summed-product and distribution views use the same paired fixed-artifact population;
- the largest-change heading states the baseline and current generations explicitly; and
- every added generation has `data/progression-runs/<generation-id>/evidence.pb`, while
  `catalog.json` is only the verified browser projection.
