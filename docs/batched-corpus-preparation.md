# Batched corpus preparation

Corpus enqueue uses parallel groups of 32 inputs and queue-write batches of at most
64 distinct actions. This bounds memory, open record files, and held transition locks.
Worker execution and the persisted protobuf schema/path layout are unchanged.

## Durability and concurrency

- Resolve dependency aliases under the shared identity-migration lock.
- Acquire per-action transition locks in action-ID order to avoid overlapping-batch
  deadlocks. Preserve terminal states, existing enqueue timestamps, and maximum priority.
- Stage all new/updated records, then sync every staged file before any rename.
- Publish records with individual atomic renames. Sync each affected directory once,
  including newly created ancestor entries, children before parents.
- Return success and release transition locks only after these syncs succeed.
- A batch is not an all-or-nothing transaction. Interruption can publish a prefix.
  Resume validates existing imports and reconciles every planned action. Existing
  unchanged pending records are synced without rewriting their bytes, covering an
  interruption after rename but before the final directory sync.

The single-record writer remains in use for worker transitions. No durability flag
is disabled, and no format migration or cache-result substitution is required.

## Reproducing the benchmark

Build release binaries for the before/after revisions with identical release
metadata, runtime images, and recipes. Keep them at separate paths. For example:

```bash
python3 scripts/benchmark_corpus_preparation.py \
  --before BEFORE_BINARY --after AFTER_BINARY \
  --input-dir FROZEN_INPUT_DIRECTORY --out-dir NEW_BENCHMARK_DIRECTORY \
  --sample-count 512 --prepare-threads 4 \
  --driver-version CRATE_VERSION --stats-driver-version STATS_VERSION \
  --dso-version DSO_VERSION
```

The script copies a deterministic input subset, prepares fresh stores in ABBA order,
compares native manifests including every action identity and runtime, and checks
native action accounting. It never starts workers or mutates the source corpus.
Logs, stores, and a measurement-only `results.json` are retained for inspection.
Use a new output directory for each invocation; existing output is never overwritten.

Preparation timings include discovery, import, enqueue, and initial export. They
are not worker goodput or an end-to-end evaluation ETA. Record concurrent machine
activity, avoid extrapolating tiny cold stores to the full corpus, and measure resume
overhead before deciding whether an in-progress preparation should be restarted.

## Initial measurement

A 512-input subset of the frozen corpus, using four preparation threads and
identical released runtimes, produced these end-to-end enqueue timings:

| Variant | First trial | Second trial | Median |
| --- | ---: | ---: | ---: |
| Individual writes | 129.50 s | 129.81 s | 129.66 s |
| Batched writes | 40.04 s | 36.02 s | 38.03 s |

That is 3.41x preparation throughput (70.7% less elapsed time) in this measurement.
Another final-candidate fresh run took 35.04 s. Reopening a fully prepared baseline
store took 14.87 s; resuming an actually killed scratch process also reproduced the
same native manifest and all 4,608 planned actions (512 imports, 4,096 pending).
The live release preparation was running concurrently, so these are matched local
measurements, not portable performance guarantees or whole-evaluation speedups.

Validation includes batch publication ordering, injected failures and replay, path
validation, unchanged-file resume, protobuf equivalence to individual enqueue,
cross-batch deduplication, concurrent opposite-order batches, priorities, terminal
states, alias resolution, and the existing corpus partial-queue resume test.
