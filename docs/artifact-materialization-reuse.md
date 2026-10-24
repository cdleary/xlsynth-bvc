# Reusing promoted artifact files

After an immutable artifact transaction commits and its Sled flush succeeds, the
transaction winner atomically renames an eligible staging directory into the
disposable materialized cache. Downstream consumers can use the files already
produced by the executor instead of recreating them from the database.

The canonical protobuf provenance and file rows, transaction, and durable flush
are unchanged. The promotion's shared materialization lock remains held through
cache publication; materialization and provenance replacement take its exclusive
side. A conflicting promotion never publishes its losing files. Staging trees
containing symlinks, special files, or multiply linked regular files fall back to
DB-driven materialization. This includes only the regular files admitted to the
canonical store and gives them independent inodes, so writes through external
hard links cannot change the cached payload or provenance. Reuse requires Unix
link-count metadata; other platforms conservatively use DB-driven materialization.

Cache publication is best-effort and occurs only after the canonical commit is
durable. The cache has the same rebuildable, non-authoritative status as before;
this is not a new power-loss durability guarantee for materialized files. If the
cache is missing after restart, it is reconstructed from the database. A failure
to remove an obsolete cache is reported instead of accepting stale contents.

## Validation and measurement

Tests cover inode-preserving retention, first-writer preservation (including
concurrent conflicting promotions), provenance replacement, cache loss and reopen,
symlink exclusion, and isolation from external hard-link writes to both payload
and provenance. The status test explicitly starts with an empty cache to keep
verifying that progress queries do not materialize outputs.

An ignored promotion-and-consumption benchmark provides a common fixture:

```bash
BVC_PROMOTION_BENCH_ACTIONS=1024 BVC_PROMOTION_BENCH_THREADS=32 \
  cargo test --release --bin xlsynth_bvc \
  store::tests::benchmark_sled_promote_and_consume -- --exact --ignored --nocapture
```

For a before/after comparison, apply only the benchmark fixture and its `include!`
line to a baseline checkout, build both release test executables, and pin copies
outside their build directories. Run the same fixture in ABBA order with fresh
stores. The harness verifies payload bytes, typed provenance, and the canonical
record count, and reports the timed promote-and-consume throughput. It excludes
final store cleanup from the reported time.

Verify that the baseline executable excludes candidate-only tests. Reusing a build
directory across worktrees can incorrectly retain a fresh-looking artifact; use
separate clean build directories or invalidate the package artifacts before each
revision, and pin the resulting executables before building the other revision.

This isolates artifact promotion, not full evaluator goodput. Record concurrent
load and compare sustained completed-action rates separately after any live drain
and resume. Keep the corpus, runtime identities, cache, and concurrency unchanged.

### Initial results

These measurements predate the hard-link eligibility check.

Eight release-mode trials used 8,192 actions and 32 threads, in ABBA then BAAB
order, while the live evaluator continued running:

| Variant | Trial times (seconds) | Median |
| --- | --- | --- |
| Rebuild files after promotion | 18.00, 21.50, 19.10, 16.70 | 18.55 s |
| Retain promoted files | 15.06, 13.08, 14.16, 11.75 | 13.62 s |

This measured 1.36x promote-and-consume throughput (26.6% less elapsed time).
Earlier 1,024-action trials lasted only 2–3 seconds and did not show a win; the
longer balanced-order comparison is the useful result, not a whole-run speedup
claim. Payload and provenance checks passed in every trial. The full test suite
passed 462 tests with 10 ignored, including this manual benchmark.
