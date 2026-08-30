# Recovery and rollback

## Failure model

The automatic procedures below cover process termination and container restart
while the operating system, filesystem, and storage remain healthy. They do not
promise transactionally ordered persistence across sudden host power loss,
kernel failure, filesystem corruption, or storage rollback.

After a host-level failure, stop writers and run `validate-store
--verify-payloads` plus `verify-published-site` before resuming. If validation
fails, restore `STORE` and its sled database from one consistent backup
generation. `WORK` is replaceable. `PUBLIC` can be restored from backup or
discarded and rebuilt from an intact store. An unmarked or otherwise invalid
store must not be relabeled in place without independent evidence that its
contents belong to the current protobuf format.

## Interrupted coordinator

Run the same `coordinate-release` command again. Queue insertion, action
execution, campaign reconciliation, snapshot/site construction, and immutable
publication are idempotent. The last stage outcome is recorded in the run's
protobuf coordinator state. Advisory locks disappear when the process exits;
there is no stale PID lock to remove.

The retry first resumes the unique in-progress campaign manifest for the
requested campaign and crate version. It does not re-resolve mutable planning
inputs before finding that manifest. If multiple in-progress generations
exist, rerun with `--run-id ID` to select the intended generation. Finalized
generations remain history; when none is in progress the coordinator binds the
currently declared runtime generation. The initially bound manifest is
persisted before the coordinator reports its planning stage as successful.

The coordinator also checkpoints the exact analysis baseline run ID and crate
version. An implicit baseline uses the latest-created finalized generation for
the preceding/requested version. Use `--baseline-run-id ID` for an exact
override. A retry fails rather than changing the checkpointed baseline; resume
with the baseline ID named by the diagnostic.

Publication attempts use unique staging directories. While holding the
publication-root lock, a retry removes abandoned staging entries for the same
content-addressed site before copying, so a container that restarts as PID 1
does not require manual staging cleanup.

A successful dataset-index checkpoint is reused instead of regenerating
timestamped browser projections only when its recorded provenance and web-index
output fingerprints match the current store. Missing or changed output rows
therefore force a rebuild even when action inputs are unchanged. Campaign and
analysis records retain their timestamps when their semantic content is
unchanged, and snapshot
`generated_at` is the deterministic newest-source watermark described in
`DESIGN.md`. Consequently, rebuilding a lost work directory from unchanged
store records reproduces the same snapshot manifest, site manifest, and
immutable publication identity.

## Static publication rollback

Immutable sites are never replaced. To roll back, select the previous
`catalogs/<site-id>.pb`, construct a matching `CurrentSitePointer`, and replace
`current.pb`, then replace the browser `current.json` pointer last. Keep at
least the two most recent immutable site trees and catalogs. Do not delete the
currently referenced site. Stop coordinators and standalone publishers that
target this publication root before a manual rollback; normal publication holds
`.publication.lock` across both pointer updates and verification.

An interrupted upload cannot damage the prior browser publication because the
new immutable site is fully copied and verified before either current pointer
is changed. If interruption occurs between the protobuf and JSON pointer
updates, the browser continues using the prior JSON pointer; rerun publication
to finish the pair. `verify-published-site` confirms the pointers, catalog, site
manifest, every declared file digest, local links, base URL, and absence of API
requests.

## Store loss

Back up the private protobuf store independently of the static output. Stop all
coordinator and worker processes, snapshot `STORE` and the sled database
together, then restart. Restore both from the same backup generation. The
static site is not a full private-store backup: operational provenance, queues,
and non-allowlisted artifacts are intentionally excluded.

If no store backup exists, initialize an empty directory and rerun campaigns
from their declared roots. Do not import the pre-protobuf JSON store.

## Static-site loss

If the action store is intact, rerun `finalize-campaign-run`,
`analyze-campaign-run`, `build-static-snapshot`, `build-static-site`, and
`publish-static-site`, or simply rerun `coordinate-release`. Content-addressed
IDs make unchanged recovered content easy to compare with backups.
