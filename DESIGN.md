# xlsynth-bvc Design

## Runtime filesystem boundaries

`xlsynth-bvc` separates immutable program resources from mutable private state
and public output:

| Boundary | Examples | Runtime access |
| --- | --- | --- |
| Resource root | executable checkout, flow scripts, Dockerfiles, vendored helpers, checked-in compatibility map | read-only |
| Private state | protobuf store, sled database, queues, caches, coordinator state, publication work directory | read-write |
| Public output | immutable sites and catalogs, current-site pointers | publication-only writes |

The current executable uses its working directory as the resource root. A
deployment therefore starts the binary with the deployed checkout or installed
resource bundle as its working directory, while passing store, sled, work, and
publication paths outside that directory.

### Read-only resource invariant

Normal production commands may read the resource root but must not create,
modify, or delete files beneath it. New runtime state belongs in an explicitly
configured private-state or public-output path. A feature that requires a
periodic source-tree write violates this invariant and must be redesigned or
classified as out-of-band maintenance.

Repository-maintenance tools are outside the invariant's runtime side. For
example, `scripts/sync-version-compat.sh` deliberately updates the checked-in
crate-to-xlsynth compatibility map. That operation is performed by a
maintainer, reviewed as a source change, and deployed normally; the coordinator
does not invoke it. A deployed binary whose map does not contain a requested
crate version fails with an actionable diagnostic rather than updating itself.

The same rule applies to upstream release bytes. The checked-in
`release-inputs/v1.textproto` file binds every deployed DSO version to the
SHA-256 of its stdlib archive and the exact source commit. It is compiled into
the binary. Root action identities carry those values, stdlib downloads must
match the declared digest, and source archives are fetched by commit rather
than mutable tag. `scripts/sync-release-inputs.sh` is the out-of-band maintainer
operation; the coordinator never rewrites or refreshes the lock.

Build tools such as Cargo may write build outputs while constructing the
application. Those writes happen before deployment and are not production
runtime state.

## Supported recovery and host-failure non-goal

`xlsynth-bvc` supports recovery from process-level interruption while the
operating system, filesystem, and storage remain healthy. A killed command or
restarted container can be rerun: advisory locks are released by process exit,
stale application-owned staging is recoverable, and persisted checkpoints and
content identities make normal retries idempotent.

Fresh-store bootstrap follows the same process-restart contract. Initialization
serializes through an advisory lock and promotes a fully encoded format marker
from application-owned staging, so an interrupted initializer leaves the final
marker absent rather than truncated and a retry can discard the staging file.
This atomic replacement is not an `fsync` protocol and does not expand the
host/storage durability claim below.

The system does not claim end-to-end transactional durability across sudden
host power loss, kernel failure, filesystem corruption, storage rollback, or
persistence reordering between sled and separate filesystem objects. Atomic
renames and explicit sled flushes reduce exposure but do not constitute an
`fsync` protocol covering every file and parent directory. After a host-level
failure, operators must validate the store and publication before reuse and may
need to restore a consistent backup or discard and rebuild `STORE`, `WORK`, or
`PUBLIC` from declared inputs.

This non-goal is limited to host/storage failure. It does not relax fail-closed
public-data validation, ordinary concurrent-execution correctness, process-
restart recovery, or the read-only resource invariant.

Queue terminal evidence follows a terminal-first transition protocol. A pending
action is reserved through its unique running path before cancellation is
written; after the durable canceled record exists, pending and reservation
records are removed. Failure is likewise written before its running lease is
removed, and success clears stale failure/cancellation evidence before marking
done. Lease recovery never requeues an action with terminal evidence, while
validate-store rejects unresolved active/terminal overlaps and incompatible
terminal states. This protocol covers process interruption without claiming the
host power-loss durability excluded above.

## Canonical data and publication boundary

Canonical Rust-side identities and persisted operational records use validated
protobuf messages. JSON remains a boundary format for external tools and APIs,
CLI projections, and browser-facing static datasets. Boundary JSON is parsed
into typed data before it influences identities or persistent state.

The private action store is content-addressed and is not served directly. The
publication pipeline selects an allowlisted protobuf snapshot, projects the
browser datasets, builds a verified static site, and promotes immutable site
and catalog objects before updating the small current-site pointers. Static web
serving requires neither the Rust process nor sled.

The public dataset projection is fail-closed: only explicitly named index keys
and structurally validated content-addressed group keys may enter a snapshot.
Adding a private index to the store does not publish it by default. Public
schemas must not contain build-host paths such as store or work directories.
Every allowlisted JSON key has an exact typed Rust schema at the browser
boundary. Snapshot construction, snapshot verification, and static-site
verification decode that schema, reject unknown, omitted, or non-canonical
fields, apply dataset-specific semantic validation, and require the canonical
encoding before trusting the bytes. Structural index files additionally require
their input bytes to already be in canonical form so their manifest-bound
content digests remain stable.

Raw executor and discovery error strings are private operational data. Public
failure and enumeration records use fixed enums and structured counters, such
as `timeout`, `failed`, or `discovery_failed`, never truncated or redacted
fragments of arbitrary process output. Public failure rows intentionally contain
only the action ID, controlled action-kind vocabulary, validated version, time,
and failure class; they do not synthesize a display subject from arbitrary
action arguments. Validation errors likewise identify the field contract
without echoing a rejected value that may contain private text.

Publication metadata follows the same content-idempotence rule as publication
identity. A snapshot's `generated_at` value is a deterministic data watermark:
the newest generation/update timestamp among its included datasets, campaign
runs, and analyses (or the Unix epoch when no included record has a timestamp).
Rebuilding unchanged inputs therefore reproduces the same snapshot and site
bytes even when the publication work directory has been lost.

Publication promotion is serialized by an advisory lock in the publication
root. The lock spans immutable site and catalog promotion, both current-site
pointers, and final verification. This is separate from the per-store
coordinator lock: publishers using different stores but the same public root
must not interleave their pointer updates.
Publication attempts use unique staging names. While holding the same lock, a
retry may remove abandoned staging entries for its validated content-addressed
site ID; process IDs are not used as persistent ownership evidence.

## Content-addressed store invariant

An action ID has exactly one committed action identity and output-file set.
Initial promotion is first-writer-wins. A concurrent or repeated promotion for
an existing ID may verify or discard its staging data, but it must never replace
the committed action or output contract. Sled commits the provenance and
complete file set in one transaction so readers cannot observe a mixed action
assembled from competing writers.

The one permitted provenance update is typed discovery enrichment after a
transient enumeration failure. It must go through the artifact-store API,
preserve action identity, dependencies, output artifact, and output-file
manifest, and atomically update both canonical Sled representations before
invalidating materialized cache state. Replacement and materialization are
serialized so an in-flight reader cannot republish stale cache state. Direct
writes to a materialized `provenance.pb` are forbidden.

Dataset-index checkpoints are valid only for the exact canonical provenance
inputs and exact web-index outputs from which they were built. The coordinator
records deterministic fingerprints of action IDs/encoded provenance and the
sorted web-index key/byte set after reconciliation and queue drain. Rebuilt Sled
outputs are flushed before Indexed success is recorded. Reuse requires
both fingerprints to match; new actions, provenance changes, missing outputs,
and modified output bytes invalidate the checkpoint.

The structural corpus index is a manifest-last generation. Its manifest lists
the exact group key set and binds every group to its structural hash, canonical
relative path, encoded-byte digest, member count, and node-count metadata.
Freshness checks and static snapshot build/verification require exact
manifest-to-group closure. A missing, extra, malformed, or concurrently partial
generation is stale or fails closed rather than being checkpointed or
published.

## Campaign work-policy invariant

Release-campaign admission policy is canonical checked-in protobuf data and is
part of the campaign identity. An exclusion rule has a stable rule ID, an
explicit decision, one or more applicable action kinds, an exact top/module
name, and a reviewable reason. Production release exclusions must not be
hidden in environment variables or inferred from prior failures.

Rules are evaluated before a suggested action enters the queue. A match is
persisted as a typed `WORK_POLICY_EXCLUDED` queue cancellation containing the
rule ID and a domain-separated fingerprint of the complete normalized rule; it
is terminal for campaign traversal but is reported separately from execution
failures. Campaign manifests, public run protobufs, browser catalogs, and static
run pages preserve that distinction.

A policy cancellation is evidence about the campaign policy that created it,
not a permanent ban on the action ID. Reconciliation removes the cancellation
when the current manifest no longer contains the matching rule and then admits
the action normally. Reconciliation refreshes the evidence when a stable rule
ID changes content or source. Completion accepts an intentional skip only when
the stored rule fingerprint, reason, and action match the current manifest, and
publishes the current rule's reason. Pending-release discovery likewise
compares current planned run identities, not crate-version strings alone.

Changing a rule requires a campaign semantic-version bump. Changing its public
projection also requires a publication-policy-version bump. This makes a policy
change explicit in content identities and avoids silently reinterpreting an
existing campaign run.

## Operational ownership

The binary owns queue leases, the per-store coordinator lock, stage
checkpoints, validation, and atomic publication promotion. It does not own
recurrence or process supervision. Operators may invoke it manually or through
systemd, cron, CI, a container scheduler, or another supervisor without
changing the data contract.

See `docs/hermetic-action-design.md` for action execution details and
`docs/build-machine-deployment.md` for the concrete filesystem contract.
