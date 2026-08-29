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

Build tools such as Cargo may write build outputs while constructing the
application. Those writes happen before deployment and are not production
runtime state.

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

Publication metadata follows the same content-idempotence rule as publication
identity. A snapshot's `generated_at` value is a deterministic data watermark:
the newest generation/update timestamp among its included datasets, campaign
runs, and analyses (or the Unix epoch when no included record has a timestamp).
Rebuilding unchanged inputs therefore reproduces the same snapshot and site
bytes even when the publication work directory has been lost.

## Campaign work-policy invariant

Release-campaign admission policy is canonical checked-in protobuf data and is
part of the campaign identity. An exclusion rule has a stable rule ID, an
explicit decision, one or more applicable action kinds, an exact top/module
name, and a reviewable reason. Production release exclusions must not be
hidden in environment variables or inferred from prior failures.

Rules are evaluated before a suggested action enters the queue. A match is
persisted as a typed `WORK_POLICY_EXCLUDED` queue cancellation containing the
rule ID; it is terminal for campaign traversal but is reported separately from
execution failures. Campaign manifests, public run protobufs, browser catalogs,
and static run pages preserve that distinction.

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
