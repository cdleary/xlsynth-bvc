# Build-machine deployment

`xlsynth-bvc` now has one release pipeline entrypoint:

```text
xlsynth_bvc --store-dir STORE --artifacts-via-sled STORE/artifacts.sled \
  coordinate-release --crate-version 0.X.0 --work-dir WORK \
  --publish-root PUBLIC --base-url /xlsynth-bvc/ --workers 8
```

The coordinator holds an advisory lock for the store, checkpoints each stage in
`STORE/coordinator/**/*.pb`, and safely resumes after interruption. It
plans/reconciles roots, drains workers, rebuilds datasets, finalizes the declared
completion contract, writes deterministic findings, verifies a protobuf
publication snapshot, verifies the static site, and only then promotes the
site. A completed dataset-index checkpoint is reused on retry only when its
fingerprints still match the exact current provenance inputs and web-index
output bytes. Rebuilt Sled indexes are flushed before checkpoint success. The
remaining content-producing stages preserve or derive stable timestamps when
their inputs are unchanged.

The initial host is deliberately filesystem/object-layout neutral. `PUBLIC`
contains immutable `sites/<site-id>/` trees, immutable
`catalogs/<site-id>.pb` records, the small `current.pb` plus browser-facing
`current.json` pointers, and a stable root `index.html` loader. The loader reads
`current.json` without caching and redirects to its immutable
`sites/<site-id>/` tree. Site-internal URLs are relative to that immutable tree,
so the same files work below the configured base URL without rewriting. Serve or
synchronize `PUBLIC` with any ordinary static host. Files below `sites/` and
`catalogs/` should receive long immutable cache lifetimes; `current.json` should
use no-cache or a short TTL. `.publication.lock` is an advisory lock file for
writers and need not be served.

## Fresh machine

1. Create a dedicated unprivileged runtime user.
2. Install Docker and grant only the access required by the existing hermetic
   driver/Yosys images.
3. Check out this repository at `RESOURCE_ROOT` and build with
   `cargo build --release`.
4. Make `RESOURCE_ROOT` readable but not writable by the runtime user. It is an
   immutable application-resource bundle containing flow scripts, Dockerfiles,
   vendored helpers, and the deployed compatibility map.
5. Create `STORE`, `WORK`, and `PUBLIC` outside `RESOURCE_ROOT`, owned by the
   runtime user. Do not copy the legacy JSON store; this deployment is
   protobuf-only.
6. From `RESOURCE_ROOT`, run two chosen historical versions manually with
   `coordinate-release` before enabling unattended publication.
7. Configure the operator's preferred scheduler or supervisor to invoke the
   same command directly. Scheduling policy is deliberately outside this
   repository.

The runtime account needs read access to `RESOURCE_ROOT` and read-write access
only to the configured store, sled database, work directory, publication root,
and Docker resources. The Rust-side locks are authoritative: overlapping
coordinators for one store fail cleanly, and overlapping publishers for one
publication root fail even when they originate from different stores.

Compatibility-map updates are out-of-band repository maintenance. A maintainer
runs `scripts/sync-version-compat.sh`, reviews the source change, and deploys a
new resource root. The coordinator never updates the checkout. If the deployed
map lacks a requested crate version, the command fails and leaves all resources
unchanged.

To discover work without changing the resource root, a scheduler can run:

```text
xlsynth_bvc --store-dir STORE --artifacts-via-sled STORE/artifacts.sled \
  list-pending-campaign-versions
```

It can then pass one selected version to the `coordinate-release` command shown
above. Bound the number of versions in scheduler policy rather than a repository
wrapper.

## Verification

Use these independent checks before pointing a CDN at the output:

```text
xlsynth_bvc verify-static-snapshot --snapshot-dir SNAPSHOT
xlsynth_bvc verify-static-site --site-dir SITE
xlsynth_bvc smoke-static-site --site-dir SITE
xlsynth_bvc verify-published-site --publish-root PUBLIC
xlsynth_bvc --store-dir STORE --artifacts-via-sled STORE/artifacts.sled \
  validate-store --verify-payloads
```

The published site needs no Rust process, sled files, queue files, or database
at request time. A plain static server is sufficient. `smoke-static-site`
starts such a server, checks page responses, and renders the root, runs, and
dataset pages in an installed headless Chrome/Chromium browser.
