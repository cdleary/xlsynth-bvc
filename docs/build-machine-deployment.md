# Build-machine deployment

`xlsynth-bvc` now has one release pipeline entrypoint:

```text
xlsynth_bvc --store-dir STORE --artifacts-via-sled STORE/artifacts.sled \
  coordinate-release --crate-version 0.X.0 --work-dir WORK \
  --publish-root PUBLIC --base-url /xlsynth-bvc/ --workers 8
```

The coordinator holds an advisory lock for the store, checkpoints each stage in
`STORE/coordinator/**/*.pb`, and safely reruns every stage after interruption.
It plans/reconciles roots, drains workers, rebuilds datasets, finalizes the
declared completion contract, writes deterministic findings, verifies a
protobuf publication snapshot, verifies the static site, and only then
promotes the site.

The initial host is deliberately filesystem/object-layout neutral. `PUBLIC`
contains immutable `sites/<site-id>/` trees, immutable
`catalogs/<site-id>.pb` records, the small `current.pb` plus browser-facing
`current.json` pointers, and a stable root `index.html` loader. The loader reads
`current.json` without caching and redirects to its immutable
`sites/<site-id>/` tree. Site-internal URLs are relative to that immutable tree,
so the same files work below the configured base URL without rewriting. Serve or
synchronize `PUBLIC` with any ordinary static host. Files below `sites/` and
`catalogs/` should receive long immutable cache lifetimes; `current.json` should
use no-cache or a short TTL.

## Fresh machine

1. Create a dedicated unprivileged `xlsynth-bvc` user.
2. Install Docker and grant only the access required by the existing hermetic
   driver/Yosys images.
3. Check out this repository at `/opt/xlsynth-bvc` and build with
   `cargo build --release`.
4. Create `/srv/xlsynth-bvc/{store,publication-work,public}` owned by the service
   user. Do not copy the legacy JSON store; this deployment is protobuf-only.
5. Copy `deploy/systemd/coordinator.env.example` to
   `/etc/xlsynth-bvc/coordinator.env` and adjust paths/workers/base URL.
6. Copy the service and timer into `/etc/systemd/system/`, run
   `systemctl daemon-reload`, then enable the timer.
7. Run two chosen historical versions manually with `coordinate-release`
   before enabling unattended publication. The scheduled script refreshes the
   compatibility map and processes the newest pending version by default.

`scripts/run-release-coordinator.sh` is the scheduled entrypoint. Set
`BVC_MAX_VERSIONS` to bound work per timer activation. The Rust-side file lock
is authoritative, so overlapping timer/manual invocations fail cleanly.

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
