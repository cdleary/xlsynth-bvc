# Vendored xlsynth-crate Assets

This directory contains hermetic third-party assets copied from specific
`xlsynth/xlsynth-crate` release tags.

## Pinned tool setup script

- Source repo: `https://github.com/xlsynth/xlsynth-crate`
- Source tag: `v0.29.0`
- Source path: `scripts/download_release.py`
- Vendored path: `third_party/xlsynth-crate/v0.29.0/scripts/download_release.py`

The Docker image uses the vendored script from this pinned tag for reproducible
setup behavior.

## Compatibility map (refreshable)

- Path: `third_party/xlsynth-crate/generated_version_compat.json`
- Upstream source: `main` branch file `generated_version_compat.json`

This compatibility JSON is the only third-party artifact intended to be updated
from head as needed, via the local `refresh-version-compat` CLI command.

## Immutable release-input lock

- Path: `release-inputs/v1.textproto`
- Source commit: resolved from each compatible `xlsynth/xlsynth` release tag
- Stdlib digest: SHA-256 of the corresponding published `dslx_stdlib.tar.gz`
- Refresh command: `scripts/sync-release-inputs.sh --update`

The lock is protobuf source data compiled into the binary. Refresh is an
out-of-band, reviewable source change; runtime code never updates it.
