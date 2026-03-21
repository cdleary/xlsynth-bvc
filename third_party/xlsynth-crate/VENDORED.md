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
