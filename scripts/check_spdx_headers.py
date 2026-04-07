#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
SPDX_ID = "Apache-2.0"
HEADER_BY_STYLE = {
    "slash": f"// SPDX-License-Identifier: {SPDX_ID}",
    "hash": f"# SPDX-License-Identifier: {SPDX_ID}",
    "html": f"<!-- SPDX-License-Identifier: {SPDX_ID} -->",
    "plain": f"SPDX-License-Identifier: {SPDX_ID}",
}
EXCLUDED_PATHS = {
    # Cargo regenerates this file and does not preserve custom leading comments.
    "Cargo.lock",
    # JSON has no comment syntax and this file is parsed as a plain BTreeMap.
    "third_party/xlsynth-crate/generated_version_compat.json",
}


def tracked_files() -> list[Path]:
    output = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [Path(line) for line in output.stdout.splitlines() if line]


def header_style_for_path(relpath: Path) -> str | None:
    rel = relpath.as_posix()
    name = relpath.name
    suffix = relpath.suffix
    if rel in EXCLUDED_PATHS:
        return None
    if rel == "LICENSE":
        return "plain"
    if suffix == ".rs":
        return "slash"
    if suffix in {".toml", ".yml", ".yaml", ".lock", ".sh", ".ys", ".py"}:
        return "hash"
    if name in {"Makefile", ".gitignore"}:
        return "hash"
    if name.endswith(".Dockerfile") or name == "Dockerfile":
        return "hash"
    return None


def has_expected_header(path: Path, expected_header: str) -> bool:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return False
    if not lines:
        return False
    if lines[0].startswith("#!"):
        return len(lines) > 1 and lines[1] == expected_header
    return lines[0] == expected_header


def main() -> int:
    missing: list[str] = []
    for relpath in tracked_files():
        style = header_style_for_path(relpath)
        if style is None:
            continue
        expected_header = HEADER_BY_STYLE[style]
        abspath = REPO_ROOT / relpath
        if not has_expected_header(abspath, expected_header):
            missing.append(f"{relpath}: expected `{expected_header}`")

    if not missing:
        return 0

    print("SPDX header check failed for the following files:", file=sys.stderr)
    for entry in missing:
        print(f"  {entry}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
