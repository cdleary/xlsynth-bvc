#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import sys
from pathlib import Path


def main() -> int:
    script_path = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "-s" and i + 1 < len(args):
            script_path = args[i + 1]
            i += 2
            continue
        i += 1
    if script_path is None:
        print("missing -s <script>", file=sys.stderr)
        return 1
    output_path = None
    for line in Path(script_path).read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("write_aiger "):
            output_path = stripped.split(None, 1)[1]
            break
    if output_path is None:
        print("missing write_aiger in yosys script", file=sys.stderr)
        return 1
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("aag 0 0 0 0 0\nc\nfake-yosys\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
