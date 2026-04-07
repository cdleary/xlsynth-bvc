#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import sys
from pathlib import Path


def main() -> int:
    args = sys.argv[1:]
    proto_out = None
    delay_model = "unknown"
    top = None
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--delay_model" and i + 1 < len(args):
            delay_model = args[i + 1]
            i += 2
            continue
        if arg == "--top" and i + 1 < len(args):
            top = args[i + 1]
            i += 2
            continue
        if arg == "--proto_out" and i + 1 < len(args):
            proto_out = args[i + 1]
            i += 2
            continue
        i += 1
    if proto_out is None:
        print("missing --proto_out", file=sys.stderr)
        return 1
    target = Path(proto_out)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        f"delay_model={delay_model}\ntop={top or 'none'}\n",
        encoding="utf-8",
    )
    print(f"wrote fake delay info for {delay_model}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
