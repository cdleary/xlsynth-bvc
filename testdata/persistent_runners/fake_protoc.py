#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import sys


def main() -> int:
    payload = sys.stdin.read()
    sys.stdout.write("model: \"fake\"\n")
    if payload.strip():
        escaped = payload.strip().replace("\\", "\\\\").replace('"', '\\"')
        sys.stdout.write(f"raw: \"{escaped}\"\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
