#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Make a decode-only copy of release schemas for pre-Edition protoc."""

from pathlib import Path
import re
import shutil
import sys


def proto3_op_schema(schema: str) -> str:
    if not re.search(r'^edition\s*=\s*"[0-9]+"\s*;', schema, re.MULTILINE):
        return schema

    schema = re.sub(
        r'^edition\s*=\s*"[0-9]+"\s*;',
        'syntax = "proto3";',
        schema,
        flags=re.MULTILINE,
    )
    # Editions reserve enum names as identifiers, while proto3 requires strings.
    # This changes schema spelling only; enum numbers and the PB wire data stay
    # unchanged. Numeric reservations already have compatible spelling.
    return re.sub(
        r'(?m)^([ \t]*reserved\s+)([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)(\s*;)',
        lambda match: match[1]
        + re.sub(r'[A-Za-z_]\w*', lambda name: f'"{name[0]}"', match[2])
        + match[3],
        schema,
    )


def prepare_decode_protos(source_root: Path, decode_root: Path) -> None:
    op_relpath = Path("xls/ir/op.proto")
    delay_relpath = Path("xls/estimators/delay_model/delay_info.proto")
    for relpath in (op_relpath, delay_relpath):
        (decode_root / relpath).parent.mkdir(parents=True, exist_ok=True)
    (decode_root / op_relpath).write_text(
        proto3_op_schema((source_root / op_relpath).read_text())
    )
    shutil.copyfile(source_root / delay_relpath, decode_root / delay_relpath)


if __name__ == "__main__":
    prepare_decode_protos(Path(sys.argv[1]), Path(sys.argv[2]))
