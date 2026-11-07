#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Prepare the pinned ASAP7 RVT SS bundle for native and legacy ABC readers.

The four source libraries share timing templates and electrical defaults except
for their dated comments and default_max_transition. We retain the first common
header and use the strictest transition bound when combining their cell blocks.
The source archive remains byte-for-byte unchanged.
"""

import argparse
import hashlib
from pathlib import Path
import re
import tarfile


MEMBERS = {
    "asap7sc7p5t_AO_RVT_SS_nldm.lib": "55584f8ac4910eefdbaf0183b021681cd87b3b9d62a62d6f802a601e79002df9",
    "asap7sc7p5t_INVBUF_RVT_SS_nldm.lib": "971247fdbb98c969300444c440ce5a5f3bc64a17c402e2025cf09c87908e97c0",
    "asap7sc7p5t_OA_RVT_SS_nldm.lib": "bf962aaa931b8bfeb8133719db6f6da0dde86fb373dd03629bf492f33f75c649",
    "asap7sc7p5t_SIMPLE_RVT_SS_nldm.lib": "29f2d9c5153299a3907df523633bb84fda8f291d90b3b0058d22ed5d67e719ee",
}


def normalized_header(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    text = re.sub(r'^\s*date\s*:[^\n]*', "", text, flags=re.M)
    text = re.sub(r'^\s*default_max_transition\s*:[^\n]*', "", text, flags=re.M)
    return re.sub(r"\s+", " ", text).strip()


def prepare(bundle, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    headers = []
    cells = []
    source_notices = []
    transition_limits = []
    seen_cells = set()
    with tarfile.open(bundle, "r:gz") as archive:
        if archive.getnames() != list(MEMBERS):
            raise ValueError("ASAP7 bundle members differ from the pinned library set")
        for name, expected_sha256 in MEMBERS.items():
            member = archive.getmember(name)
            if not member.isfile():
                raise ValueError(f"ASAP7 library member is not a file: {name}")
            data = archive.extractfile(member).read()
            if hashlib.sha256(data).hexdigest() != expected_sha256:
                raise ValueError(f"ASAP7 library member checksum differs: {name}")
            (output_dir / name).write_bytes(data)
            text = data.decode("utf-8")
            library = re.search(r"\blibrary\s*\([^)]*\)\s*\{", text)
            first_cell = re.search(r"^  cell\s*\(", text, flags=re.M)
            if library is None or first_cell is None or not text.rstrip().endswith("}"):
                raise ValueError(f"Unexpected pinned Liberty layout: {name}")
            source_notices.append(text[:library.start()])
            header = text[library.end():first_cell.start()]
            headers.append(header)
            limits = re.findall(r"\bdefault_max_transition\s*:\s*([\d.]+)\s*;", header)
            if len(limits) != 1:
                raise ValueError(f"Missing library transition default: {name}")
            transition_limits.append(float(limits[0]))
            for cell_name in re.findall(r"^  cell\s*\(([^)]*)\)", text, flags=re.M):
                if cell_name in seen_cells:
                    raise ValueError(f"Duplicate ASAP7 cell: {cell_name}")
                seen_cells.add(cell_name)
            cells.append(text[first_cell.start():text.rfind("}")])
    if any(normalized_header(header) != normalized_header(headers[0]) for header in headers[1:]):
        raise ValueError("ASAP7 library headers or timing templates differ")
    header = re.sub(r"(\bdefault_max_transition\s*:\s*)[\d.]+", lambda m: m.group(1) + format(min(transition_limits), ".15g"), headers[0])
    merged = "\n".join(source_notices) + "\nlibrary (asap7_rvt_ss) {" + header + "\n".join(cells) + "}\n"
    (output_dir / "merged.lib").write_text(merged, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("output_dir", type=Path)
    args = parser.parse_args()
    prepare(args.bundle, args.output_dir)


if __name__ == "__main__":
    main()
