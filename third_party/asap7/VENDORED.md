# Pinned ASAP7 library summary

The canonical [`summary.textproto`](summary.textproto) records compact derived metadata and content identities for the four combinational ASAP7 7.5-track regular-VT, slow-slow NLDM libraries used by the `Tech mapped ASAP7` comparison. Builds compile this summary to a typed protobuf. Full Liberty files and timing tables are fetched into private cache storage when mapping; they are not checked into Git. They share a 0.63 V, 100 C corner, `1ps` time units, and `1ff` capacitance units. The sequential library is omitted because this comparison maps combinational AIGs.

- Source: https://github.com/siliconcompiler/lambdapdk
- Revision: `720a7e3d4c765496884dd58159152a142c2e769b`
- Source directory: `lambdapdk/asap7/libs/asap7sc7p5t_rvt/nldm/`
- License: BSD 3-Clause; see [LICENSE](LICENSE). Source copyright notices remain in every library.
- Reconstructed cache archive: `bundle.tar.gz`
- Archive SHA-256: `30d3c357ccbcc58d8f3aacdc0006b511a34329568e4eea4a1a03d37f39fa835a`

The cache materializer verifies the pinned upstream sources and reconstructs the historical archive byte-for-byte, preserving existing measurement identities. Each archive member is the exact decompressed byte content of its corresponding upstream `.lib.gz` file:

| Member | SHA-256 (uncompressed) |
| --- | --- |
| `asap7sc7p5t_AO_RVT_SS_nldm.lib` | `55584f8ac4910eefdbaf0183b021681cd87b3b9d62a62d6f802a601e79002df9` |
| `asap7sc7p5t_INVBUF_RVT_SS_nldm.lib` | `971247fdbb98c969300444c440ce5a5f3bc64a17c402e2025cf09c87908e97c0` |
| `asap7sc7p5t_OA_RVT_SS_nldm.lib` | `bf962aaa931b8bfeb8133719db6f6da0dde86fb373dd03629bf492f33f75c649` |
| `asap7sc7p5t_SIMPLE_RVT_SS_nldm.lib` | `29f2d9c5153299a3907df523633bb84fda8f291d90b3b0058d22ed5d67e719ee` |

## Reproducing the cached archive

From a checkout of the pinned source revision, run the following Python with its path as the first argument and the archive output path as the second. Members are ordered by name, and archive timestamps, ownership, and gzip filename are fixed. Library text is preserved without merging library blocks or changing timing tables.

```python
import gzip
import io
from pathlib import Path
import sys
import tarfile

source_dir = Path(sys.argv[1]) / "lambdapdk/asap7/libs/asap7sc7p5t_rvt/nldm"
with open(sys.argv[2], "wb") as raw:
    with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=9) as gz:
        with tarfile.open(fileobj=gz, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            for family in ("AO", "INVBUF", "OA", "SIMPLE"):
                source = source_dir / f"asap7sc7p5t_{family}_RVT_SS_nldm.lib.gz"
                data = gzip.decompress(source.read_bytes())
                member = tarfile.TarInfo(source.name.removesuffix(".gz"))
                member.size = len(data)
                member.mtime = 0
                member.mode = 0o644
                member.uid = member.gid = 0
                member.uname = member.gname = ""
                archive.addfile(member, io.BytesIO(data))
```

The runner prepares the cached archive with the unchanged `scripts/prepare_asap7.py` helper, passing the verified cache archive and a private output directory. This extracts the exact source libraries and produces one `merged.lib` for ABC versions without `read_lib -m`. The script verifies every source checksum, rejects duplicate cells, and verifies that the library headers and timing templates match except for dated comments and `default_max_transition`. The merged header uses the strictest transition limit (320 ps); source libraries use 320 ps except SIMPLE, which uses 4000 ps. Both mappers consume this same merged file, preserving all source cell areas, functions, capacitances, and timing tables.

Convert `merged.lib` with `xlsynth-driver lib2proto --include-timing --default-output-load 1 --output library.proto merged.lib`. Evaluation uses a 10 ps primary-input transition and 1 fF module-output load. The native delay strategy is experimental and selects cells using fixed reference slew/load; `gv-stats` evaluates every mapped netlist with these same settings using the `gv-sta` timing engine.

Site rendering consumes the canonical measured-result protobufs and does not read or fetch these libraries. See [the comparison guide](../../docs/tech-mapped-asap7.md) for mapping and offline-cache use.

The [reference-conditions guide](../../docs/techmap-reference-conditions.md) documents a selected
convention for future measurements, using a reference DFF's data-pin capacitance and output
transition. Its ASAP7 example identifies a sequential library from the same pinned revision;
that library is a source for the documented calibration only and is not added to the
combinational mapping bundle. The runner and existing measurements retain the 10 ps / 1 fF settings.
