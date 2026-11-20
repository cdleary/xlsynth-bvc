#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
import subprocess
import tempfile
import unittest

from prepare_delay_info_decode_protos import prepare_decode_protos, proto3_op_schema


EDITION_OP_SCHEMA = '''edition = "2024";
package xls;
enum OpProto {
  OP_INVALID = 0;
  OP_PARAM = 39;
  OP_XOR = 71;
  reserved 22, 80 to 82;
  reserved OP_DYNAMIC_COUNTED_FOR;
  reserved OP_REMOVED_A,
      OP_REMOVED_B;
}
'''

PROTO3_OP_SCHEMA = '''syntax = "proto3";
package xls;
enum OpProto {
  OP_INVALID = 0;
  OP_PARAM = 39;
  OP_XOR = 71;
  reserved 22, 80 to 82;
  reserved "OP_DYNAMIC_COUNTED_FOR";
  reserved "OP_REMOVED_A",
      "OP_REMOVED_B";
}
'''

DELAY_INFO_SCHEMA = '''syntax = "proto3";
package xls;
import "xls/ir/op.proto";
message DelayInfoNodeProto {
  uint64 total_delay_ps = 1;
  uint64 node_delay_ps = 2;
  OpProto op = 3;
  uint64 id = 4;
  string ir = 5;
}
message CriticalPathProto {
  uint64 total_delay_ps = 1;
  repeated DelayInfoNodeProto nodes = 2;
}
message DelayInfoProto {
  CriticalPathProto combinational_critical_path = 1;
  repeated DelayInfoNodeProto all_nodes = 3;
}
'''

DELAY_INFO_TEXT = '''combinational_critical_path {
  total_delay_ps: 19
  nodes {
    total_delay_ps: 19
    node_delay_ps: 7
    op: OP_XOR
    id: 3
    ir: "ret xor.3: bits[1] = xor(x, y, id=3)"
  }
}
all_nodes {
  op: OP_PARAM
  id: 1
  ir: "x: bits[1] = param(name=x, id=1)"
}
'''


class PrepareDelayInfoDecodeProtosTest(unittest.TestCase):
    def test_edition_reserved_names_use_proto3_spelling(self) -> None:
        self.assertEqual(proto3_op_schema(EDITION_OP_SCHEMA), PROTO3_OP_SCHEMA)

    def test_existing_proto3_schema_is_unchanged(self) -> None:
        self.assertEqual(proto3_op_schema(PROTO3_OP_SCHEMA), PROTO3_OP_SCHEMA)

    def test_compatibility_copy_decodes_the_same_wire_message(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_root = root / "release"
            baseline_root = root / "baseline"
            decode_root = root / "decode"
            for schema_root, op_schema in (
                (source_root, EDITION_OP_SCHEMA),
                (baseline_root, PROTO3_OP_SCHEMA),
            ):
                (schema_root / "xls/ir").mkdir(parents=True)
                (schema_root / "xls/estimators/delay_model").mkdir(parents=True)
                (schema_root / "xls/ir/op.proto").write_text(op_schema)
                (schema_root / "xls/estimators/delay_model/delay_info.proto").write_text(
                    DELAY_INFO_SCHEMA
                )

            def protoc(schema_root: Path, operation: str, payload: bytes) -> bytes:
                return subprocess.run(
                    [
                        "protoc",
                        f"-I{schema_root}",
                        f"--{operation}=xls.DelayInfoProto",
                        "xls/estimators/delay_model/delay_info.proto",
                    ],
                    input=payload,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    check=True,
                ).stdout

            wire_message = protoc(baseline_root, "encode", DELAY_INFO_TEXT.encode())
            prepare_decode_protos(source_root, decode_root)
            decoded = protoc(decode_root, "decode", wire_message)

            self.assertEqual(decoded.decode(), DELAY_INFO_TEXT)
            self.assertEqual(protoc(decode_root, "encode", decoded), wire_message)
            self.assertEqual(
                (source_root / "xls/ir/op.proto").read_text(), EDITION_OP_SCHEMA
            )
            self.assertEqual(
                (decode_root / "xls/estimators/delay_model/delay_info.proto").read_text(),
                DELAY_INFO_SCHEMA,
            )


if __name__ == "__main__":
    unittest.main()
