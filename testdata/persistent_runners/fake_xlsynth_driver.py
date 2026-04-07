#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import hashlib
import json
import sys
from pathlib import Path


SUBCOMMANDS = [
    "dslx2ir",
    "ir-fn-structural-hash",
    "ir2opt",
    "ir-equiv",
    "aig2ir",
    "ir2g8r",
    "ir2gates",
    "ir2combo",
    "ir-bool-cones",
    "aig-stats",
]


def print_help() -> int:
    print("Usage: xlsynth-driver <subcommand>")
    print("Commands:")
    for subcommand in SUBCOMMANDS:
        print(f"  {subcommand} fake")
    return 0


def sanitize_ident(value: str) -> str:
    out = []
    for char in value:
        if char.isalnum() or char == "_":
            out.append(char)
        else:
            out.append("_")
    ident = "".join(out)
    return ident or "top"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write_text(path: str, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def write_json(path: str, payload: dict) -> None:
    write_text(path, json.dumps(payload, indent=2, sort_keys=True))


def write_aig(path: str, label: str) -> None:
    write_text(path, f"aag 0 0 0 0 0\nc\n{label}\n")


def parse_args(args: list[str]) -> tuple[dict[str, str], list[str]]:
    options: dict[str, str] = {}
    positionals: list[str] = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg.startswith("--") and "=" in arg:
            key, value = arg.split("=", 1)
            options[key] = value
            i += 1
            continue
        if arg.startswith("--") and i + 1 < len(args) and not args[i + 1].startswith("--"):
            options[arg] = args[i + 1]
            i += 2
            continue
        if arg.startswith("--"):
            options[arg] = "true"
            i += 1
            continue
        positionals.append(arg)
        i += 1
    return options, positionals


def top_from_ir_text(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("top fn ") or stripped.startswith("fn "):
            rest = stripped.split("fn ", 1)[1]
            return sanitize_ident(rest.split("(", 1)[0].strip())
    return "top"


def make_ir(top: str, variant: str) -> str:
    top = sanitize_ident(top)
    variant = sanitize_ident(variant)
    return (
        "package fake_pkg\n\n"
        f"top fn {top}(x: bits[1] id=1) -> bits[1] {{\n"
        "  literal.2: bits[1] = literal(value=0, id=2)\n"
        f"  ret xor_{variant}.3: bits[1] = xor(x, literal.2, id=3)\n"
        "}\n"
    )


def print_subcommand_help(subcommand: str) -> int:
    if subcommand == "ir2gates":
        print("Usage: xlsynth-driver ir2gates [options]")
        print("  --prepared-ir-out PATH")
        return 0
    print(f"Usage: xlsynth-driver {subcommand}")
    return 0


def strip_global_options(argv: list[str]) -> list[str]:
    stripped: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == "--toolchain":
            i += 2
            continue
        stripped.append(argv[i])
        i += 1
    return stripped


def command_dslx2ir(args: list[str]) -> int:
    options, _ = parse_args(args)
    top = options.get("--dslx_top", "dslx_top")
    sys.stdout.write(make_ir(f"__dslx__{top}", "dslx"))
    return 0


def command_ir_fn_structural_hash(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir-fn-structural-hash")
    ir_text = read_text(positionals[-1])
    top = options.get("--top", top_from_ir_text(ir_text))
    payload = {"structural_hash": sha256_hex(f"{ir_text}\n{top}".encode("utf-8"))}
    sys.stdout.write(json.dumps(payload))
    return 0


def command_ir2opt(args: list[str]) -> int:
    _, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir2opt")
    ir_text = read_text(positionals[-1])
    sys.stdout.write(ir_text)
    return 0


def command_ir_equiv(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if len(positionals) < 2:
        raise SystemExit("missing IR inputs for ir-equiv")
    output_json = options.get("--output_json")
    if not output_json:
        raise SystemExit("missing --output_json for ir-equiv")
    write_json(
        output_json,
        {
            "equivalent": True,
            "lhs": positionals[0],
            "rhs": positionals[1],
            "top": options.get("--top"),
            "solver": options.get("--solver", "auto"),
        },
    )
    return 0


def command_aig2ir(args: list[str]) -> int:
    _, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing AIG input for aig2ir")
    top = sanitize_ident(Path(positionals[-1]).stem)
    sys.stdout.write(make_ir(f"{top}_from_aig", "aig2ir"))
    return 0


def command_ir2g8r(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir2g8r")
    ir_text = read_text(positionals[-1])
    top = options.get("--top", top_from_ir_text(ir_text))
    output_path = options.get("--aiger-out") or options.get("--bin-out")
    if output_path:
        write_aig(output_path, f"ir2g8r:{top}:{options.get('--fraig', 'false')}")
    stats_path = options.get("--stats-out")
    if stats_path:
        write_json(
            stats_path,
            {
                "and_nodes": len(ir_text.splitlines()) + 1,
                "depth": 3,
                "top": top,
                "fraig": options.get("--fraig", "false"),
            },
        )
    return 0


def command_ir2gates(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir2gates")
    prepared_ir_out = options.get("--prepared-ir-out")
    if not prepared_ir_out:
        raise SystemExit("missing --prepared-ir-out for ir2gates")
    ir_text = read_text(positionals[-1])
    top = options.get("--top", top_from_ir_text(ir_text))
    write_text(prepared_ir_out, make_ir(top, "prepared"))
    return 0


def command_ir2combo(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir2combo")
    ir_text = read_text(positionals[-1])
    top = sanitize_ident(options.get("--top", top_from_ir_text(ir_text)))
    sys.stdout.write(
        f"module {top}(input wire x, output wire y);\n"
        "  assign y = ~x;\n"
        "endmodule\n"
    )
    return 0


def command_ir_bool_cones(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing IR input for ir-bool-cones")
    ir_text = read_text(positionals[-1])
    output_dir = options.get("--output_dir")
    manifest_path = options.get("--manifest_jsonl")
    k = options.get("--k", "3")
    top = options.get("--top", top_from_ir_text(ir_text))
    if not output_dir or not manifest_path:
        raise SystemExit("missing --output_dir or --manifest_jsonl for ir-bool-cones")
    structural_hash = sha256_hex(f"{ir_text}\n{top}\n{k}".encode("utf-8"))
    cone_ir = (
        "package bool_cone\n\n"
        "top fn cone(leaf_1: bits[1] id=1) -> bits[1] {\n"
        "  ret not.2: bits[1] = not(leaf_1, id=2)\n"
        "}\n"
    )
    write_text(str(Path(output_dir) / f"{structural_hash}.ir"), cone_ir)
    write_text(
        manifest_path,
        json.dumps(
            {
                "sha256": structural_hash,
                "sink_node_index": 2,
                "frontier_leaf_indices": [1],
                "frontier_non_literal_count": 1,
                "included_node_count": 2,
            },
            sort_keys=True,
        )
        + "\n",
    )
    return 0


def command_aig_stats(args: list[str]) -> int:
    options, positionals = parse_args(args)
    if not positionals:
        raise SystemExit("missing AIG input for aig-stats")
    output_json = options.get("--output_json")
    if not output_json:
        raise SystemExit("missing --output_json for aig-stats")
    data = Path(positionals[-1]).read_bytes()
    digest = sha256_hex(data)
    write_json(
        output_json,
        {
            "and_nodes": int(digest[:2], 16) % 32 + 1,
            "depth": int(digest[2:4], 16) % 8 + 1,
            "input_sha256": digest,
        },
    )
    return 0


def main() -> int:
    argv = strip_global_options(sys.argv[1:])
    if not argv or argv[0] == "--help":
        return print_help()
    subcommand = argv[0]
    subargs = argv[1:]
    if subargs and subargs[0] == "--help":
        return print_subcommand_help(subcommand)
    handlers = {
        "dslx2ir": command_dslx2ir,
        "ir-fn-structural-hash": command_ir_fn_structural_hash,
        "ir2opt": command_ir2opt,
        "ir-equiv": command_ir_equiv,
        "aig2ir": command_aig2ir,
        "ir2g8r": command_ir2g8r,
        "ir2gates": command_ir2gates,
        "ir2combo": command_ir2combo,
        "ir-bool-cones": command_ir_bool_cones,
        "aig-stats": command_aig_stats,
    }
    handler = handlers.get(subcommand)
    if handler is None:
        print(f"unsupported fake xlsynth-driver subcommand: {subcommand}", file=sys.stderr)
        return 1
    return handler(subargs)


if __name__ == "__main__":
    raise SystemExit(main())
