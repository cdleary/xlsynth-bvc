#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Matched ABBA preparation benchmark; never starts workers or modifies source inputs.

Uses fresh stores and an identical deterministic subset for every trial. Operational
JSON reports are measurement exports, not canonical evaluation results. Run on a quiet
machine for controlled numbers; record competing activity when that is not possible.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import time


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def projection(path):
    manifest = json.loads((path / "manifest.json").read_text())
    return {
        key: manifest[key]
        for key in ("recipe_preset", "driver_runtime", "stats_runtime", "yosys_runtime",
                    "dso_version", "fraig", "yosys_script_sha256", "samples")
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--sample-count", type=int, default=512)
    parser.add_argument("--prepare-threads", type=int, default=4)
    parser.add_argument("--driver-version", required=True)
    parser.add_argument("--stats-driver-version", required=True)
    parser.add_argument("--dso-version", required=True)
    parser.add_argument("--top-fn-policy", default="from-filename",
                        choices=["from-filename", "infer-single-package"])
    args = parser.parse_args()
    if args.sample_count < 1 or not 1 <= args.prepare_threads <= 64:
        parser.error("positive sample count and 1..64 preparation threads required")
    binaries = {"before": args.before.resolve(strict=True), "after": args.after.resolve(strict=True)}
    source = args.input_dir.resolve(strict=True)
    files = sorted(source.rglob("*.ir"), key=lambda p: hashlib.sha256(
        p.relative_to(source).as_posix().encode()).digest())[:args.sample_count]
    if len(files) != args.sample_count:
        parser.error("input directory has fewer files than requested")
    root = args.out_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    inputs = root / "inputs"
    inputs.mkdir()
    identities = {}
    for path in files:
        relative = path.relative_to(source)
        target = inputs / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        identities[relative.as_posix()] = sha256(target)
    env = dict(os.environ, BVC_DISABLE_AUTO_SUGGESTED_ENQUEUE="1",
               BVC_ENABLE_INCREMENTAL_IR_CORPUS_UPSERT="0")
    results = []
    expected = None
    for trial, variant in enumerate(["before", "after", "after", "before"], 1):
        output = root / f"{trial}-{variant}"
        command = [str(binaries[variant]), "run-ir-dir-corpus", "--input-dir", str(inputs),
                   "--output-dir", str(output), "--execution-mode", "enqueue",
                   "--recipe-preset", "g8r-abc-vs-yabc-aig-diff",
                   "--top-fn-policy", args.top_fn_policy,
                   "--version", args.dso_version, "--driver-version", args.driver_version,
                   "--stats-driver-version", args.stats_driver_version,
                   "--prepare-threads", str(args.prepare_threads)]
        print(f"Starting trial {trial}: {variant}", flush=True)
        start = time.monotonic()
        with (root / f"{trial}-{variant}.log").open("w") as log:
            subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        elapsed = time.monotonic() - start
        actual = projection(output)
        samples = actual["samples"]
        if len(samples) != len(identities) or {
            s["source_relpath"]: s["source_sha256"] for s in samples
        } != identities:
            raise RuntimeError("prepared input identities differ from selected bytes")
        if expected is None:
            expected = actual
        elif actual != expected:
            raise RuntimeError("prepared runtimes/actions/statuses differ between trials")
        # Native progress validates the persisted store, outside the timed preparation region.
        report = subprocess.run([str(binaries[variant]), "show-corpus-progress",
                                 "--output-dir", str(output)], env=env, capture_output=True,
                                text=True, check=True)
        progress = json.loads(report.stdout)
        counts = progress["action_counts"]
        if any(counts[k] for k in ["failed", "missing", "canceled", "running", "done_missing_artifact"]):
            raise RuntimeError(f"invalid preparation coverage: {counts}")
        if counts["done"] + counts["pending"] != counts["planned_unique"]:
            raise RuntimeError(f"unaccounted prepared actions: {counts}")
        row = {"trial": trial, "variant": variant, "seconds": elapsed,
               "inputs_per_second": len(samples) / elapsed, "action_counts": counts}
        results.append(row)
        print(json.dumps(row), flush=True)
        (root / "results.json").write_text(json.dumps({"trials": results}, indent=2) + "\n")
    medians = {variant: statistics.median(r["seconds"] for r in results if r["variant"] == variant)
               for variant in binaries}
    summary = {"trials": results, "sample_count": len(files), "prepare_threads": args.prepare_threads,
               "binary_sha256": {v: sha256(p) for v, p in binaries.items()},
               "median_seconds": medians, "speedup": medians["before"] / medians["after"],
               "wall_time_reduction_pct": 100 * (1 - medians["after"] / medians["before"]),
               "equivalent_prepared_manifests": True}
    (root / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
