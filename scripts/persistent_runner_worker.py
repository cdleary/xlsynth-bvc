#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import argparse
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path


SCHEMA_VERSION = 1
HEARTBEAT_INTERVAL_SECS = 1.0
OUTPUT_TAIL_MAX_BYTES = 256 * 1024
DRIVER_PROBED_SUBCOMMANDS = [
    "aig2ir",
    "ir-equiv",
    "ir2g8r",
    "ir2gates",
]
DRIVER_PROBED_HELP_TOKENS = {
    "ir2gates": ["--prepared-ir-out"],
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{time.time_ns()}")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def read_tail(path: Path, max_bytes: int) -> str:
    if not path.exists():
        return ""
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        seek = max(0, size - max_bytes)
        f.seek(seek)
        data = f.read()
    if seek > 0:
        return f"[truncated to last {max_bytes} bytes]\n" + data.decode(
            "utf-8", errors="replace"
        )
    return data.decode("utf-8", errors="replace")


def remove_existing_target(path: Path) -> None:
    try:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    except FileNotFoundError:
        pass


def ensure_mount_target(target: str, source: str) -> None:
    target_path = Path(target)
    remove_existing_target(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(source, target)


def heartbeat_thread_fn(
    heartbeat_path: Path,
    instance_id: str,
    runner_key: str,
    state_fn,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(HEARTBEAT_INTERVAL_SECS):
        state, current_request_id = state_fn()
        write_json_atomic(
            heartbeat_path,
            {
                "schema_version": SCHEMA_VERSION,
                "runner_instance_id": instance_id,
                "runner_key": runner_key,
                "state": state,
                "current_request_id": current_request_id,
                "started_utc": state_fn.started_utc,
                "last_heartbeat_utc": utc_now(),
            },
        )


def list_inbox_requests(inbox_dir: Path) -> list[Path]:
    if not inbox_dir.exists():
        return []
    return sorted(
        [
            path
            for path in inbox_dir.iterdir()
            if path.is_file() and path.suffix == ".json"
        ]
    )


def claim_request(inbox_dir: Path, processing_dir: Path) -> Path | None:
    for candidate in list_inbox_requests(inbox_dir):
        claimed = processing_dir / candidate.name
        try:
            processing_dir.mkdir(parents=True, exist_ok=True)
            os.replace(candidate, claimed)
            return claimed
        except FileNotFoundError:
            continue
        except OSError:
            continue
    return None


def write_result(result_path: Path, payload: dict) -> None:
    write_json_atomic(result_path, payload)


def sync_cache_symlink(store_root: Path) -> None:
    cache_target = store_root / "driver-release-cache"
    cache_path = Path("/cache")
    if not cache_target.exists():
        return
    remove_existing_target(cache_path)
    os.symlink(str(cache_target), str(cache_path))


def write_capabilities(
    capabilities_path: Path,
    runner_key: str,
    instance_id: str,
    image: str,
) -> None:
    runner_family = "generic"
    driver_subcommands: dict[str, bool] = {}
    driver_help_tokens: dict[str, dict[str, bool]] = {}
    driver_exe = shutil.which("xlsynth-driver")
    if driver_exe is not None:
        runner_family = "driver"
        help_proc = subprocess.run(
            [driver_exe, "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        combined_help = f"{help_proc.stdout}\n{help_proc.stderr}"
        for subcommand in DRIVER_PROBED_SUBCOMMANDS:
            driver_subcommands[subcommand] = (
                re.search(rf"^[ \t]+{re.escape(subcommand)}(?:[ \t]|$)", combined_help, re.MULTILINE)
                is not None
            )
        for subcommand, tokens in DRIVER_PROBED_HELP_TOKENS.items():
            help_proc = subprocess.run(
                [driver_exe, subcommand, "--help"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            subcommand_help = f"{help_proc.stdout}\n{help_proc.stderr}"
            driver_help_tokens[subcommand] = {
                token: token in subcommand_help for token in tokens
            }
    elif shutil.which("yosys") is not None:
        runner_family = "yosys"

    write_json_atomic(
        capabilities_path,
        {
            "schema_version": SCHEMA_VERSION,
            "runner_key": runner_key,
            "runner_instance_id": instance_id,
            "image": image,
            "runner_family": runner_family,
            "python_version": sys.version,
            "platform": platform.platform(),
            "hostname": socket.gethostname(),
            "supports": {
                "generic_script_execution": True,
            },
            "driver_subcommands": driver_subcommands,
            "driver_help_tokens": driver_help_tokens,
        },
    )


def process_request(
    request_path: Path,
    results_dir: Path,
    archive_dir: Path,
    heartbeat_state,
) -> None:
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request_id = request["request_id"]
    runner_key = request["runner_key"]
    instance_id = request["runner_instance_id"]
    container_name = request["container_name"]
    timeout_secs = int(request["timeout_secs"])
    heartbeat_state["state"] = "busy"
    heartbeat_state["current_request_id"] = request_id

    job_root = Path(request["job_root"])
    stdout_log = job_root / "stdout.log"
    stderr_log = job_root / "stderr.log"
    started_utc = utc_now()

    mounted_targets: list[str] = []
    exit_code = None
    timed_out = False
    error = None

    try:
        for mount in request["mounts"]:
            ensure_mount_target(mount["target_path"], mount["source_path"])
            mounted_targets.append(mount["target_path"])

        env = os.environ.copy()
        env.update(request["env"])
        env["BVC_PERSISTENT_RUNNER_REQUEST_ID"] = request_id

        with stdout_log.open("wb") as stdout_file, stderr_log.open("wb") as stderr_file:
            proc = subprocess.Popen(
                ["bash", "-lc", request["script"]],
                stdout=stdout_file,
                stderr=stderr_file,
                env=env,
                cwd="/",
            )
            stop_event = threading.Event()

            def running_state():
                return heartbeat_state["state"], heartbeat_state["current_request_id"]

            running_state.started_utc = heartbeat_state["started_utc"]
            thread = threading.Thread(
                target=heartbeat_thread_fn,
                args=(
                    Path(request["heartbeat_path"]),
                    instance_id,
                    runner_key,
                    running_state,
                    stop_event,
                ),
                daemon=True,
            )
            thread.start()
            try:
                proc.wait(timeout=timeout_secs)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                proc.kill()
                exit_code = proc.wait()
            finally:
                stop_event.set()
                thread.join(timeout=2.0)

        if timed_out:
            error = (
                f"TIMEOUT({timeout_secs}) persistent runner command exceeded "
                f"{timeout_secs} seconds"
            )
        elif exit_code != 0:
            error = f"persistent runner command failed (exit={exit_code})"
    except Exception as exc:
        error = f"persistent runner worker exception: {exc}"
        if exit_code is None:
            exit_code = 1
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        with stderr_log.open("ab") as stderr_file:
            stderr_file.write(b"\nworker exception:\n")
            stderr_file.write(traceback.format_exc().encode("utf-8", errors="replace"))
    finally:
        for target in reversed(mounted_targets):
            remove_existing_target(Path(target))

    stdout_tail = read_tail(stdout_log, OUTPUT_TAIL_MAX_BYTES)
    stderr_tail = read_tail(stderr_log, OUTPUT_TAIL_MAX_BYTES)
    result = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "runner_key": runner_key,
        "runner_instance_id": instance_id,
        "container_name": container_name,
        "started_utc": started_utc,
        "finished_utc": utc_now(),
        "status": "completed" if error is None else "failed",
        "exit_code": exit_code,
        "timed_out": timed_out,
        "error": error,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "command_argv": ["bash", "-lc", request["script"]],
    }
    write_result(results_dir / f"{request_id}.json", result)
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived_request = archive_dir / request_path.name
    try:
        os.replace(request_path, archived_request)
    except OSError:
        pass
    heartbeat_state["state"] = "idle"
    heartbeat_state["current_request_id"] = None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-root", required=True)
    parser.add_argument("--runner-root", required=True)
    parser.add_argument("--runner-key", required=True)
    parser.add_argument("--runner-instance-id", required=True)
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()

    store_root = Path(args.store_root)
    runner_root = Path(args.runner_root)
    inbox_dir = runner_root / "inbox"
    processing_dir = runner_root / "processing"
    results_dir = runner_root / "results"
    archive_dir = runner_root / "archive"
    heartbeat_path = runner_root / "heartbeat.json"
    live_path = runner_root / "live.json"
    capabilities_path = runner_root / "capabilities.json"

    for path in [runner_root, inbox_dir, processing_dir, results_dir, archive_dir]:
        path.mkdir(parents=True, exist_ok=True)

    sync_cache_symlink(store_root)

    started_utc = utc_now()
    heartbeat_state = {
        "state": "idle",
        "current_request_id": None,
        "started_utc": started_utc,
    }

    def state_fn():
        return heartbeat_state["state"], heartbeat_state["current_request_id"]

    state_fn.started_utc = started_utc
    write_json_atomic(
        live_path,
        {
            "schema_version": SCHEMA_VERSION,
            "runner_key": args.runner_key,
            "runner_instance_id": args.runner_instance_id,
            "container_name": args.container_name,
            "image": args.image,
            "started_utc": started_utc,
            "hostname": socket.gethostname(),
        },
    )
    write_capabilities(
        capabilities_path,
        args.runner_key,
        args.runner_instance_id,
        args.image,
    )

    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat_thread_fn,
        args=(
            heartbeat_path,
            args.runner_instance_id,
            args.runner_key,
            state_fn,
            stop_event,
        ),
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        while True:
            sync_cache_symlink(store_root)
            request_path = claim_request(inbox_dir, processing_dir)
            if request_path is None:
                time.sleep(0.2)
                continue
            process_request(request_path, results_dir, archive_dir, heartbeat_state)
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=2.0)


if __name__ == "__main__":
    raise SystemExit(main())
