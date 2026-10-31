#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import argparse
import fcntl
import json
import os
import platform
import re
import select
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, TextIO


SCHEMA_VERSION = 2
HEARTBEAT_INTERVAL_SECS = 1.0
OUTPUT_TAIL_MAX_BYTES = 256 * 1024
DRIVER_CACHE_MOUNT = "/cache-input"
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


class HeartbeatPublisher:
    def __init__(self, path: Path, instance_id: str, runner_key: str, started_utc: str):
        self.path = path
        self.lock = threading.Lock()
        self.payload = {
            "schema_version": SCHEMA_VERSION,
            "runner_instance_id": instance_id,
            "runner_key": runner_key,
            "state": "idle",
            "current_request_id": None,
            "idle_since_unix_seconds": int(time.time()),
            "started_utc": started_utc,
        }

    def publish(self) -> None:
        # Serialize the snapshot and publication so a delayed writer cannot
        # overwrite a newer transition with an old busy/idle state.
        with self.lock:
            write_json_atomic(self.path, {**self.payload, "last_heartbeat_utc": utc_now()})

    def transition(self, state: str, request_id: str | None = None) -> None:
        with self.lock:
            self.payload.update(
                state=state,
                current_request_id=request_id,
                idle_since_unix_seconds=int(time.time()) if state == "idle" else None,
            )
            write_json_atomic(self.path, {**self.payload, "last_heartbeat_utc": utc_now()})


def heartbeat_thread_fn(
    heartbeat: HeartbeatPublisher,
    stop_event: threading.Event,
) -> None:
    while not stop_event.wait(HEARTBEAT_INTERVAL_SECS):
        heartbeat.publish()


def tree_identity(root: Path) -> tuple:
    entries = []
    for path in [root, *sorted(root.rglob("*"))]:
        info = path.lstat()
        entries.append((
            str(path.relative_to(root)), info.st_dev, info.st_ino, info.st_mode,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns,
            os.readlink(path) if path.is_symlink() else None,
        ))
    return tuple(entries)


class PreparedDriverTools:
    def __init__(self, target: Path):
        self.target = target
        self.source_identity = None
        self.prepared_identity = None
        self.checked_at_ns = 0

    def prepare(self, source: Path, version: str, platform: str) -> Path:
        source = source.resolve(strict=True)
        checked_at_ns = time.time_ns()
        identity = (str(source), version, platform, tree_identity(source))
        if self.source_identity == identity and self.target.is_dir():
            if tree_identity(self.target) == self.prepared_identity:
                # A same-tick edit can predate this request. Trust metadata
                # only if it had already settled at the previous validation.
                cutoff = min(self.checked_at_ns, checked_at_ns) - 1_000_000_000
                settled = all(max(entry[5], entry[6]) < cutoff
                              for entry in (*identity[3], *self.prepared_identity))
                if settled or all(
                    files_equal(source / path.relative_to(self.target), path)
                    for path in self.target.rglob("*")
                    if not path.is_symlink() and path.is_file()
                ):
                    self.checked_at_ns = checked_at_ns
                    return self.target
        self.source_identity = None
        self.prepared_identity = None
        self.target.parent.mkdir(parents=True, exist_ok=True)
        # Only a complete copy becomes visible at the execution path. A failed
        # preparation leaves no reusable in-memory validation record.
        with tempfile.TemporaryDirectory(prefix=".xlsynth-tools-", dir=self.target.parent) as tmp:
            staging = Path(tmp) / "release"
            shutil.copytree(source, staging, copy_function=shutil.copy2)
            for ext in ["so", "dylib"]:
                library = staging / f"libxls-{platform}.{ext}"
                if library.is_file():
                    alias = staging / f"libxls-{version}-{platform}.{ext}"
                    remove_existing_target(alias)
                    alias.symlink_to(library.name)
            if tree_identity(source) != identity[3]:
                raise RuntimeError("driver tool cache changed during preparation")
            remove_existing_target(self.target)
            os.replace(staging, self.target)
        self.prepared_identity = tree_identity(self.target)
        self.source_identity = identity
        self.checked_at_ns = checked_at_ns
        return self.target


def files_equal(left: Path, right: Path) -> bool:
    with left.open("rb") as a, right.open("rb") as b:
        while True:
            chunk = a.read(1024 * 1024)
            if chunk != b.read(1024 * 1024):
                return False
            if not chunk:
                return True


@contextmanager
def limit_tool_preparation(seconds: float):
    # The worker prepares tools on its main thread. Interrupt a slow copy even
    # if it would consume the host's entire result-publication grace period.
    def expired(*_):
        raise subprocess.TimeoutExpired("driver tool preparation", seconds)

    if seconds <= 0:
        expired()
    previous = signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


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


def watch_inbox(directory: Path) -> BinaryIO | None:
    """Watch atomic inbox publication when Linux filesystem events are available."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        import ctypes

        libc = ctypes.CDLL(None)
        initialize, add_watch = libc.inotify_init1, libc.inotify_add_watch
    except (ImportError, AttributeError, OSError):
        return None
    fd = initialize(os.O_CLOEXEC | os.O_NONBLOCK)
    if fd < 0:
        return None
    in_moved_to = 0x80
    if add_watch(fd, os.fsencode(directory), in_moved_to) < 0:
        os.close(fd)
        return None
    return os.fdopen(fd, "rb", buffering=0)


def wait_for_inbox(watch: BinaryIO | None, interval: float) -> bool:
    """Wake on a rename or the normal retirement and request polling interval."""
    if watch is None or watch.closed:
        time.sleep(interval)
        return False
    try:
        poller = select.poll()
        poller.register(watch, select.POLLIN)
        if not poller.poll(round(interval * 1000)):
            return False
        if watch.read(4096):
            return True
    except OSError:
        pass
    # Poll may already have waited. Rescan now, then sleep on subsequent waits.
    watch.close()
    return False


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


def try_reserve_idle_retirement(
    runner_root: Path, idle_ttl_secs: int, idle_since_monotonic: float
) -> TextIO | None:
    """Return a held slot lock if this idle worker can safely retire.

    The host holds the same flock from selection through result writeback. In
    particular, an empty inbox alone does not mean a caller has no reservation.
    The caller must publish a final retired heartbeat before closing this lock.
    """
    if idle_ttl_secs == 0 or time.monotonic() - idle_since_monotonic < idle_ttl_secs:
        return None
    # Startup creates this file on the host; never replace its inode.
    slot_lock = (runner_root / "slot.lock").open("r+")
    try:
        try:
            fcntl.flock(slot_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            slot_lock.close()
            return None
        if list_inbox_requests(runner_root / "inbox") or list_inbox_requests(
            runner_root / "processing"
        ):
            slot_lock.close()
            return None
        return slot_lock
    except BaseException:
        slot_lock.close()
        raise


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

        def driver_help(*args: str) -> str:
            return subprocess.run(
                [driver_exe, *args, "--help"],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True,
            ).stdout

        combined_help = driver_help()
        for subcommand in DRIVER_PROBED_SUBCOMMANDS:
            driver_subcommands[subcommand] = (
                re.search(rf"^[ \t]+{re.escape(subcommand)}(?:[ \t]|$)", combined_help, re.MULTILINE)
                is not None
            )
        for subcommand, tokens in DRIVER_PROBED_HELP_TOKENS.items():
            subcommand_help = driver_help(subcommand) if driver_subcommands[subcommand] else ""
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
    heartbeat: HeartbeatPublisher,
    driver_tools: PreparedDriverTools,
) -> bool:
    request = json.loads(request_path.read_text(encoding="utf-8"))
    if request.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(
            f"unsupported request schema {request.get('schema_version')}; "
            f"expected {SCHEMA_VERSION}"
        )
    request_id = request["request_id"]
    request_token = request["request_token"]
    runner_key = request["runner_key"]
    instance_id = request["runner_instance_id"]
    container_name = request["container_name"]
    timeout_secs = int(request["timeout_secs"])
    heartbeat.transition("busy", request_id)

    job_root = Path(request["job_root"])
    stdout_log = job_root / "stdout.log"
    stderr_log = job_root / "stderr.log"
    started_utc = utc_now()

    mounted_targets: list[str] = []
    exit_code = None
    timed_out = False
    error = None
    preparation_started = time.monotonic_ns()
    deadline = preparation_started + timeout_secs * 1_000_000_000
    preparation_finished = None

    def remaining_seconds():
        return (deadline - time.monotonic_ns()) / 1_000_000_000

    try:
        for mount in request["mounts"]:
            ensure_mount_target(mount["target_path"], mount["source_path"])
            mounted_targets.append(mount["target_path"])

        env = os.environ.copy()
        env.update(request["env"])
        env.pop("BVC_PREPARED_XLSYNTH_TOOLS", None)
        env["BVC_PERSISTENT_RUNNER_REQUEST_ID"] = request_id
        if (DRIVER_CACHE_MOUNT in mounted_targets and "XLSYNTH_VERSION" in env
                and "XLSYNTH_PLATFORM" in env):
            with limit_tool_preparation(remaining_seconds()):
                env["BVC_PREPARED_XLSYNTH_TOOLS"] = str(driver_tools.prepare(
                    Path(DRIVER_CACHE_MOUNT), env["XLSYNTH_VERSION"], env["XLSYNTH_PLATFORM"]
                ))
        preparation_finished = time.monotonic_ns()

        with stdout_log.open("wb") as stdout_file, stderr_log.open("wb") as stderr_file:
            if remaining_seconds() <= 0:
                raise subprocess.TimeoutExpired(request["script"], timeout_secs)
            proc = subprocess.Popen(
                ["bash", "-lc", request["script"]],
                stdout=stdout_file,
                stderr=stderr_file,
                env=env,
                cwd="/",
                start_new_session=True,
            )
            try:
                proc.wait(timeout=remaining_seconds())
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                exit_code = proc.wait()
                raise

        if exit_code != 0:
            error = f"persistent runner command failed (exit={exit_code})"
    except subprocess.TimeoutExpired:
        timed_out = True
        error = f"TIMEOUT({timeout_secs}) persistent runner request exceeded {timeout_secs} seconds"
    except Exception as exc:
        error = f"persistent runner worker exception: {exc}"
        if exit_code is None:
            exit_code = 1
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
        with stderr_log.open("ab") as stderr_file:
            stderr_file.write(b"\nworker exception:\n")
            stderr_file.write(traceback.format_exc().encode("utf-8", errors="replace"))
    finally:
        command_finished = time.monotonic_ns()
        if preparation_finished is None:
            preparation_finished = command_finished
        for target in reversed(mounted_targets):
            try:
                remove_existing_target(Path(target))
            except OSError as exc:
                error = error or f"persistent runner mount cleanup failed: {exc}"
    try:
        archive_dir.mkdir(parents=True, exist_ok=True)
        os.replace(request_path, archive_dir / request_path.name)
    except OSError as exc:
        error = error or f"persistent runner request archival failed: {exc}"
    cleanup_finished = time.monotonic_ns()

    stdout_tail = read_tail(stdout_log, OUTPUT_TAIL_MAX_BYTES)
    stderr_tail = read_tail(stderr_log, OUTPUT_TAIL_MAX_BYTES)
    result = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "request_token": request_token,
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
        "timings_us": {
            "prepare": (preparation_finished - preparation_started) // 1000,
            "command": (command_finished - preparation_finished) // 1000,
            "cleanup": (cleanup_finished - command_finished) // 1000,
        },
    }
    retire = error is not None
    heartbeat.transition("retired" if retire else "idle")
    # Result publication is the completion barrier for cleanup and runner state.
    write_result(results_dir / f"{request_id}.json", result)
    return not retire


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-root", required=True)
    parser.add_argument("--runner-root", required=True)
    parser.add_argument("--runner-key", required=True)
    parser.add_argument("--runner-instance-id", required=True)
    parser.add_argument("--container-name", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--idle-ttl-secs", type=int, required=True)
    args = parser.parse_args()
    if args.idle_ttl_secs < 0:
        parser.error("--idle-ttl-secs must be nonnegative (0 disables retirement)")

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
    heartbeat = HeartbeatPublisher(heartbeat_path, args.runner_instance_id, args.runner_key, started_utc)
    driver_tools = PreparedDriverTools(Path("/tmp/xlsynth-release"))
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
    try:
        write_capabilities(capabilities_path, args.runner_key, args.runner_instance_id, args.image)
    except subprocess.CalledProcessError as error:
        # Script requests can still run. The host retries missing capability evidence.
        print(f"driver capability probe failed: {error}", file=sys.stderr)
    heartbeat.publish()

    stop_event = threading.Event()
    heartbeat_thread = threading.Thread(
        target=heartbeat_thread_fn,
        args=(heartbeat, stop_event),
        daemon=True,
    )
    heartbeat_thread.start()

    idle_since_monotonic = time.monotonic()
    retirement_lock = None
    inbox_events = watch_inbox(inbox_dir)
    try:
        while True:
            sync_cache_symlink(store_root)
            request_path = claim_request(inbox_dir, processing_dir)
            if request_path is None:
                retirement_lock = try_reserve_idle_retirement(
                    runner_root, args.idle_ttl_secs, idle_since_monotonic
                )
                if retirement_lock is not None:
                    break
                wait_for_inbox(inbox_events, 0.2)
                continue
            if not process_request(request_path, results_dir, archive_dir, heartbeat, driver_tools):
                break
            idle_since_monotonic = time.monotonic()
    finally:
        if inbox_events is not None:
            inbox_events.close()
        stop_event.set()
        heartbeat_thread.join()
        if retirement_lock is not None:
            # Do not let a final background heartbeat overwrite this terminal
            # state, or let a new host select us in the interval before exit.
            with retirement_lock:
                heartbeat.transition("retired")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
