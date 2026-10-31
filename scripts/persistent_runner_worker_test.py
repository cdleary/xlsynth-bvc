#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import fcntl
import json
import os
import random
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

import persistent_runner_worker as worker


class InboxWakeupTest(unittest.TestCase):
    def test_polling_fallback_checks_the_authoritative_inbox(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inbox, processing = root / "inbox", root / "processing"
            self.assertIsNone(worker.watch_inbox(inbox))
            inbox.mkdir()
            processing.mkdir()
            self.assertIsNone(worker.claim_request(inbox, processing))

            def publish(_interval):
                worker.write_json_atomic(inbox / "ready.json", {})

            with mock.patch.object(worker.time, "sleep", side_effect=publish) as sleep:
                self.assertFalse(worker.wait_for_inbox(None, 0.2))
                sleep.assert_called_once_with(0.2)
            self.assertEqual(worker.claim_request(inbox, processing), processing / "ready.json")

    def test_watch_read_failure_rescans_before_polling_fallback(self):
        reader, writer = os.pipe()
        with (
            os.fdopen(reader, "rb", buffering=0) as watch,
            os.fdopen(writer, "wb", buffering=0) as pipe,
            mock.patch.object(watch, "read", side_effect=OSError("watch failed")) as read,
            mock.patch.object(worker.time, "sleep") as sleep,
        ):
            pipe.write(b"hint")
            self.assertFalse(worker.wait_for_inbox(watch, 0.2))
            read.assert_called_once()
            self.assertTrue(watch.closed)
            sleep.assert_not_called()
            self.assertFalse(worker.wait_for_inbox(watch, 0.2))
            sleep.assert_called_once_with(0.2)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux rename notifications")
    def test_queued_rename_survives_until_the_worker_waits(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            inbox, processing = root / "inbox", root / "processing"
            inbox.mkdir()
            processing.mkdir()
            watch = worker.watch_inbox(inbox)
            self.assertIsNotNone(watch)
            self.addCleanup(watch.close)
            (root / "temp").write_text("partial")
            (root / "temp").replace(inbox / "unrelated")
            self.assertTrue(worker.wait_for_inbox(watch, 0))
            self.assertIsNone(worker.claim_request(inbox, processing))
            (root / "temp").write_text("complete")
            (root / "temp").replace(inbox / "ready.json")
            self.assertTrue(worker.wait_for_inbox(watch, 0))
            self.assertFalse(worker.wait_for_inbox(watch, 0))
            request = worker.claim_request(inbox, processing)
            self.assertEqual(request.read_text(), "complete")


class PreparedDriverToolsTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "tool").write_bytes(b"original")
        (self.source / "libxls-x64.so").write_bytes(b"library")
        self.tools = worker.PreparedDriverTools(self.root / "prepared")

    def test_reuse_and_mutation_match_fresh_copy(self):
        # Preparation replaces any versioned file with the generated alias.
        (self.source / "libxls-v1-x64.so").write_bytes(b"superseded")
        target = self.tools.prepare(self.source, "v1", "x64")
        inode = (target / "tool").stat().st_ino
        with mock.patch.object(worker.shutil, "copytree", side_effect=AssertionError("unexpected copy")):
            self.assertEqual(self.tools.prepare(self.source, "v1", "x64"), target)
        self.assertEqual((target / "tool").stat().st_ino, inode)
        self.assertEqual((target / "libxls-v1-x64.so").read_bytes(), b"library")
        # Same length and restored mtime still change inode ctime.
        original = (self.source / "tool").stat()
        (self.source / "tool").write_bytes(b"modified")
        os.utime(self.source / "tool", ns=(original.st_atime_ns, original.st_mtime_ns))
        self.tools.prepare(self.source, "v1", "x64")
        self.assertEqual((target / "tool").read_bytes(), b"modified")
        (target / "tool").write_bytes(b"tampered")
        self.tools.prepare(self.source, "v1", "x64")
        self.assertEqual((target / "tool").read_bytes(), b"modified")

    def test_version_and_source_identity_are_not_interchangeable(self):
        self.tools.prepare(self.source, "v1", "x64")
        target = self.tools.prepare(self.source, "v2", "x64")
        self.assertFalse((target / "libxls-v1-x64.so").exists())
        self.assertEqual((target / "libxls-v2-x64.so").read_bytes(), b"library")
        other = self.root / "other"
        worker.shutil.copytree(self.source, other)
        (other / "tool").write_bytes(b"another")
        self.tools.prepare(other, "v2", "x64")
        self.assertEqual((target / "tool").read_bytes(), b"another")

    def test_same_tick_edit_before_first_settled_reuse(self):
        # Model a filesystem on which the copy and edit share a timestamp tick.
        identity = worker.tree_identity

        def coarse_identity(root):
            return tuple((*entry[:5], 0, 0, entry[7]) for entry in identity(root))

        with (mock.patch.object(worker, "tree_identity", side_effect=coarse_identity),
              mock.patch.object(worker.time, "time_ns", return_value=0) as clock):
            target = self.tools.prepare(self.source, "v1", "x64")
            (target / "tool").write_bytes(b"tampered")
            clock.return_value = 2_000_000_000
            self.tools.prepare(self.source, "v1", "x64")
            self.assertEqual((target / "tool").read_bytes(), b"original")

    def test_failed_copy_does_not_publish_or_reuse_partial_state(self):
        target = self.tools.prepare(self.source, "v1", "x64")
        (self.source / "tool").write_bytes(b"replacement")
        with mock.patch.object(worker.shutil, "copytree", side_effect=OSError("copy interrupted")):
            with self.assertRaises(OSError):
                self.tools.prepare(self.source, "v1", "x64")
        self.assertEqual((target / "tool").read_bytes(), b"original")
        self.tools.prepare(self.source, "v1", "x64")
        self.assertEqual((target / "tool").read_bytes(), b"replacement")
        self.assertEqual(list(self.root.glob(".xlsynth-tools-*")), [])

    def test_generated_requests_match_fresh_preparation(self):
        rng = random.Random(71029)
        for _ in range(200):
            operation = rng.randrange(4)
            if operation == 0:
                (self.source / "tool").write_bytes(bytes(rng.randrange(256) for _ in range(8)))
            elif operation == 1 and self.tools.target.exists():
                (self.tools.target / "tool").write_bytes(b"modified")
            elif operation == 2:
                (self.source / "extra").write_bytes(b"extra")
            else:
                (self.source / "extra").unlink(missing_ok=True)
            version = "v{}".format(rng.randrange(3))
            target = self.tools.prepare(self.source, version, "x64")
            expected = {str(p.relative_to(self.source)): p.read_bytes()
                        for p in self.source.rglob("*") if p.is_file()}
            expected["libxls-{}-x64.so".format(version)] = b"library"
            actual = {str(p.relative_to(target)): p.read_bytes()
                      for p in target.rglob("*") if p.is_file()}
            self.assertEqual(actual, expected)


class HeartbeatPublicationTest(unittest.TestCase):
    def test_delayed_background_writer_cannot_overwrite_completion(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "heartbeat.json"
            heartbeat = worker.HeartbeatPublisher(path, "instance", "runner", worker.utc_now())
            heartbeat.transition("busy", "request")
            publishing = threading.Event()
            release = threading.Event()
            original_write = worker.write_json_atomic

            def delayed_write(path, payload):
                if threading.current_thread().name == "background":
                    publishing.set()
                    self.assertTrue(release.wait(5))
                original_write(path, payload)

            with mock.patch.object(worker, "write_json_atomic", side_effect=delayed_write):
                background = threading.Thread(target=heartbeat.publish, name="background")
                background.start()
                self.assertTrue(publishing.wait(5))
                completion = threading.Thread(target=lambda: heartbeat.transition("idle"))
                completion.start()
                release.set()
                background.join(5)
                completion.join(5)
                self.assertFalse(background.is_alive() or completion.is_alive())
            self.assertEqual(json.loads(path.read_text())["state"], "idle")
            self.assertIsNone(json.loads(path.read_text())["current_request_id"])


class ProcessRequestTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for directory in ["job", "source", "processing", "archive"]:
            (self.root / directory).mkdir()
        self.mount = self.root / "cache-input"
        patch = mock.patch.object(worker, "DRIVER_CACHE_MOUNT", str(self.mount))
        patch.start()
        self.addCleanup(patch.stop)
        self.tools = worker.PreparedDriverTools(self.root / "tools")
        self.heartbeat = worker.HeartbeatPublisher(
            self.root / "heartbeat.json", "instance", "runner", worker.utc_now()
        )
        self.request_path = self.root / "processing/request.json"

    def run_request(self, **overrides):
        request = {
            "schema_version": worker.SCHEMA_VERSION,
            "request_id": "request", "request_token": "token",
            "runner_key": "runner", "runner_instance_id": "instance",
            "container_name": "container", "timeout_secs": 5,
            "job_root": str(self.root / "job"), "heartbeat_path": str(self.heartbeat.path),
            "mounts": [{"source_path": str(self.root / "source"), "target_path": str(self.mount)}],
            "env": {"XLSYNTH_VERSION": "v1", "XLSYNTH_PLATFORM": "x64"},
            "script": "printf completed",
            **overrides,
        }
        self.request_path.write_text(json.dumps(request))
        reusable = worker.process_request(
            self.request_path, self.root / "results", self.root / "archive", self.heartbeat, self.tools
        )
        return reusable, json.loads((self.root / "results/request.json").read_text())

    def test_short_request_publishes_idle_without_a_heartbeat_tick(self):
        original_write_result = worker.write_result

        def publish_completed_result(path, payload):
            state = json.loads(self.heartbeat.path.read_text())
            self.assertEqual(state["state"], "idle")
            self.assertIsNone(state["current_request_id"])
            self.assertTrue((self.root / "archive/request.json").is_file())
            self.assertFalse(self.request_path.exists())
            self.assertFalse(self.mount.is_symlink())
            original_write_result(path, payload)

        with mock.patch.object(worker, "write_result", side_effect=publish_completed_result):
            reusable, result = self.run_request()
        self.assertTrue(reusable)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["stdout_tail"], "completed")

    def test_command_gets_only_time_left_after_preparation(self):
        with (mock.patch.object(worker.time, "monotonic_ns", return_value=0) as clock,
              mock.patch.object(worker.subprocess, "Popen") as launch):
            def finish_preparation(*args):
                clock.return_value = 3_000_000_000
                return self.tools.target

            launch.return_value.returncode = 0
            with mock.patch.object(self.tools, "prepare", side_effect=finish_preparation):
                reusable, result = self.run_request(timeout_secs=5)
            launch.return_value.wait.assert_called_once_with(timeout=2.0)
        self.assertTrue(reusable)
        self.assertEqual(result["timings_us"]["prepare"], 3_000_000)

    def test_timeout_interrupts_cold_copy_and_skips_command(self):
        def slow_copy(source, staging, **kwargs):
            staging.mkdir()
            (staging / "tool").write_bytes(b"partial")
            time.sleep(2)
            self.fail("copy should have been interrupted")

        with (mock.patch.object(worker.shutil, "copytree", side_effect=slow_copy),
              mock.patch.object(worker.subprocess, "Popen") as launch):
            reusable, result = self.run_request(timeout_secs=1)
            launch.assert_not_called()
        self.assertFalse(reusable)
        self.assertTrue(result["timed_out"])
        self.assertTrue(result["error"].startswith("TIMEOUT(1)"))
        self.assertEqual(result["timings_us"]["command"], 0)
        self.assertFalse(self.tools.target.exists())
        self.assertEqual(list(self.root.glob(".xlsynth-tools-*")), [])
        self.assertFalse(self.mount.is_symlink())
        self.assertTrue((self.root / "archive/request.json").is_file())
        self.assertEqual(json.loads(self.heartbeat.path.read_text())["state"], "retired")

    def test_command_timeout_kills_running_process(self):
        reusable, result = self.run_request(mounts=[], timeout_secs=1, script="exec sleep 5")
        self.assertFalse(reusable)
        self.assertTrue(result["timed_out"])
        self.assertLess(result["exit_code"], 0)
        self.assertEqual(json.loads(self.heartbeat.path.read_text())["state"], "retired")

    def test_archive_failure_retires_worker_and_publishes_failure(self):
        (self.root / "archive/request.json").mkdir()
        reusable, result = self.run_request()
        self.assertFalse(reusable)
        self.assertEqual(result["status"], "failed")
        self.assertIn("archival failed", result["error"])
        self.assertEqual(json.loads(self.heartbeat.path.read_text())["state"], "retired")
        self.assertTrue(self.request_path.is_file())

    def test_mount_cleanup_continues_after_failure_and_retires_worker(self):
        mounts = [
            {"source_path": str(self.root / "source"), "target_path": str(self.root / name)}
            for name in ["first", "second"]
        ]
        remove = worker.remove_existing_target

        def fail_last_unmount(path):
            if path == self.root / "second" and path.is_symlink():
                raise OSError("injected unlink failure")
            remove(path)

        with mock.patch.object(worker, "remove_existing_target", side_effect=fail_last_unmount):
            reusable, result = self.run_request(mounts=mounts)
        self.assertFalse(reusable)
        self.assertEqual(result["status"], "failed")
        self.assertIn("mount cleanup failed", result["error"])
        self.assertFalse((self.root / "first").is_symlink())
        self.assertEqual(json.loads(self.heartbeat.path.read_text())["state"], "retired")


class IdleRetirementTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for directory in ["inbox", "processing", "results", "archive"]:
            (self.root / directory).mkdir()
        (self.root / "slot.lock").touch()
        self.clock = mock.patch.object(worker.time, "monotonic", return_value=1000.0)
        self.clock.start()
        self.addCleanup(self.clock.stop)

    def reserve(self, ttl=900, idle_since=0.0):
        return worker.try_reserve_idle_retirement(self.root, ttl, idle_since)

    def test_timeout_disabled(self):
        self.assertIsNone(self.reserve(ttl=0))

    def test_timeout_not_elapsed(self):
        self.assertIsNone(self.reserve(idle_since=101.0))

    def test_timeout_boundary_reserves_slot(self):
        with self.reserve(idle_since=100.0) as reservation:
            self.assertFalse(reservation.closed)

    def test_host_reservation_prevents_retirement(self):
        with (self.root / "slot.lock").open("r+") as host_lock:
            fcntl.flock(host_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertIsNone(self.reserve())
        with self.reserve() as reservation:
            self.assertFalse(reservation.closed)

    def test_retirement_reservation_blocks_host(self):
        with self.reserve():
            with (self.root / "slot.lock").open("r+") as host_lock:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(host_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_pending_or_processing_request_prevents_retirement(self):
        for directory in ["inbox", "processing"]:
            with self.subTest(directory=directory):
                request = self.root / directory / "request.json"
                request.write_text("{}", encoding="utf-8")
                self.assertIsNone(self.reserve())
                request.unlink()
                with self.reserve() as reservation:
                    self.assertFalse(reservation.closed)

    def test_completed_results_are_preserved(self):
        result = self.root / "results" / "request.json"
        result.write_text('{"status":"completed"}', encoding="utf-8")
        with self.reserve():
            self.assertEqual(json.loads(result.read_text()), {"status": "completed"})
        self.assertTrue(result.exists())

    def test_wall_clock_and_heartbeat_do_not_extend_monotonic_timeout(self):
        with mock.patch.object(worker.time, "time", return_value=0):
            worker.write_json_atomic(self.root / "heartbeat.json", {"state": "idle"})
            with self.reserve() as reservation:
                self.assertFalse(reservation.closed)


class DriverCapabilityTest(unittest.TestCase):
    def test_failed_help_can_retry_and_absent_subcommands_need_no_probe(self):
        with tempfile.TemporaryDirectory() as temp:
            report = Path(temp) / "capabilities.json"
            driver = Path(temp) / "xlsynth-driver"
            driver.write_text("#!/bin/sh\nexit 1\n")
            driver.chmod(0o755)
            with mock.patch.object(worker.shutil, "which", return_value=str(driver)):
                with self.assertRaises(subprocess.CalledProcessError):
                    worker.write_capabilities(report, "slot", "instance", "image")
                self.assertFalse(report.exists())

                # An older driver advertises aig2ir and errors on unknown commands.
                driver.write_text('#!/bin/sh\n[ "$*" = "--help" ] || exit 2\necho "  aig2ir"\n')
                worker.write_capabilities(report, "slot", "instance", "image")
            result = json.loads(report.read_text())
            self.assertTrue(result["driver_subcommands"]["aig2ir"])
            self.assertFalse(result["driver_subcommands"]["ir2gates"])
            self.assertFalse(result["driver_help_tokens"]["ir2gates"]["--prepared-ir-out"])


class WorkerLifecycleTest(unittest.TestCase):
    def wait_for(self, predicate, process, timeout=8):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            self.assertIsNone(process.poll(), "worker exited before expected state")
            time.sleep(0.02)
        self.fail("worker did not reach expected state")

    def test_real_worker_reservation_job_and_idle_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runner = root / "runner"
            runner.mkdir()
            heartbeat = runner / "heartbeat.json"
            with (runner / "slot.lock").open("w+") as host_lock:
                fcntl.flock(host_lock, fcntl.LOCK_EX)
                process = subprocess.Popen(
                    [
                        sys.executable, str(Path(worker.__file__).resolve()),
                        "--store-root", str(root), "--runner-root", str(runner),
                        "--runner-key", "test-runner", "--runner-instance-id", "instance-1",
                        "--container-name", "test-container", "--image", "test-image",
                        "--idle-ttl-secs", "1",
                    ],
                    # Avoid probing any operator-installed driver in this local
                    # subprocess test. The request only needs the system shell.
                    env={**os.environ, "PATH": os.defpath},
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                try:
                    self.wait_for(heartbeat.exists, process)
                    time.sleep(1.2)
                    self.assertIsNone(process.poll())
                    self.assertEqual(json.loads(heartbeat.read_text())["state"], "idle")
                    job = root / "job"
                    job.mkdir()
                    request = {
                        "schema_version": worker.SCHEMA_VERSION,
                        "request_id": "request-1", "request_token": "token-1",
                        "runner_key": "test-runner", "runner_instance_id": "instance-1",
                        "container_name": "test-container", "timeout_secs": 10,
                        "job_root": str(job), "heartbeat_path": str(heartbeat),
                        "mounts": [], "env": {}, "script": "sleep 2; printf completed",
                    }
                    worker.write_json_atomic(runner / "inbox" / "request-1.json", request)
                    result = runner / "results" / "request-1.json"
                    self.wait_for(result.exists, process)
                    payload = json.loads(result.read_text())
                    self.assertEqual(payload["status"], "completed")
                    self.assertEqual(payload["stdout_tail"], "completed")
                    self.assertEqual(payload["exit_code"], 0)
                    # Completion resets the idle timer: the expired pre-job
                    # interval must not cause immediate exit when unreserved.
                    fcntl.flock(host_lock, fcntl.LOCK_UN)
                    time.sleep(0.2)
                    self.assertIsNone(process.poll())
                    fcntl.flock(host_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    # Retain the host reservation during result consumption,
                    # even after the new idle period has expired.
                    time.sleep(1.2)
                    self.assertIsNone(process.poll())
                    fcntl.flock(host_lock, fcntl.LOCK_UN)
                    stdout, stderr = process.communicate(timeout=8)
                    self.assertEqual((process.returncode, stdout, stderr), (0, b"", b""))
                    final = json.loads(heartbeat.read_text())
                    self.assertEqual(final["state"], "retired")
                    self.assertIsNone(final["current_request_id"])
                    self.assertIsNone(final["idle_since_unix_seconds"])
                    self.assertEqual(json.loads(result.read_text()), payload)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.communicate(timeout=8)


if __name__ == "__main__":
    unittest.main()
