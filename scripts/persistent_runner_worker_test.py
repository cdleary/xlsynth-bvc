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

    def test_short_request_publishes_idle_without_a_heartbeat_tick(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "job").mkdir()
            heartbeat = worker.HeartbeatPublisher(root / "heartbeat.json", "instance", "runner", worker.utc_now())
            request = {
                "schema_version": worker.SCHEMA_VERSION,
                "request_id": "request", "request_token": "token",
                "runner_key": "runner", "runner_instance_id": "instance",
                "container_name": "container", "timeout_secs": 5,
                "job_root": str(root / "job"), "heartbeat_path": str(heartbeat.path),
                "mounts": [], "env": {}, "script": "printf completed",
            }
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request))
            original_write_result = worker.write_result

            def publish_completed_result(path, payload):
                state = json.loads(heartbeat.path.read_text())
                self.assertEqual(state["state"], "idle")
                self.assertIsNone(state["current_request_id"])
                self.assertTrue((root / "archive/request.json").is_file())
                self.assertFalse(request_path.exists())
                original_write_result(path, payload)

            # There is no periodic heartbeat thread; the result must imply completion.
            with mock.patch.object(worker, "write_result", side_effect=publish_completed_result):
                self.assertTrue(worker.process_request(request_path, root / "results", root / "archive",
                                                      heartbeat, worker.PreparedDriverTools(root / "tools")))
            result = json.loads((root / "results/request.json").read_text())
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["stdout_tail"], "completed")
            self.assertEqual(json.loads(heartbeat.path.read_text())["state"], "idle")


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
