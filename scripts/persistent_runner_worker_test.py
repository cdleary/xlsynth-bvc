#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
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
