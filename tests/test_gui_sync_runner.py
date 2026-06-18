"""Tests for InboxSyncRunner concurrency."""

import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.workflows.dashboard import DashboardRebuildQueue
from spejder.workflows.gui_sync import GuiSyncContext, InboxSyncResult, InboxSyncRunner


class InboxSyncRunnerConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_path = os.path.join(self._tmpdir.name, "report.html")
        self.rebuild_queue = DashboardRebuildQueue(
            self.db_path,
            self.report_path,
            AppConfig(),
        )
        self.context = GuiSyncContext(
            db_path=self.db_path,
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=self.rebuild_queue.queue,
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )
        self.runner = InboxSyncRunner(self.context, self.rebuild_queue)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_second_trigger_rejected_while_running(self):
        started = threading.Event()
        release = threading.Event()

        def slow_sync(_context):
            started.set()
            release.wait(timeout=5)
            return InboxSyncResult(status="skipped")

        with patch("spejder.workflows.gui_sync.run_inbox_sync", side_effect=slow_sync):
            first = self.runner.trigger()
            self.assertEqual(first, {"ok": True, "started": True})
            self.assertTrue(self.runner.get_status()["running"])

            self.assertTrue(started.wait(timeout=2))

            second = self.runner.trigger()
            self.assertEqual(second, {"ok": False, "error": "sync already running"})

            release.set()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if not self.runner.get_status()["running"]:
                    break
                time.sleep(0.05)
            self.assertFalse(self.runner.get_status()["running"])


if __name__ == "__main__":
    unittest.main()
