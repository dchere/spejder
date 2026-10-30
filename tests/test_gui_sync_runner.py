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
from spejder.workflows.sync_log import SyncRunLog

# Capture before any test patches replace SyncRunLog.open on the shared class.
_REAL_SYNC_RUN_LOG_OPEN = SyncRunLog.open.__func__


def _open_sync_log_quiet(path: str, *, echo_stdout: bool = True, **kwargs):
    """File-only open so runner tests do not dump ``sync …`` to unittest stdout."""
    return _REAL_SYNC_RUN_LOG_OPEN(SyncRunLog, path, echo_stdout=False, **kwargs)


class InboxSyncRunnerConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_path = os.path.join(self._tmpdir.name, "report.html")
        self.sync_log_path = os.path.join(self._tmpdir.name, "outbox", "sync.log")
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
            sync_log_path=self.sync_log_path,
        )
        self.runner = InboxSyncRunner(self.context, self.rebuild_queue)
        self._echo_patcher = patch(
            "spejder.workflows.gui_sync.SyncRunLog.open",
            side_effect=_open_sync_log_quiet,
        )
        self._echo_patcher.start()
        self.addCleanup(self._echo_patcher.stop)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _wait_until_idle(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not self.runner.get_status()["running"]:
                return
            time.sleep(0.05)
        self.fail("runner still running after timeout")

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
            self._wait_until_idle()
            self.assertFalse(self.runner.get_status()["running"])

    def test_runner_writes_run_start_rebuild_run_end(self):
        def done_sync(_context):
            return InboxSyncResult(status="done")

        with patch("spejder.workflows.gui_sync.run_inbox_sync", side_effect=done_sync):
            with patch.object(self.rebuild_queue, "wait_until_idle", return_value=True):
                result = self.runner.trigger()
                self.assertEqual(result, {"ok": True, "started": True})
                self._wait_until_idle()

        self.assertTrue(os.path.isfile(self.sync_log_path))
        with open(self.sync_log_path, encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("event=run_start", text)
        self.assertIn("source=gui_sync", text)
        self.assertIn("event=stage_start", text)
        self.assertIn("stage=rebuild", text)
        self.assertIn("event=stage_end", text)
        self.assertIn("event=run_end", text)
        self.assertIn("status=complete", text)
        status = self.runner.get_status()
        self.assertEqual(status["status"], "complete")
        self.assertFalse(status["running"])
        self.assertEqual(status["reload_generation"], 0)

    def test_status_includes_reload_generation_idle(self):
        status = self.runner.get_status()
        self.assertIn("reload_generation", status)
        self.assertEqual(status["reload_generation"], 0)
        self.assertFalse(status["running"])
        self.assertEqual(status["status"], "idle")

    def test_mid_sync_refresh_bumps_generation_while_running(self):
        mid = threading.Event()
        hold = threading.Event()
        wait_timeouts: list[float] = []

        def fake_sync(context):
            self.assertIsNotNone(context.after_ingest_dashboard_refresh)
            context.after_ingest_dashboard_refresh()
            mid.set()
            hold.wait(timeout=5)
            return InboxSyncResult(status="done")

        def fake_wait(*, timeout=None):
            wait_timeouts.append(float(timeout) if timeout is not None else -1.0)
            return True

        with patch("spejder.workflows.gui_sync.run_inbox_sync", side_effect=fake_sync):
            with patch.object(
                self.rebuild_queue, "wait_until_idle", side_effect=fake_wait
            ):
                with patch.object(self.rebuild_queue, "queue") as mock_queue:
                    result = self.runner.trigger()
                    self.assertEqual(result, {"ok": True, "started": True})
                    self.assertTrue(mid.wait(timeout=2))
                    status = self.runner.get_status()
                    self.assertTrue(status["running"])
                    self.assertEqual(status["reload_generation"], 1)
                    self.assertEqual(status["stage_id"], "dashboard_refresh")
                    mock_queue.assert_called_with(
                        reason="post-ingest dashboard refresh"
                    )
                    hold.set()
                    self._wait_until_idle()

        status = self.runner.get_status()
        self.assertFalse(status["running"])
        self.assertEqual(status["reload_generation"], 1)
        self.assertEqual(status["status"], "complete")
        self.assertEqual(status["message"], "Sync complete")
        self.assertIn(120.0, wait_timeouts)
        self.assertIn(600.0, wait_timeouts)

    def test_end_sync_rebumps_generation_when_later_rebuilds(self):
        def refresh_then_enrich(context):
            context.after_ingest_dashboard_refresh()
            context.queue_dashboard_rebuild(reason="skills materialized=1")
            return InboxSyncResult(status="done")

        with patch(
            "spejder.workflows.gui_sync.run_inbox_sync", side_effect=refresh_then_enrich
        ):
            with patch.object(self.rebuild_queue, "wait_until_idle", return_value=True):
                with patch.object(
                    self.rebuild_queue, "queue", wraps=self.rebuild_queue.queue
                ) as mock_queue:
                    result = self.runner.trigger()
                    self.assertEqual(result, {"ok": True, "started": True})
                    self._wait_until_idle()

        status = self.runner.get_status()
        self.assertFalse(status["running"])
        self.assertEqual(status["reload_generation"], 2)
        self.assertEqual(status["status"], "complete")
        self.assertEqual(status["message"], "Sync complete")
        reasons = [call.kwargs.get("reason") for call in mock_queue.call_args_list]
        self.assertIn("post-ingest dashboard refresh", reasons)
        self.assertIn("skills materialized=1", reasons)

    def test_end_sync_no_rebump_without_later_rebuilds(self):
        def refresh_only(context):
            context.after_ingest_dashboard_refresh()
            return InboxSyncResult(status="done")

        with patch(
            "spejder.workflows.gui_sync.run_inbox_sync", side_effect=refresh_only
        ):
            with patch.object(self.rebuild_queue, "wait_until_idle", return_value=True):
                self.runner.trigger()
                self._wait_until_idle()

        status = self.runner.get_status()
        self.assertEqual(status["reload_generation"], 1)
        self.assertEqual(status["message"], "Sync complete")

    def test_reload_generation_monotonic_across_runs(self):
        def refresh_sync(context):
            context.after_ingest_dashboard_refresh()
            return InboxSyncResult(status="done")

        def plain_sync(_context):
            return InboxSyncResult(status="done")

        with patch.object(self.rebuild_queue, "wait_until_idle", return_value=True):
            with patch(
                "spejder.workflows.gui_sync.run_inbox_sync", side_effect=refresh_sync
            ):
                self.runner.trigger()
                self._wait_until_idle()
            self.assertEqual(self.runner.get_status()["reload_generation"], 1)

            with patch(
                "spejder.workflows.gui_sync.run_inbox_sync", side_effect=plain_sync
            ):
                self.runner.trigger()
                self._wait_until_idle()
            self.assertEqual(self.runner.get_status()["reload_generation"], 1)
            self.assertIn(
                "reload the page to see new positions",
                self.runner.get_status()["message"],
            )

    def test_mid_sync_refresh_timeout_keeps_reload_nudge(self):
        wait_timeouts: list[float] = []

        def fake_sync(context):
            context.after_ingest_dashboard_refresh()
            return InboxSyncResult(status="done")

        def fake_wait(*, timeout=None):
            timeout_s = float(timeout) if timeout is not None else -1.0
            wait_timeouts.append(timeout_s)
            # Mid-sync 120s path times out; end-of-sync 600s wait succeeds.
            return timeout_s != 120.0

        with patch("spejder.workflows.gui_sync.run_inbox_sync", side_effect=fake_sync):
            with patch.object(
                self.rebuild_queue, "wait_until_idle", side_effect=fake_wait
            ):
                with patch.object(self.rebuild_queue, "queue"):
                    result = self.runner.trigger()
                    self.assertEqual(result, {"ok": True, "started": True})
                    self._wait_until_idle()

        status = self.runner.get_status()
        self.assertFalse(status["running"])
        self.assertEqual(status["reload_generation"], 0)
        self.assertEqual(status["status"], "complete")
        self.assertIn("reload the page to see new positions", status["message"])
        self.assertIn(120.0, wait_timeouts)
        self.assertIn(600.0, wait_timeouts)
        with open(self.sync_log_path, encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("event=dashboard_refresh_timeout", text)
        self.assertIn("stage=dashboard_refresh", text)
        self.assertIn("timeout_s=120", text)


if __name__ == "__main__":
    unittest.main()
