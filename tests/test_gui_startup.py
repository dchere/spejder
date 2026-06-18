"""Tests for serve_gui unified startup flow."""

import os
import tempfile
import threading
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.workflows.gui import serve_gui
from spejder.workflows.gui_sync import InboxSyncRunner


class ServeGuiStartupTest(unittest.TestCase):
    def _run_serve_gui(self, *, no_open: bool = False):
        profile = AppConfig()
        mock_queue = MagicMock()
        mock_queue.wait_until_idle.return_value = True
        mock_runner = MagicMock()
        mock_runner.trigger.return_value = {"ok": True, "started": True}
        call_order: list[str] = []
        trigger_event = threading.Event()

        def track_wait_until_idle(*args, **kwargs):
            call_order.append("wait_until_idle")
            return True

        def track_start_server(*args, **kwargs):
            call_order.append("start_server")

        def track_trigger():
            trigger_event.set()
            return {"ok": True, "started": True}

        mock_queue.wait_until_idle.side_effect = track_wait_until_idle
        mock_runner.trigger.side_effect = track_trigger

        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"SPEJDER_WORKSPACE": tmp}, clear=False):
                with ExitStack() as stack:
                    stack.enter_context(
                        patch("spejder.workflows.gui.load_runtime_profile", return_value=profile)
                    )
                    stack.enter_context(patch("spejder.workflows.gui.ensure_db"))
                    stack.enter_context(
                        patch("spejder.workflows.gui._ensure_skill_pattern_seed_migration")
                    )
                    stack.enter_context(
                        patch(
                            "spejder.workflows.gui.DashboardRebuildQueue",
                            return_value=mock_queue,
                        )
                    )
                    stack.enter_context(
                        patch("spejder.workflows.gui.InboxSyncRunner", return_value=mock_runner)
                    )
                    stack.enter_context(patch("spejder.workflows.gui.time.sleep"))
                    mock_open = stack.enter_context(
                        patch("spejder.workflows.gui.webbrowser.open", return_value=True)
                    )
                    stack.enter_context(
                        patch("spejder.server.start_server", side_effect=track_start_server)
                    )
                    serve_gui(
                        profile=os.path.join(tmp, "profile.json"),
                        report_dir=os.path.join(tmp, "outbox"),
                        db=os.path.join(tmp, "jobs.db"),
                        no_open=no_open,
                    )

        self.assertTrue(trigger_event.wait(timeout=2))
        return mock_queue, mock_runner, mock_open, call_order

    def test_startup_blocks_rebuild_before_server(self):
        mock_queue, mock_runner, _mock_open, call_order = self._run_serve_gui()

        mock_queue.start_worker.assert_called_once()
        mock_queue.queue.assert_called_once_with(reason="startup snapshot")
        mock_queue.wait_until_idle.assert_called_once_with(timeout=600)
        self.assertEqual(call_order, ["wait_until_idle", "start_server"])
        mock_runner.trigger.assert_called_once()
        self.assertFalse(hasattr(InboxSyncRunner, "start_startup_sync"))

    def test_startup_opens_browser_when_not_no_open(self):
        _mock_queue, _mock_runner, mock_open, _call_order = self._run_serve_gui(no_open=False)

        mock_open.assert_called_once_with(
            "http://127.0.0.1:8765/report.html",
            new=2,
        )

    def test_startup_skips_browser_when_no_open(self):
        _mock_queue, mock_runner, mock_open, _call_order = self._run_serve_gui(no_open=True)

        mock_open.assert_not_called()
        mock_runner.trigger.assert_called_once()


if __name__ == "__main__":
    unittest.main()
