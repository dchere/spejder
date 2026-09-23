"""Tests for append-only sync event log."""

from __future__ import annotations

import os
import re
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import MagicMock

from spejder.config import AppConfig
from spejder.workflows.gui_sync import GuiSyncContext, _emit_stage
from spejder.workflows.sync_log import IngestProgressTracker, SyncRunLog, _NullSyncRunLog


_TS_RE = re.compile(r"^ts=\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\b")


class SyncRunLogTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self._tmpdir.name, "outbox", "sync.log")

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _read_lines(self) -> list[str]:
        with open(self.log_path, encoding="utf-8") as handle:
            return [line.rstrip("\n") for line in handle if line.strip()]

    def _open(self, **kwargs) -> SyncRunLog | _NullSyncRunLog:
        return SyncRunLog.open(self.log_path, echo_stdout=False, **kwargs)

    def test_run_and_stage_elapsed_and_ts(self) -> None:
        log = self._open()
        self.assertIsInstance(log, SyncRunLog)
        run_id = log.run_start(source="test", run_id="abc123def456")
        self.assertEqual(run_id, "abc123def456")
        log.stage_start("portal", "Checking IT-DAY job portal")
        log.stage_end()
        log.run_end(status="complete")
        lines = self._read_lines()
        self.assertGreaterEqual(len(lines), 4)
        self.assertTrue(_TS_RE.match(lines[0]))
        self.assertIn("event=run_start", lines[0])
        self.assertIn("source=test", lines[0])
        self.assertIn("run=abc123def456", lines[0])
        self.assertIn("event=stage_start", lines[1])
        self.assertIn("stage=portal", lines[1])
        self.assertIn('message="Checking IT-DAY job portal"', lines[1])
        self.assertIn("event=stage_end", lines[2])
        self.assertIn("stage=portal", lines[2])
        self.assertRegex(lines[2], r"elapsed_s=\d+\.\d{3}")
        self.assertIn("event=run_end", lines[3])
        self.assertRegex(lines[3], r"elapsed_s=\d+\.\d{3}")

    def test_progress_writes_pct(self) -> None:
        log = self._open()
        log.run_start(source="test", run_id="prog01")
        log.progress("skills", checked=50, total=200, updated=12)
        log.run_end(status="complete")
        lines = self._read_lines()
        progress = next(line for line in lines if "event=progress" in line)
        self.assertIn("checked=50/200", progress)
        self.assertIn("pct=25.0", progress)
        self.assertIn("updated=12", progress)

    def test_empty_path_is_null(self) -> None:
        log = SyncRunLog.open("")
        self.assertIsInstance(log, _NullSyncRunLog)
        log.run_start(source="test")
        log.stage_start("ingest")
        log.progress("ingest", checked=1, total=1)
        log.pipeline_end(status="done")
        log.run_end(status="complete")

    def test_unwritable_path_is_null(self) -> None:
        blocked = os.path.join(self._tmpdir.name, "blocked")
        os.makedirs(blocked, exist_ok=True)
        # Create a regular file where a directory is needed so open fails.
        bad_parent = os.path.join(blocked, "not_a_dir")
        with open(bad_parent, "w", encoding="utf-8") as handle:
            handle.write("x")
        bad_path = os.path.join(bad_parent, "sync.log")
        import io
        from contextlib import redirect_stderr

        err = io.StringIO()
        with redirect_stderr(err):
            log = SyncRunLog.open(bad_path)
        self.assertIsInstance(log, _NullSyncRunLog)
        self.assertIn("sync_log: failed to open", err.getvalue())
        self.assertIn("silent sinks", err.getvalue())
        log.run_start(source="test")
        log.run_end(status="failed")

    def test_null_log_is_silent_on_stdout(self) -> None:
        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            log = SyncRunLog.open("")
            self.assertIsInstance(log, _NullSyncRunLog)
            log.run_start(source="test")
            log.stage_start("ingest", "hi")
            log.progress("ingest", checked=1, total=1)
            log.pipeline_end(status="done")
            log.run_end(status="complete")
        self.assertEqual(buf.getvalue(), "")

    def _make_context(self, log: SyncRunLog) -> GuiSyncContext:
        return GuiSyncContext(
            db_path=os.path.join(self._tmpdir.name, "jobs.db"),
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": None,
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
            sync_log=log,
        )

    def test_emit_stage_writes_stage_start(self) -> None:
        log = self._open()
        log.run_start(source="gui_sync", run_id="emit01")
        context = self._make_context(log)
        on_stage = MagicMock()
        context = replace(context, on_stage=on_stage)
        _emit_stage(context, "portal", "Checking IT-DAY job portal")
        on_stage.assert_called_once_with("portal", "Checking IT-DAY job portal")
        lines = self._read_lines()
        self.assertTrue(any("event=stage_start" in line and "stage=portal" in line for line in lines))
        log.run_end(status="complete")

    def test_emit_stage_terminal_ids_write_pipeline_end(self) -> None:
        log = self._open()
        log.run_start(source="gui_sync", run_id="term01")
        context = self._make_context(log)
        for status, message in (
            ("done", "Inbox sync pipeline complete"),
            ("failed", "boom"),
            ("skipped", "Nothing to sync"),
        ):
            _emit_stage(context, status, message)
        log.run_end(status="complete")
        lines = self._read_lines()
        pipeline_lines = [line for line in lines if "event=pipeline_end" in line]
        self.assertEqual(len(pipeline_lines), 3)
        self.assertIn("status=done", pipeline_lines[0])
        self.assertIn('message="Inbox sync pipeline complete"', pipeline_lines[0])
        self.assertIn("status=failed", pipeline_lines[1])
        self.assertIn("message=boom", pipeline_lines[1])
        self.assertIn("status=skipped", pipeline_lines[2])
        self.assertTrue(all("event=stage_start" not in line for line in pipeline_lines))

    def test_stage_start_auto_ends_previous_stage(self) -> None:
        log = self._open()
        log.run_start(source="test", run_id="auto01")
        log.stage_start("portal", "Checking portal")
        log.stage_start("ingest", "Ingesting files")
        log.run_end(status="complete")
        lines = self._read_lines()
        events = [line for line in lines if "event=" in line]
        self.assertIn("event=stage_start", events[1])
        self.assertIn("stage=portal", events[1])
        self.assertIn("event=stage_end", events[2])
        self.assertIn("stage=portal", events[2])
        self.assertIn("event=stage_start", events[3])
        self.assertIn("stage=ingest", events[3])

    def test_quote_escaping_in_message(self) -> None:
        log = self._open()
        log.run_start(source="test", run_id="quote01")
        log.stage_start("ingest", 'path="C:\\inbox\\a.eml"')
        log.run_end(status="complete")
        lines = self._read_lines()
        start = next(line for line in lines if "event=stage_start" in line)
        self.assertIn(r'message="path=\"C:\\inbox\\a.eml\""', start)

    def test_progress_total_zero_omits_pct(self) -> None:
        log = self._open()
        log.run_start(source="test", run_id="nopct01")
        log.progress("ingest", checked=12, total=0, inserted=3, files=2)
        log.run_end(status="complete")
        lines = self._read_lines()
        progress = next(line for line in lines if "event=progress" in line)
        self.assertIn("checked=12/0", progress)
        self.assertNotIn("pct=", progress)
        self.assertIn("inserted=3", progress)
        self.assertIn("files=2", progress)

    def test_console_mirrors_file_without_ts(self) -> None:
        console: list[str] = []
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        handle = open(self.log_path, "a", encoding="utf-8")
        log = SyncRunLog(
            handle,
            self.log_path,
            echo_stdout=True,
            console_write=console.append,
        )
        log.run_start(source="gui_sync", run_id="a1b2c3d4e5f6")
        log.stage_start("portal", "Checking IT-DAY job portal")
        log.progress("skills", checked=50, total=200, updated=12)
        log.progress(
            "ingest",
            checked=25,
            total=0,
            inserted=3,
            skipped_existing=22,
            files=2,
        )
        log.stage_end()
        log.pipeline_end(status="done", message="Inbox sync pipeline complete")
        log.run_end(status="complete")

        file_lines = self._read_lines()
        self.assertEqual(len(console), len(file_lines))
        for file_line, console_line in zip(file_lines, console):
            self.assertTrue(_TS_RE.match(file_line))
            body = file_line.split(" ", 1)[1]
            self.assertEqual(console_line, f"sync {body}")
            self.assertNotIn("ts=", console_line)
        self.assertIn("event=run_start", console[0])
        self.assertIn("source=gui_sync", console[0])
        self.assertTrue(console[0].startswith("sync run=a1b2c3d4e5f6"))
        self.assertIn("event=progress", console[2])
        self.assertIn("checked=50/200", console[2])
        self.assertIn("pct=25.0", console[2])
        self.assertIn("checked=25/0", console[3])
        self.assertIn("inserted=3", console[3])
        self.assertIn("event=pipeline_end", console[5])
        self.assertIn("status=done", console[5])
        self.assertIn("event=run_end", console[6])
        self.assertIn("status=complete", console[6])

    def test_echo_stdout_false_is_silent(self) -> None:
        console: list[str] = []
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        handle = open(self.log_path, "a", encoding="utf-8")
        log = SyncRunLog(
            handle,
            self.log_path,
            echo_stdout=False,
            console_write=console.append,
        )
        log.run_start(source="test", run_id="silent01")
        log.progress("skills", checked=1, total=1, updated=0)
        log.run_end(status="complete")
        self.assertEqual(console, [])
        self.assertTrue(any("event=run_start" in line for line in self._read_lines()))

    def test_mid_write_soft_closes_file_keeps_console(self) -> None:
        import io
        from contextlib import redirect_stderr
        from unittest.mock import MagicMock

        console: list[str] = []
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        handle = open(self.log_path, "a", encoding="utf-8")
        log = SyncRunLog(
            handle,
            self.log_path,
            echo_stdout=True,
            console_write=console.append,
        )
        log.run_start(source="test", run_id="soft01")
        self.assertEqual(len(console), 1)
        self.assertIn("event=run_start", console[0])

        real_handle = log._handle
        broken = MagicMock()
        broken.write.side_effect = OSError("disk full")
        log._handle = broken
        real_handle.close()

        err = io.StringIO()
        with redirect_stderr(err):
            log.progress("skills", checked=1, total=1)
            log.pipeline_end(status="done")
            log.run_end(status="complete")

        self.assertIn("sync_log: failed to write", err.getvalue())
        self.assertIn("console-only", err.getvalue())
        self.assertEqual(err.getvalue().count("failed to write"), 1)
        self.assertFalse(log._file_live)
        self.assertTrue(log.closed)
        self.assertEqual(len(console), 4)
        self.assertIn("event=progress", console[1])
        self.assertIn("event=pipeline_end", console[2])
        self.assertIn("event=run_end", console[3])
        self.assertIn("status=complete", console[3])
        file_lines = self._read_lines()
        self.assertTrue(any("event=run_start" in line for line in file_lines))
        self.assertFalse(any("event=progress" in line for line in file_lines))
        self.assertFalse(any("event=run_end" in line for line in file_lines))


class IngestProgressTrackerTest(unittest.TestCase):
    def test_duplicate_heavy_emits_milestones_and_needs_final(self) -> None:
        tracker = IngestProgressTracker(milestone=25)
        self.assertTrue(tracker.note(1, 0))
        self.assertFalse(tracker.note(2, 0))
        self.assertTrue(tracker.note(25, 0))
        self.assertTrue(tracker.needs_final(40))
        self.assertFalse(tracker.needs_final(25))

    def test_insert_change_emits(self) -> None:
        tracker = IngestProgressTracker()
        tracker.note(1, 0)
        self.assertTrue(tracker.note(2, 1))


if __name__ == "__main__":
    unittest.main()
