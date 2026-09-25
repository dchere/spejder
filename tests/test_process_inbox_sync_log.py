"""Thin wiring tests: process_inbox sync.log source and terminal events."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.workflows.inbox_workflow import process_inbox
from spejder.workflows.skill_hygiene import SkillHygieneResult
from spejder.workflows.sync_log import SyncRunLog

# Capture before any test patches replace SyncRunLog.open on the shared class.
_REAL_SYNC_RUN_LOG_OPEN = SyncRunLog.open.__func__

_HYGIENE_EMPTY = SkillHygieneResult(
    blocked_cleanup={
        "skills_processed": 0,
        "skill_rows_deleted": 0,
        "job_skill_links_deleted": 0,
        "affected_job_ids": [],
    },
    blocked_rescored=0,
    stale_cleanup={
        "skills_deleted": 0,
        "skill_rows_deleted": 0,
        "job_skill_links_deleted": 0,
        "affected_job_ids": [],
        "profile_removed": 0,
        "deleted_skill_names": [],
    },
    stale_rescored=0,
    cloud_stats={"seeded": False, "pruned": []},
    new_threshold=0.1,
    previous_threshold=None,
    threshold_changed=False,
)


def _fake_hygiene(db_path, profile, *, on_stage=None):
    if on_stage is not None:
        on_stage("blocked_skills", "Cleaning blocked skills from database")
        on_stage("stale_skills", "Cleaning stale low-share skills")
        on_stage("bad_cloud", "Initializing bad cloud")
    return _HYGIENE_EMPTY


def _open_sync_log_quiet(path: str, *, echo_stdout: bool = True, **kwargs):
    """File-only open so integration tests do not dump ``sync …`` to unittest stdout."""
    return _REAL_SYNC_RUN_LOG_OPEN(SyncRunLog, path, echo_stdout=False, **kwargs)


class ProcessInboxSyncLogTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.inbox = os.path.join(self._tmpdir.name, "inbox")
        os.makedirs(self.inbox)
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir)
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        with open(self.profile_path, "w", encoding="utf-8") as handle:
            json.dump({"itday_portal_sync_enabled": False}, handle)
        self.sync_log_path = os.path.join(self.report_dir, "sync.log")
        self._echo_patcher = patch(
            "spejder.workflows.inbox_workflow.SyncRunLog.open",
            side_effect=_open_sync_log_quiet,
        )
        self._echo_patcher.start()
        self.addCleanup(self._echo_patcher.stop)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _read_log(self) -> str:
        with open(self.sync_log_path, encoding="utf-8") as handle:
            return handle.read()

    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_refresh",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    def test_skip_path_writes_source_pipeline_end_and_run_end(
        self, _portal, _desc_refresh
    ):
        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="",
        )

        text = self._read_log()
        self.assertIn("event=run_start", text)
        self.assertIn("source=process_inbox", text)
        self.assertIn("event=pipeline_end", text)
        self.assertIn("status=skipped", text)
        self.assertIn("event=run_end", text)
        self.assertIn("status=skipped", text.split("event=run_end", 1)[1])

    @patch(
        "spejder.workflows.inbox_workflow.write_inbox_dashboard_report",
        return_value=None,
    )
    @patch(
        "spejder.workflows.inbox_workflow.summarize_relevant_jobs_for_inbox",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.update_profile_from_db_signals",
        return_value={
            "labeled_count": 0,
            "learned_include_count": 0,
            "learned_exclude_count": 0,
            "missing_skills_count": 0,
        },
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.inbox_workflow._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
    )
    @patch("spejder.workflows.inbox_workflow.get_relevant_jobs", return_value=[])
    @patch("spejder.workflows.inbox_workflow.materialize_relevant_and_applied_skills")
    @patch(
        "spejder.workflows.inbox_workflow._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch(
        "spejder.workflows.inbox_workflow.delete_processed_inbox_files",
        return_value={"eligible": 0, "deleted": 0, "missing": 0, "failed": 0},
    )
    @patch("spejder.workflows.inbox_workflow.print_ingest_file_stats")
    @patch(
        "spejder.workflows.inbox_workflow.ingest_docs_to_db",
        return_value={
            "processed": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "files": [],
        },
    )
    @patch("spejder.workflows.inbox_workflow.LocalLLM")
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_refresh",
        return_value=[{"id": 1}],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    def test_success_path_writes_source_pipeline_end_and_run_end(
        self,
        _portal,
        _desc_refresh,
        mock_llm,
        _ingest,
        _print_stats,
        _delete_files,
        _gen_desc,
        _materialize,
        _relevant,
        _learn,
        _hygiene,
        _signals,
        _summarize,
        _report,
    ):
        mock_llm.return_value = object()

        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="/fake/model.gguf",
        )

        text = self._read_log()
        self.assertIn("event=run_start", text)
        self.assertIn("source=process_inbox", text)
        for stage in (
            "ingest",
            "cleanup",
            "descriptions",
            "skills",
            "patterns",
            "blocked_skills",
            "stale_skills",
            "bad_cloud",
        ):
            self.assertIn(f"event=stage_start stage={stage}", text)
        self.assertIn("event=pipeline_end", text)
        self.assertIn("status=done", text)
        self.assertIn("event=run_end", text)
        self.assertIn("status=complete", text.split("event=run_end", 1)[1])

    @patch(
        "spejder.workflows.inbox_workflow.write_inbox_dashboard_report",
        return_value=None,
    )
    @patch(
        "spejder.workflows.inbox_workflow.summarize_relevant_jobs_for_inbox",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.update_profile_from_db_signals",
        return_value={
            "labeled_count": 0,
            "learned_include_count": 0,
            "learned_exclude_count": 0,
            "missing_skills_count": 0,
        },
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.inbox_workflow._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
    )
    @patch("spejder.workflows.inbox_workflow.get_relevant_jobs", return_value=[])
    @patch("spejder.workflows.inbox_workflow.materialize_relevant_and_applied_skills")
    @patch(
        "spejder.workflows.inbox_workflow._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch(
        "spejder.workflows.inbox_workflow.delete_processed_inbox_files",
        return_value={"eligible": 0, "deleted": 0, "missing": 0, "failed": 0},
    )
    @patch("spejder.workflows.inbox_workflow.print_ingest_file_stats")
    @patch(
        "spejder.workflows.inbox_workflow.ingest_docs_to_db",
        return_value={
            "processed": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "files": [],
        },
    )
    @patch("spejder.workflows.inbox_workflow.LocalLLM")
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_refresh",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_cross_source_dedupe",
        return_value={},
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 3,
            "inserted_new": 2,
            "skipped_existing": 1,
            "processed": 3,
        },
    )
    def test_portal_enabled_writes_portal_and_portal_dedupe_stages(
        self,
        _portal,
        _dedupe,
        _desc_refresh,
        mock_llm,
        _ingest,
        _print_stats,
        _delete_files,
        _gen_desc,
        _materialize,
        _relevant,
        _learn,
        _hygiene,
        _signals,
        _summarize,
        _report,
    ):
        with open(self.profile_path, "w", encoding="utf-8") as handle:
            json.dump({"itday_portal_sync_enabled": True}, handle)
        mock_llm.return_value = object()

        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="/fake/model.gguf",
        )

        text = self._read_log()
        self.assertIn("event=stage_start stage=portal", text)
        self.assertIn("event=stage_start stage=portal_dedupe", text)

    @patch(
        "spejder.workflows.inbox_workflow.write_inbox_dashboard_report",
        return_value=None,
    )
    @patch(
        "spejder.workflows.inbox_workflow.summarize_relevant_jobs_for_inbox",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.update_profile_from_db_signals",
        return_value={
            "labeled_count": 0,
            "learned_include_count": 0,
            "learned_exclude_count": 0,
            "missing_skills_count": 0,
        },
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.inbox_workflow._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
    )
    @patch("spejder.workflows.inbox_workflow.get_relevant_jobs", return_value=[])
    @patch("spejder.workflows.inbox_workflow.materialize_relevant_and_applied_skills")
    @patch(
        "spejder.workflows.inbox_workflow._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch(
        "spejder.workflows.inbox_workflow.delete_processed_inbox_files",
        return_value={"eligible": 0, "deleted": 0, "missing": 0, "failed": 0},
    )
    @patch("spejder.workflows.inbox_workflow.print_ingest_file_stats")
    @patch("spejder.workflows.inbox_workflow.ingest_docs_to_db")
    @patch("spejder.workflows.inbox_workflow.LocalLLM")
    @patch(
        "spejder.workflows.inbox_workflow.email_parser.load_files",
        return_value=[{"path": "/tmp/dup.eml", "html": ""}],
    )
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_refresh",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    def test_duplicate_heavy_ingest_writes_final_progress(
        self,
        _portal,
        _desc_refresh,
        _load_files,
        mock_llm,
        mock_ingest,
        _print_stats,
        _delete_files,
        _gen_desc,
        _materialize,
        _relevant,
        _learn,
        _hygiene,
        _signals,
        _summarize,
        _report,
    ):
        mock_llm.return_value = object()

        def _ingest(_db, _docs, **kwargs):
            on_progress = kwargs.get("on_progress")
            # All duplicates: inserted_new stays 0 after the first tick.
            if on_progress is not None:
                for processed in range(1, 11):
                    on_progress(processed, 0, processed)
            return {
                "processed": 10,
                "inserted_new": 0,
                "skipped_existing": 10,
                "positions_by_file": [],
            }

        mock_ingest.side_effect = _ingest

        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="/fake/model.gguf",
        )

        text = self._read_log()
        progress_lines = [
            line for line in text.splitlines() if "event=progress stage=ingest" in line
        ]
        self.assertGreaterEqual(len(progress_lines), 2)
        self.assertTrue(
            any("checked=10/0" in line for line in progress_lines),
            msg=f"expected final processed=10 in {progress_lines!r}",
        )


if __name__ == "__main__":
    unittest.main()
