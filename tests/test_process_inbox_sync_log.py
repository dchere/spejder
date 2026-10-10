"""Thin wiring tests: process_inbox sync.log source and terminal events."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.workflows.inbox_workflow import process_inbox
from spejder.workflows.profile_learning import ProfileLearningResult
from spejder.workflows.progress_eta import ingest_eta_store_path
from spejder.workflows.skill_hygiene import SkillHygieneResult
from spejder.workflows.sync_log import SyncRunLog

# Capture before any test patches replace SyncRunLog.open on the shared class.
_REAL_SYNC_RUN_LOG_OPEN = SyncRunLog.open.__func__

_PROFILE_LEARNING_EMPTY = ProfileLearningResult(
    learning_info={
        "labeled_count": 0,
        "learned_include_count": 0,
        "learned_exclude_count": 0,
        "missing_skills_count": 0,
    },
    keywords_changed=False,
    suggestions_changed=False,
)


def _fake_profile_learning(db_path, profile_path, *, on_stage=None):
    if on_stage is not None:
        on_stage("profile_learning", "Learning profile keywords")
    return _PROFILE_LEARNING_EMPTY


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
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
    )
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
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
        self, _portal, mock_desc_refresh, mock_profile_learning
    ):
        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="",
        )

        kwargs = mock_desc_refresh.call_args.kwargs
        self.assertEqual(kwargs.get("limit"), 1)
        mock_profile_learning.assert_not_called()

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
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
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
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
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
            "profile_learning",
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
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
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
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
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
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
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
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
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

    @patch(
        "spejder.workflows.inbox_workflow.write_inbox_dashboard_report",
        return_value=None,
    )
    @patch(
        "spejder.workflows.inbox_workflow.summarize_relevant_jobs_for_inbox",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
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
        return_value=[{"path": "/tmp/one.eml", "html": ""}],
    )
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
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
    def test_ingest_eta_store_and_clears_gated_eta(
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
        """process_inbox wires eta_store_path; same-counts eta clear still emits."""
        mock_llm.return_value = object()

        def _ingest(_db, _docs, **kwargs):
            self.assertEqual(
                kwargs.get("eta_store_path"),
                ingest_eta_store_path(self.db_path),
            )
            on_progress = kwargs["on_progress"]
            self.assertIsNotNone(on_progress)
            # Mid-run ETA, then same-counts clear (tracker would gate without bypass).
            on_progress(1, 1, 0, 90.0)
            on_progress(1, 1, 0, None)
            return {
                "processed": 1,
                "inserted_new": 1,
                "skipped_existing": 0,
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
        with_eta = [line for line in progress_lines if "eta_s=" in line]
        self.assertTrue(with_eta, msg=f"expected mid-run eta_s in {progress_lines!r}")
        self.assertIn("eta_s=90.0", with_eta[0])
        # Exactly one clear tick after mid-run ETA (no needs_final duplicate).
        self.assertEqual(
            len(progress_lines),
            2,
            msg=f"expected mid-run + clear only, got {progress_lines!r}",
        )
        after_clear = progress_lines[progress_lines.index(with_eta[0]) + 1 :]
        self.assertEqual(len(after_clear), 1)
        for line in after_clear:
            self.assertNotIn("eta_s=", line, msg=f"stale eta after clear: {line!r}")
            self.assertNotIn("eta=", line, msg=f"stale eta after clear: {line!r}")


if __name__ == "__main__":
    unittest.main()
