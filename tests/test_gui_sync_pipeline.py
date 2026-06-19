"""Tests for run_inbox_sync pipeline orchestration."""

import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync


class RunInboxSyncRebuildTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.rebuild_reasons: list[str] = []
        self.context = GuiSyncContext(
            db_path=os.path.join(self._tmpdir.name, "jobs.db"),
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.workflows.gui_sync.should_sync_skill_antipatterns", return_value=False)
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
    )
    @patch(
        "spejder.workflows.gui_sync.cleanup_blocked_skills_from_db",
        return_value={
            "skills_processed": 0,
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
        },
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[{"id": 1}])
    def test_skips_skills_rebuild_when_nothing_updated(
        self,
        *_mocks,
    ):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        self.assertFalse(
            any("skills materialized" in reason for reason in self.rebuild_reasons),
            msg=f"unexpected rebuild reasons: {self.rebuild_reasons}",
        )

    @patch("spejder.workflows.gui_sync.should_sync_skill_antipatterns", return_value=False)
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 1,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 1,
        },
    )
    @patch(
        "spejder.workflows.gui_sync.cleanup_blocked_skills_from_db",
        return_value={
            "skills_processed": 0,
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
        },
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[{"id": 1}])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[{"id": 1}])
    def test_queues_skills_rebuild_when_jobs_updated(self, *_mocks):
        context = GuiSyncContext(
            db_path=self.context.db_path,
            inbox_path=self.context.inbox_path,
            model_path="",
            profile_path=self.context.profile_path,
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 3,
        )
        result = run_inbox_sync(context)
        self.assertEqual(result.status, "done")
        self.assertIn("skills materialized=3", self.rebuild_reasons)


if __name__ == "__main__":
    unittest.main()
