"""Tests for run_inbox_sync pipeline orchestration."""

import os
import tempfile
import unittest
from unittest.mock import ANY, patch

from spejder.config import AppConfig
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync
from spejder.workflows.skill_hygiene import SkillHygieneResult

_LEARN_EMPTY = {
    "considered_positions": 0,
    "new_skill_patterns": 0,
    "total_known_skill_patterns": 0,
}

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

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[{"id": 1}])
    def test_skips_skills_rebuild_when_nothing_updated(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        mock_hygiene,
        _portal,
    ):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        mock_hygiene.assert_called_once()
        self.assertEqual(mock_hygiene.call_args.args[0], self.context.db_path)
        self.assertIs(mock_hygiene.call_args.args[1], self.context.runtime_profile)
        self.assertFalse(
            any("skills materialized" in reason for reason in self.rebuild_reasons),
            msg=f"unexpected rebuild reasons: {self.rebuild_reasons}",
        )

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 1,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 1,
        },
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[{"id": 1}])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[{"id": 1}])
    def test_queues_skills_rebuild_when_jobs_updated(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        mock_hygiene,
        _portal,
    ):
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
        mock_hygiene.assert_called_once_with(
            context.db_path,
            context.runtime_profile,
            on_stage=ANY,
        )
        self.assertIn("skills materialized=3", self.rebuild_reasons)


if __name__ == "__main__":
    unittest.main()
