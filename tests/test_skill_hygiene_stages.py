"""Tests for shared skill hygiene stages (blocked → stale → bad cloud)."""

import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_skills, upsert_job, upsert_skill_pattern
from spejder.db.connection import _connect
from spejder.workflows.skill_hygiene import (
    SkillHygieneResult,
    run_skill_hygiene_stages,
)


def _insert_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Engineer",
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


class RunSkillHygieneStagesTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_stage_callback_order(self):
        stages: list[str] = []
        with (
            patch(
                "spejder.workflows.skill_hygiene.cleanup_blocked_skills_from_db",
                return_value={
                    "skills_processed": 0,
                    "skill_rows_deleted": 0,
                    "job_skill_links_deleted": 0,
                    "affected_job_ids": [],
                },
            ) as mock_blocked,
            patch(
                "spejder.workflows.skill_hygiene.run_stale_skill_cleanup",
                return_value={
                    "skills_deleted": 0,
                    "skill_rows_deleted": 0,
                    "job_skill_links_deleted": 0,
                    "affected_job_ids": [],
                    "profile_removed": 0,
                    "deleted_skill_names": [],
                },
            ) as mock_stale,
            patch(
                "spejder.workflows.skill_hygiene.rescore_jobs_if_active",
                return_value=0,
            ),
            patch(
                "spejder.workflows.skill_hygiene.ensure_bad_cloud_initialized",
                return_value={"seeded": False, "pruned": []},
            ) as mock_cloud,
            patch(
                "spejder.workflows.skill_hygiene.recalibrate_and_store_threshold",
                return_value=0.1,
            ) as mock_recal,
        ):
            profile = AppConfig(blocked_skills=["noise phrase"])
            result = run_skill_hygiene_stages(
                self.db_path,
                profile,
                on_stage=lambda stage_id, _message: stages.append(stage_id),
            )

        self.assertEqual(stages, ["blocked_skills", "stale_skills", "bad_cloud"])
        mock_blocked.assert_called_once_with(self.db_path, ["noise phrase"])
        mock_stale.assert_called_once_with(self.db_path, profile)
        mock_cloud.assert_called_once_with(profile, self.db_path)
        mock_recal.assert_called_once_with(profile, self.db_path)
        self.assertIsInstance(result, SkillHygieneResult)
        self.assertFalse(result.profile_dirty)

    def test_profile_dirty_when_threshold_changes(self):
        profile = AppConfig(skill_bigram_toxicity_threshold=0.2)
        with (
            patch(
                "spejder.workflows.skill_hygiene.cleanup_blocked_skills_from_db",
                return_value={
                    "skills_processed": 0,
                    "skill_rows_deleted": 0,
                    "job_skill_links_deleted": 0,
                    "affected_job_ids": [],
                },
            ),
            patch(
                "spejder.workflows.skill_hygiene.run_stale_skill_cleanup",
                return_value={
                    "skills_deleted": 0,
                    "skill_rows_deleted": 0,
                    "job_skill_links_deleted": 0,
                    "affected_job_ids": [],
                    "profile_removed": 0,
                    "deleted_skill_names": [],
                },
            ),
            patch(
                "spejder.workflows.skill_hygiene.rescore_jobs_if_active",
                return_value=0,
            ),
            patch(
                "spejder.workflows.skill_hygiene.ensure_bad_cloud_initialized",
                return_value={"seeded": False, "pruned": []},
            ),
            patch(
                "spejder.workflows.skill_hygiene.recalibrate_and_store_threshold",
                side_effect=lambda p, _db: setattr(
                    p, "skill_bigram_toxicity_threshold", 0.5
                )
                or 0.5,
            ),
        ):
            result = run_skill_hygiene_stages(self.db_path, profile)

        self.assertTrue(result.threshold_changed)
        self.assertTrue(result.profile_dirty)
        self.assertTrue(result.cloud_needs_rebuild())

    def test_blocked_cleanup_removes_db_rows(self):
        upsert_skill_pattern(
            self.db_path, name="Python", pattern=r"\bPython\b", source="test"
        )
        job_id = _insert_job(self.db_path, "https://example.com/python-job")
        set_job_skills(self.db_path, job_id, ["Python"])
        profile = AppConfig(blocked_skills=["Python"], bad_cloud_seeded=True)
        with (
            patch(
                "spejder.workflows.skill_hygiene.ensure_bad_cloud_initialized",
                return_value={"seeded": False, "pruned": []},
            ),
            patch(
                "spejder.workflows.skill_hygiene.recalibrate_and_store_threshold",
                return_value=0.1,
            ),
            patch(
                "spejder.workflows.skill_hygiene.rescore_jobs_if_active",
                return_value=1,
            ),
        ):
            result = run_skill_hygiene_stages(self.db_path, profile)

        self.assertEqual(result.blocked_cleanup.get("skills_processed"), 1)
        self.assertEqual(result.blocked_rescored, 1)
        self.assertTrue(result.blocked_needs_rebuild())
        self.assertIn(job_id, result.blocked_cleanup.get("affected_job_ids", []))


if __name__ == "__main__":
    unittest.main()
