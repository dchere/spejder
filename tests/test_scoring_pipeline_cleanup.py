"""Tests for scoring pipeline cleanup (replace_job_skills, rescore scope)."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_job_skills,
    replace_job_skills,
    set_job_applied,
    set_job_viewed,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect
from spejder.jobs.scoring import job_in_active_rescore_scope, rescore_jobs_if_active
from spejder.workflows.job_skills_materialize import materialize_job_skills


def _insert_job(
    db_path: str,
    link: str,
    *,
    company: str = "Acme",
    title: str = "Engineer",
    viewed: int = 0,
    applied: int = 0,
) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": company,
            "title": title,
            "position_link": link,
            "raw_text": "Requires python and docker.",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
        if viewed:
            set_job_viewed(db_path, job_id, True)
        if applied:
            set_job_applied(db_path, job_id, True)
        return job_id
    finally:
        conn.close()


class ReplaceJobSkillsTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_replace_updates_links_and_returns_true(self):
        job_id = _insert_job(self.db_path, "https://example.com/replace-job")
        changed = replace_job_skills(self.db_path, job_id, ["Python", "SQL"])
        self.assertTrue(changed)
        self.assertEqual(sorted(get_job_skills(self.db_path, job_id)), ["Python", "SQL"])

        changed_again = replace_job_skills(self.db_path, job_id, ["SQL", "python"])
        self.assertFalse(changed_again)
        self.assertEqual(sorted(get_job_skills(self.db_path, job_id)), ["Python", "SQL"])

    def test_replace_clears_existing_links(self):
        job_id = _insert_job(self.db_path, "https://example.com/clear-job")
        replace_job_skills(self.db_path, job_id, ["Python"])
        changed = replace_job_skills(self.db_path, job_id, ["Rust"])
        self.assertTrue(changed)
        self.assertEqual(get_job_skills(self.db_path, job_id), ["Rust"])

    def test_set_job_skills_delegates_to_replace(self):
        job_id = _insert_job(self.db_path, "https://example.com/alias-job")
        from spejder.db import set_job_skills

        set_job_skills(self.db_path, job_id, ["Go"])
        self.assertEqual(get_job_skills(self.db_path, job_id), ["Go"])


class JobInActiveRescoreScopeTest(unittest.TestCase):
    def test_active_scope_rules(self):
        self.assertTrue(job_in_active_rescore_scope({"viewed": 0}))
        self.assertFalse(job_in_active_rescore_scope({"viewed": 1}))
        self.assertTrue(job_in_active_rescore_scope({"viewed": 1, "applied": 1}))
        self.assertTrue(job_in_active_rescore_scope({"viewed": 1, "on_interview": 1}))
        self.assertTrue(job_in_active_rescore_scope({"viewed": 1, "interview_stopped": 1}))


class MaterializeRescoreGuardTest(unittest.TestCase):
    @patch("spejder.workflows.job_skills_materialize.rescore_job_by_id")
    @patch("spejder.workflows.job_skills_materialize._get_or_extract_job_skills")
    @patch("spejder.workflows.job_skills_materialize._enrich_raw_text_with_position_page")
    def test_rescore_only_when_skills_changed_and_in_scope(
        self, mock_enrich, mock_extract, mock_rescore
    ):
        mock_enrich.return_value = "enriched"
        mock_extract.return_value = ("python", False)
        row = {"id": 1, "viewed": 0}
        profile = MagicMock(spec=AppConfig)

        materialize_job_skills(
            "jobs.db",
            row,
            runtime_profile=profile,
            rescore=True,
        )
        mock_rescore.assert_not_called()

        mock_extract.return_value = ("python", True)
        materialize_job_skills(
            "jobs.db",
            row,
            runtime_profile=profile,
            rescore=True,
        )
        mock_rescore.assert_called_once()

        mock_rescore.reset_mock()
        mock_extract.return_value = ("", False)
        materialize_job_skills(
            "jobs.db",
            row,
            runtime_profile=profile,
            rescore=True,
            first_materialize=True,
        )
        mock_rescore.assert_called_once()

        mock_rescore.reset_mock()
        mock_extract.return_value = ("python", True)
        materialize_job_skills(
            "jobs.db",
            {"id": 2, "viewed": 1, "applied": 0},
            runtime_profile=profile,
            rescore=True,
        )
        mock_rescore.assert_not_called()


class RescoreJobsIfActiveTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.profile = AppConfig()
        self.profile.include_keywords = []
        self.profile.exclude_keywords = []
        self.profile.min_score = 0.0
        self.profile.user_skills = ["python"]
        self.profile.skill_match_weight = 1.0
        self.profile.skill_missing_penalty = 0.5
        self.profile.easy_apply_bonus = 0.0

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_rescore_skips_viewed_non_applied_jobs(self):
        active_id = _insert_job(self.db_path, "https://example.com/active", company="ActiveCo")
        inactive_id = _insert_job(
            self.db_path,
            "https://example.com/inactive",
            company="InactiveCo",
            viewed=1,
        )
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="test",
        )
        replace_job_skills(self.db_path, active_id, ["Python"])
        replace_job_skills(self.db_path, inactive_id, ["Python"])

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE jobs SET relevance_reason=? WHERE id=?",
                ("manual_feedback=relevant", inactive_id),
            )
            conn.commit()
        finally:
            conn.close()

        rescored = rescore_jobs_if_active(
            self.db_path,
            self.profile,
            [active_id, inactive_id],
        )
        self.assertEqual(rescored, 1)


if __name__ == "__main__":
    unittest.main()
