"""Tests for shared filtered job_skills read path."""

import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_job_skills,
    replace_job_skills,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect
from spejder.extractors.skill_extractor import (
    get_job_skills_filtered,
    get_job_skills_filtered_for_jobs,
)
from spejder.extractors.skill_extractor.bad_cloud import ingest_blocked_skill
from spejder.jobs.rescore import rescore_job_by_id
from spejder.workflows.dashboard_records import build_dashboard_record


class GetJobSkillsFilteredTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/filtered-skills",
                "raw_text": "Requirements: python and sql.",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM jobs WHERE position_link=?",
                ("https://example.com/filtered-skills",),
            )
            self.job_id = int(cur.fetchone()[0])
        finally:
            conn.close()
        upsert_skill_pattern(self.db_path, "python", r"\bpython\b", source="seed")
        replace_job_skills(
            self.db_path, self.job_id, ["python", "we are looking", "sql"]
        )
        ingest_blocked_skill("we are looking", self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_drops_toxic_and_rewrites_cache(self):
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        filtered = get_job_skills_filtered(self.db_path, self.job_id, profile)
        self.assertEqual(sorted(s.lower() for s in filtered), ["python", "sql"])
        self.assertEqual(
            sorted(s.lower() for s in get_job_skills(self.db_path, self.job_id)),
            ["python", "sql"],
        )

    def test_rewrite_cache_false_leaves_db(self):
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        filtered = get_job_skills_filtered(
            self.db_path, self.job_id, profile, rewrite_cache=False
        )
        self.assertEqual(sorted(s.lower() for s in filtered), ["python", "sql"])
        raw = get_job_skills(self.db_path, self.job_id)
        self.assertIn("we are looking", [s.lower() for s in raw])

    def test_batch_variant(self):
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        by_job = get_job_skills_filtered_for_jobs(
            self.db_path, [self.job_id, 0, self.job_id], profile
        )
        self.assertEqual(list(by_job.keys()), [self.job_id])
        self.assertEqual(
            sorted(s.lower() for s in by_job[self.job_id]), ["python", "sql"]
        )

    def test_drops_exact_blocked(self):
        profile = AppConfig(blocked_skills=["sql"])
        filtered = get_job_skills_filtered(
            self.db_path, self.job_id, profile, rewrite_cache=False
        )
        keys = {s.lower() for s in filtered}
        self.assertIn("python", keys)
        self.assertNotIn("sql", keys)


class FilteredSkillsConsumersTest(unittest.TestCase):
    def test_dashboard_record_uses_filtered_helper(self):
        with patch(
            "spejder.workflows.dashboard_records.get_job_skills_filtered",
            return_value=["python"],
        ) as mock_filtered:
            result = build_dashboard_record(
                db_path="/tmp/jobs.db",
                runtime_profile=AppConfig(),
                title_translation_cache={},
                row={"id": 1, "title": "Eng", "title_english": "Eng"},
                default_category="relevant",
                translate_title=False,
            )
        mock_filtered.assert_called_once()
        self.assertIn("python", result["skills"])

    def test_rescore_uses_filtered_helper(self):
        with patch(
            "spejder.jobs.rescore.get_job_skills_filtered", return_value=["python"]
        ) as mock_filtered, patch(
            "spejder.jobs.rescore.get_job_for_rescoring",
            return_value={
                "id": 9,
                "source": "Test",
                "title": "Eng",
                "company": "Acme",
                "position_link": "https://example.com/x",
                "raw_text": "python",
                "applied": 0,
            },
        ), patch(
            "spejder.jobs.rescore._load_skill_patterns", return_value=[]
        ), patch(
            "spejder.jobs.rescore.get_applied_pipeline_company_keys", return_value=set()
        ), patch(
            "spejder.jobs.rescore.score_relevance",
            return_value=(1.0, "ok", 1, "relevant"),
        ) as mock_score, patch(
            "spejder.jobs.rescore.update_jobs_relevance"
        ):
            ok = rescore_job_by_id("/tmp/jobs.db", AppConfig(), 9)
        self.assertTrue(ok)
        mock_filtered.assert_called_once()
        kwargs = mock_score.call_args.kwargs
        self.assertEqual(kwargs["cached_required_skills"], ["python"])


if __name__ == "__main__":
    unittest.main()
