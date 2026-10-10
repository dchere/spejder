"""Integration tests for run_profile_keyword_learning dirty detection."""

import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_feedback, upsert_job
from spejder.db.connection import _connect
from spejder.workflows.profile_learning import run_profile_keyword_learning


def _insert_job(db_path: str, link: str, *, title: str, raw_text: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
            "position_link": link,
            "raw_text": raw_text,
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        row = cur.fetchone()
        if row is None:
            raise AssertionError(f"job not found for link={link!r}")
        return int(row[0])
    finally:
        conn.close()


class ProfileLearningDirtyDetectionTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        ensure_db(self.db_path)
        AppConfig().save(self.profile_path)

        # Distinctive tokens: include needs rel_df>=2 + delta>=0.2;
        # exclude needs nrel_df>=2 + delta<=-0.2 (see suggestions.py).
        # Unique titles so company+title dedupe does not merge rows.
        for i in range(2):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/rel-{i}",
                title=f"Relevant Engineer {i}",
                raw_text="kubernetes orchestration cluster platform",
            )
            set_job_feedback(self.db_path, job_id, "relevant")
        for i in range(2):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/nrel-{i}",
                title=f"Not Relevant Engineer {i}",
                raw_text="cobol mainframe legacy batch",
            )
            set_job_feedback(self.db_path, job_id, "not relevant")

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_first_run_changes_lists_second_run_unchanged(self):
        stages: list[tuple[str, str]] = []
        first = run_profile_keyword_learning(
            self.db_path,
            self.profile_path,
            on_stage=lambda sid, msg: stages.append((sid, msg)),
        )
        self.assertEqual(stages, [("profile_learning", "Learning profile keywords")])
        self.assertTrue(first.keywords_changed)
        self.assertTrue(first.profile_changed)
        self.assertEqual(first.learning_info["labeled_count"], 4)
        self.assertGreater(first.learning_info["learned_include_count"], 0)
        self.assertGreater(first.learning_info["learned_exclude_count"], 0)

        profile = AppConfig.load(self.profile_path)
        self.assertIn("kubernetes", profile.learned_include_keywords)
        self.assertIn("cobol", profile.learned_exclude_keywords)

        second = run_profile_keyword_learning(self.db_path, self.profile_path)
        self.assertFalse(second.keywords_changed)
        self.assertFalse(second.suggestions_changed)
        self.assertFalse(second.profile_changed)
        self.assertEqual(second.learning_info["labeled_count"], 4)
        self.assertGreater(second.learning_info["learned_include_count"], 0)
        self.assertEqual(
            AppConfig.load(self.profile_path).learned_include_keywords,
            profile.learned_include_keywords,
        )


if __name__ == "__main__":
    unittest.main()
