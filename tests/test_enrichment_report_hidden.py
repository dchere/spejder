"""Regression: enrichment report write includes Hidden tab items."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_hidden, upsert_job
from spejder.db.connection import _connect
from spejder.workflows.enrichment import refresh_descriptions


def _insert_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Parked Engineer",
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


class EnrichmentReportHiddenTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.workflows.enrichment._learn_skill_patterns_from_positions")
    @patch("spejder.workflows.enrichment.materialize_relevant_and_applied_skills")
    @patch("spejder.workflows.enrichment.materialize_job_skills")
    @patch("spejder.workflows.enrichment.set_job_description")
    @patch("spejder.workflows.enrichment._enrich_raw_text_with_position_page")
    @patch("spejder.workflows.enrichment._build_description_summary")
    @patch("spejder.workflows.enrichment.get_jobs_for_description_refresh")
    @patch("spejder.workflows.enrichment.LocalLLM")
    @patch("spejder.workflows.enrichment.load_runtime_profile")
    def test_refresh_descriptions_report_includes_hidden(
        self,
        mock_load_profile,
        mock_llm,
        mock_get_rows,
        mock_summary,
        mock_enrich,
        _mock_set_desc,
        mock_materialize,
        _mock_materialize_batch,
        mock_learn,
    ):
        mock_load_profile.return_value = AppConfig()
        mock_llm.return_value = MagicMock()
        mock_materialize.return_value = ([], "raw", False)
        mock_learn.return_value = {
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        }
        mock_enrich.return_value = "enriched raw"
        mock_summary.return_value = "summary"

        job_id = _insert_job(self.db_path, "https://www.linkedin.com/jobs/view/1234567890")
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        mock_get_rows.return_value = [
            {
                "id": job_id,
                "source": "Test",
                "company": "Acme",
                "title": "Parked Engineer",
                "position_link": "https://www.linkedin.com/jobs/view/1234567890",
                "raw_text": "raw",
                "description": "",
                "category": "relevant",
            }
        ]

        refresh_descriptions(
            profile=os.path.join(self._tmpdir.name, "profile.json"),
            db=self.db_path,
            model="fake-model",
            quiet_model=True,
            report_dir=self.report_dir,
            allow_empty=True,
        )

        path = os.path.join(self.report_dir, "report.html")
        with open(path, encoding="utf-8") as handle:
            html = handle.read()

        self.assertIn("Hidden (1)", html)
        self.assertIn("Parked Engineer", html)
        self.assertIn(f"setHidden({job_id}", html)


if __name__ == "__main__":
    unittest.main()
