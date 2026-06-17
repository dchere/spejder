"""API tests for applied manual raw-text endpoint."""

import os
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_applied, upsert_job
from spejder.db.connection import _connect
from spejder.server import create_app


def _insert_applied_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Engineer",
            "position_link": link,
            "raw_text": "original snippet",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    set_job_applied(db_path, job_id, True)
    return job_id


def create_test_app(db_path: str, report_dir: str, runtime_profile: AppConfig):
    rebuild_calls = []

    def queue_dashboard_rebuild(reason: str = ""):
        rebuild_calls.append(reason)

    app = create_app(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=runtime_profile,
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=queue_dashboard_rebuild,
        cli_verbose=False,
    )
    return app, rebuild_calls


class ServerAppliedRawTextApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.runtime_profile = AppConfig()
        self.app, self.rebuild_calls = create_test_app(
            self.db_path, self.report_dir, self.runtime_profile
        )
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.server.materialize_job_skills")
    def test_applied_raw_text_rescores_after_skill_clear(self, mock_materialize):
        mock_materialize.return_value = ("", "enriched", False)
        job_id = _insert_applied_job(self.db_path, "https://example.com/applied-raw")

        response = self.client.post(
            "/api/applied/raw-text",
            json={"job_id": job_id, "text": "Requires Python and SQL."},
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        mock_materialize.assert_called_once()
        kwargs = mock_materialize.call_args.kwargs
        self.assertTrue(kwargs.get("rescore"))
        self.assertTrue(kwargs.get("first_materialize"))
        self.assertIn(f"manual raw text job {job_id}", self.rebuild_calls)

    def test_applied_raw_text_rejects_empty_text(self):
        job_id = _insert_applied_job(self.db_path, "https://example.com/empty-text")

        response = self.client.post(
            "/api/applied/raw-text",
            json={"job_id": job_id, "text": "   "},
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["error"], "text is required")

    @patch("spejder.server.get_all_applied_jobs", return_value=[])
    def test_applied_raw_text_fails_when_row_missing_after_save(self, _mock_rows):
        job_id = _insert_applied_job(self.db_path, "https://example.com/missing-row")

        response = self.client.post(
            "/api/applied/raw-text",
            json={"job_id": job_id, "text": "Manual description"},
        )

        self.assertEqual(response.status_code, 500)
        self.assertFalse(response.json()["ok"])
        self.assertIn("enrichment row missing", response.json()["error"])
        self.assertEqual(self.rebuild_calls, [])

    @patch("spejder.workflows.job_skills_materialize.rescore_job_by_id")
    @patch("spejder.workflows.job_skills_materialize._get_or_extract_job_skills")
    @patch("spejder.workflows.job_skills_materialize._enrich_raw_text_with_position_page")
    def test_applied_raw_text_rescores_via_first_materialize_without_skill_change(
        self, mock_enrich, mock_extract, mock_rescore
    ):
        mock_enrich.return_value = "enriched manual body"
        mock_extract.return_value = ("", False)
        job_id = _insert_applied_job(self.db_path, "https://example.com/integration-rescore")

        response = self.client.post(
            "/api/applied/raw-text",
            json={"job_id": job_id, "text": "Requires Python."},
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        mock_rescore.assert_called_once()
        self.assertEqual(mock_rescore.call_args.args[2], job_id)

    def test_applied_raw_text_rejects_non_applied_job(self):
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/not-applied",
                "raw_text": "raw",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM jobs WHERE position_link=?",
                ("https://example.com/not-applied",),
            )
            job_id = int(cur.fetchone()[0])
        finally:
            conn.close()

        response = self.client.post(
            "/api/applied/raw-text",
            json={"job_id": job_id, "text": "Manual description"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
