"""API tests for cover letter endpoints."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_applied, upsert_job
from spejder.db.connection import _connect
from spejder.server import create_app


def _insert_job(db_path: str, link: str, *, applied: bool = False) -> int:
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
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    if applied:
        set_job_applied(db_path, job_id, True)
    return job_id


def create_test_app(db_path: str, report_dir: str):
    rebuild_calls = []

    def queue_dashboard_rebuild(reason: str):
        rebuild_calls.append(reason)

    app = create_app(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=AppConfig(),
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=queue_dashboard_rebuild,
        cli_verbose=False,
    )
    return app, rebuild_calls


class ServerCoverLetterApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.app, self.rebuild_calls = create_test_app(self.db_path, self.report_dir)
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_cover_letter_request_requires_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/cover-pending", applied=False)
        response = self.client.post(
            "/api/applied/cover-letter/request",
            json={"job_id": job_id, "requested": True},
        )
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])

    def test_cover_letter_request_and_save(self):
        job_id = _insert_job(self.db_path, "https://example.com/cover-save", applied=True)
        response = self.client.post(
            "/api/applied/cover-letter/request",
            json={"job_id": job_id, "requested": True},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn(f"cover letter requested=on job {job_id}", self.rebuild_calls)

        response = self.client.post(
            "/api/applied/cover-letter",
            json={"job_id": job_id, "text": "Dear team"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn(f"cover letter job {job_id}", self.rebuild_calls)

        response = self.client.post(
            "/api/applied/cover-letter/request",
            json={"job_id": job_id, "requested": False},
        )
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
