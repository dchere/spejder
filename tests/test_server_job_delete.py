"""API tests for deleting a position."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db, get_job_skills, set_job_skills, upsert_job
from spejder.db.connection import _connect
from spejder.server import create_app


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


def _count(db_path: str, sql: str, params: tuple) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return int(cur.fetchone()[0])
    finally:
        conn.close()


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


class ServerJobDeleteApiTest(unittest.TestCase):
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

    def test_api_job_delete_removes_row_and_skills(self):
        job_id = _insert_job(self.db_path, "https://example.com/api-delete")
        set_job_skills(self.db_path, job_id, ["Python"])
        self.assertEqual(get_job_skills(self.db_path, job_id), ["Python"])

        response = self.client.post("/api/job/delete", json={"job_id": job_id})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["job_id"], job_id)
        self.assertEqual(_count(self.db_path, "SELECT COUNT(*) FROM jobs WHERE id=?", (job_id,)), 0)
        self.assertEqual(
            _count(self.db_path, "SELECT COUNT(*) FROM job_skills WHERE job_id=?", (job_id,)),
            0,
        )
        self.assertEqual(get_job_skills(self.db_path, job_id), [])
        self.assertIn(f"job {job_id} deleted", self.rebuild_calls)

    def test_api_job_delete_unknown_id_is_404(self):
        response = self.client.post("/api/job/delete", json={"job_id": 999999})
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(self.rebuild_calls, [])

    def test_api_job_delete_non_positive_id_is_400(self):
        for job_id in (0, -1):
            with self.subTest(job_id=job_id):
                response = self.client.post("/api/job/delete", json={"job_id": job_id})
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
        self.assertEqual(self.rebuild_calls, [])


if __name__ == "__main__":
    unittest.main()
