"""API tests for soft-hide Hidden endpoint."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_jobs_by_category,
    set_job_applied,
    set_job_feedback,
    set_job_hidden,
    set_job_viewed,
    upsert_job,
)
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


class ServerHiddenApiTest(unittest.TestCase):
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

    def test_api_hidden_soft_hides_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/api-hidden")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))

        response = self.client.post("/api/hidden", json={"job_id": job_id, "hidden": True})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["hidden"])
        self.assertIn(f"job {job_id} marked hidden", self.rebuild_calls)

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT hidden, applied, viewed FROM jobs WHERE id=?", (job_id,))
            hidden, applied, viewed = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(hidden, 1)
        self.assertEqual(applied, 1)
        self.assertEqual(viewed, 1)

    def test_api_hidden_soft_hides_non_applied_preserves_flags(self):
        job_id = _insert_job(self.db_path, "https://example.com/api-soft")
        self.assertTrue(set_job_feedback(self.db_path, job_id, "relevant"))
        self.assertTrue(set_job_viewed(self.db_path, job_id, True))

        response = self.client.post("/api/hidden", json={"job_id": job_id, "hidden": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT hidden, applied, viewed, category FROM jobs WHERE id=?",
                (job_id,),
            )
            hidden, applied, viewed, category = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(hidden, 1)
        self.assertEqual(applied, 0)
        self.assertEqual(viewed, 1)
        self.assertEqual(category, "relevant")
        rows = get_jobs_by_category(
            self.db_path, "relevant", limit=0, exclude_hidden=False, unviewed_only=False
        )
        self.assertIn(job_id, {row["id"] for row in rows})

    def test_api_hidden_false(self):
        job_id = _insert_job(self.db_path, "https://example.com/api-unhide")
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))

        response = self.client.post("/api/hidden", json={"job_id": job_id, "hidden": False})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertFalse(response.json()["hidden"])
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT hidden FROM jobs WHERE id=?", (job_id,))
            self.assertEqual(cur.fetchone()[0], 0)
        finally:
            conn.close()
        self.assertIn(f"job {job_id} unhidden", self.rebuild_calls)


if __name__ == "__main__":
    unittest.main()
