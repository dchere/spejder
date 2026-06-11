"""API tests for interview stage endpoints."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_applied_jobs,
    get_interview_jobs,
    get_stopped_interview_jobs,
    set_job_applied,
    set_job_company_feedback,
    set_job_interview_stopped,
    set_job_on_interview,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.server import create_app


def _insert_job(db_path: str, link: str, *, applied: bool = False, title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
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


class ServerInterviewApiTest(unittest.TestCase):
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

    def _interview_row(self, job_id: int):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT applied, on_interview, interview_stopped, company_feedback FROM jobs WHERE id=?",
                (job_id,),
            )
            return cur.fetchone()
        finally:
            conn.close()

    def test_interview_requires_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/pending", applied=False)
        response = self.client.post("/api/interview", json={"job_id": job_id, "on_interview": True})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])

        applied_id = _insert_job(
            self.db_path, "https://example.com/applied", applied=True, title="Applied Engineer"
        )
        response = self.client.post("/api/interview", json={"job_id": applied_id, "on_interview": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertTrue(response.json()["on_interview"])
        self.assertIn(f"interview on job {applied_id}", self.rebuild_calls)

    def test_interview_stopped_requires_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/stopped-pending", applied=False)
        response = self.client.post("/api/interview/stopped", json={"job_id": job_id, "stopped": True})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])

        applied_id = _insert_job(
            self.db_path,
            "https://example.com/stopped-applied",
            applied=True,
            title="Stopped Applied Engineer",
        )
        response = self.client.post("/api/interview/stopped", json={"job_id": applied_id, "stopped": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertTrue(response.json()["stopped"])
        self.assertIn(f"interview stopped on job {applied_id}", self.rebuild_calls)

    def test_interview_feedback_requires_stopped(self):
        job_id = _insert_job(self.db_path, "https://example.com/feedback-pending", applied=True)
        response = self.client.post(
            "/api/interview/feedback",
            json={"job_id": job_id, "feedback": "No response"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])

        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        response = self.client.post(
            "/api/interview/feedback",
            json={"job_id": job_id, "feedback": "No response"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertEqual(response.json()["feedback"], "No response")
        self.assertIn(f"company feedback job {job_id}", self.rebuild_calls)

    def test_viewed_false_clears_interview_via_api(self):
        job_id = _insert_job(self.db_path, "https://example.com/viewed-clear-api", applied=True)
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "Ghosted"))

        response = self.client.post("/api/viewed", json={"job_id": job_id, "viewed": False})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn(f"job {job_id} marked viewed", self.rebuild_calls)

        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual((applied, on_interview, interview_stopped, company_feedback), (0, 0, 0, None))

        response = self.client.post("/api/applied", json={"job_id": job_id, "applied": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        applied_ids = {row["id"] for row in get_applied_jobs(self.db_path, limit=0)}
        interview_ids = {row["id"] for row in get_interview_jobs(self.db_path, limit=0)}
        stopped_ids = {row["id"] for row in get_stopped_interview_jobs(self.db_path, limit=0)}
        self.assertEqual(applied_ids, {job_id})
        self.assertEqual(interview_ids, set())
        self.assertEqual(stopped_ids, set())

    def test_not_relevant_feedback_clears_interview_via_api(self):
        job_id = _insert_job(self.db_path, "https://example.com/feedback-clear-api", applied=True)
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "Rejected"))

        response = self.client.post("/api/feedback", json={"job_id": job_id, "signal": "not relevant"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        self.assertIn(f"feedback not relevant on job {job_id}", self.rebuild_calls)

        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual((applied, on_interview, interview_stopped, company_feedback), (0, 0, 0, None))

        response = self.client.post("/api/applied", json={"job_id": job_id, "applied": True})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        applied_ids = {row["id"] for row in get_applied_jobs(self.db_path, limit=0)}
        interview_ids = {row["id"] for row in get_interview_jobs(self.db_path, limit=0)}
        stopped_ids = {row["id"] for row in get_stopped_interview_jobs(self.db_path, limit=0)}
        self.assertEqual(applied_ids, {job_id})
        self.assertEqual(interview_ids, set())
        self.assertEqual(stopped_ids, set())


if __name__ == "__main__":
    unittest.main()
