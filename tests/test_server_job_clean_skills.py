"""API tests for clearing one job's extracted skills."""

import os
import tempfile
import time
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_job_skills,
    set_job_applied,
    set_job_feedback,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_skills,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.server import create_app


def _insert_job(db_path: str, link: str, title: str = "Engineer") -> int:
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
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _count(db_path: str, sql: str, params: tuple = ()) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _job_flags(db_path: str, job_id: int) -> dict:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT viewed, applied, on_interview, interview_stopped,
                   relevance_reason, category, relevant, applied_at
            FROM jobs WHERE id=?
            """,
            (job_id,),
        )
        row = cur.fetchone()
        keys = (
            "viewed",
            "applied",
            "on_interview",
            "interview_stopped",
            "relevance_reason",
            "category",
            "relevant",
            "applied_at",
        )
        return dict(zip(keys, row))
    finally:
        conn.close()


def _set_reason(db_path: str, job_id: int, reason: str) -> None:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET relevance_reason=? WHERE id=?",
            (reason, job_id),
        )
        conn.commit()
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


class ServerJobCleanSkillsApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.app, self.rebuild_calls = create_test_app(self.db_path, self.report_dir)
        self.client = TestClient(self.app)
        # Default: do not start a real rematerialize worker against a temp DB.
        self._ensure_patcher = patch.object(
            self.app.state.runtime.skills_rematerialize,
            "ensure",
            return_value="started",
        )
        self._ensure_patcher.start()

    def tearDown(self):
        self._ensure_patcher.stop()
        self._tmpdir.cleanup()

    def test_api_clean_skills_unviews_non_applied_and_clears_manual_reason(self):
        job_id = _insert_job(self.db_path, "https://example.com/clean-viewed")
        other_id = _insert_job(self.db_path, "https://example.com/clean-other", title="Other")
        set_job_skills(self.db_path, job_id, ["Python"])
        set_job_skills(self.db_path, other_id, ["Python"])
        set_job_viewed(self.db_path, job_id, True)
        set_job_feedback(self.db_path, job_id, "not relevant")
        patterns_before = _count(self.db_path, "SELECT COUNT(*) FROM skill_patterns")
        self.assertGreater(patterns_before, 0)

        response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["job_id"], job_id)
        self.assertTrue(body["viewed_cleared"])
        self.assertEqual(get_job_skills(self.db_path, job_id), [])
        self.assertEqual(get_job_skills(self.db_path, other_id), ["Python"])
        self.assertEqual(
            _count(self.db_path, "SELECT COUNT(*) FROM skill_patterns"),
            patterns_before,
        )
        self.assertEqual(_count(self.db_path, "SELECT COUNT(*) FROM jobs WHERE id=?", (job_id,)), 1)
        flags = _job_flags(self.db_path, job_id)
        self.assertEqual(flags["viewed"], 0)
        self.assertEqual(flags["applied"], 0)
        self.assertEqual(flags["on_interview"], 0)
        self.assertEqual(flags["interview_stopped"], 0)
        self.assertEqual(flags["relevance_reason"], "")
        self.assertEqual(flags["category"], "not relevant")
        self.assertEqual(flags["relevant"], 0)
        self.assertIn(f"job {job_id} skills cleaned", self.rebuild_calls)

    def test_api_clean_skills_leaves_non_manual_reason(self):
        job_id = _insert_job(self.db_path, "https://example.com/clean-scored")
        set_job_skills(self.db_path, job_id, ["Go"])
        set_job_viewed(self.db_path, job_id, True)
        set_job_feedback(self.db_path, job_id, "relevant")
        _set_reason(self.db_path, job_id, "keyword=python")

        response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["viewed_cleared"])
        flags = _job_flags(self.db_path, job_id)
        self.assertEqual(flags["viewed"], 0)
        self.assertEqual(flags["relevance_reason"], "keyword=python")
        self.assertEqual(flags["category"], "relevant")
        self.assertEqual(flags["relevant"], 1)
        self.assertEqual(get_job_skills(self.db_path, job_id), [])

    def test_api_clean_skills_pipeline_jobs_stay_viewed(self):
        cases = (
            ("applied", None),
            ("interview", "on_interview"),
            ("stopped", "stopped"),
        )
        for name, stage in cases:
            with self.subTest(stage=name):
                job_id = _insert_job(
                    self.db_path,
                    f"https://example.com/clean-{name}",
                    title=f"Engineer {name}",
                )
                set_job_applied(self.db_path, job_id, True)
                if stage == "on_interview":
                    self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
                elif stage == "stopped":
                    self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
                set_job_skills(self.db_path, job_id, ["Python"])
                before = _job_flags(self.db_path, job_id)

                response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
                self.assertEqual(response.status_code, 200)
                body = response.json()
                self.assertTrue(body["ok"])
                self.assertFalse(body["viewed_cleared"])
                self.assertEqual(get_job_skills(self.db_path, job_id), [])
                flags = _job_flags(self.db_path, job_id)
                self.assertEqual(flags["viewed"], 1)
                self.assertEqual(flags["applied"], 1)
                self.assertEqual(flags["on_interview"], before["on_interview"])
                self.assertEqual(flags["interview_stopped"], before["interview_stopped"])
                self.assertEqual(flags["applied_at"], before["applied_at"])
                self.assertTrue(flags["applied_at"])
                self.assertEqual(flags["relevance_reason"], "")
                self.assertEqual(flags["category"], before["category"])
                self.assertEqual(flags["relevant"], before["relevant"])

    def test_api_clean_skills_unknown_id_is_404(self):
        response = self.client.post("/api/job/clean-skills", json={"job_id": 999999})
        self.assertEqual(response.status_code, 404)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(self.rebuild_calls, [])

    def test_api_clean_skills_non_positive_id_is_400(self):
        for job_id in (0, -1):
            with self.subTest(job_id=job_id):
                response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
                self.assertEqual(response.status_code, 400)
                self.assertFalse(response.json()["ok"])
        self.assertEqual(self.rebuild_calls, [])

    def test_api_clean_skills_queues_rematerialize(self):
        job_id = _insert_job(self.db_path, "https://example.com/clean-remat")
        set_job_skills(self.db_path, job_id, ["Python"])
        ensure_calls = []

        def fake_ensure(jid):
            ensure_calls.append(int(jid))
            return "started"

        self._ensure_patcher.stop()
        runtime = self.app.state.runtime
        with patch.object(runtime.skills_rematerialize, "ensure", side_effect=fake_ensure):
            response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
        self._ensure_patcher = patch.object(
            runtime.skills_rematerialize, "ensure", return_value="started"
        )
        self._ensure_patcher.start()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(ensure_calls, [job_id])
        self.assertEqual(get_job_skills(self.db_path, job_id), [])
        self.assertIn(f"job {job_id} skills cleaned", self.rebuild_calls)

    def test_api_clean_skills_dedicated_worker_rebuilds(self):
        job_id = _insert_job(self.db_path, "https://example.com/clean-worker")
        set_job_skills(self.db_path, job_id, ["Go"])
        self._ensure_patcher.stop()
        with patch.object(
            self.app.state.runtime.skills_rematerialize,
            "_materialize_one",
            return_value=True,
        ) as mock_one:
            response = self.client.post("/api/job/clean-skills", json={"job_id": job_id})
            self.assertEqual(response.status_code, 200)
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if any("skills rematerialized" in reason for reason in self.rebuild_calls):
                    break
                time.sleep(0.02)
            self.assertTrue(mock_one.called)
        self._ensure_patcher = patch.object(
            self.app.state.runtime.skills_rematerialize,
            "ensure",
            return_value="started",
        )
        self._ensure_patcher.start()
        self.assertTrue(
            any("skills rematerialized" in reason for reason in self.rebuild_calls)
        )


if __name__ == "__main__":
    unittest.main()
