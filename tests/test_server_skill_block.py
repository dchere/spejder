"""API tests for skill block endpoint."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_job_skills,
    get_skill_patterns,
    set_job_skills,
    upsert_job,
    upsert_skill_pattern,
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


def create_test_app(db_path: str, report_dir: str, runtime_profile: AppConfig):
    rebuild_calls = []

    def queue_dashboard_rebuild(reason: str):
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


class ServerSkillBlockApiTest(unittest.TestCase):
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

    def test_block_skill_cleans_db_before_rebuild(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="test",
        )
        job_id = _insert_job(self.db_path, "https://example.com/block-skill")
        set_job_skills(self.db_path, job_id, ["Python"])

        response = self.client.post("/api/skill/block", json={"skill": "Python"})

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["skill"], "python")
        self.assertEqual(body["db_deleted"]["skill_rows_deleted"], 1)
        self.assertEqual(body["db_deleted"]["job_skill_links_deleted"], 1)
        self.assertIn("python", self.runtime_profile.blocked_skills)
        self.assertEqual(get_job_skills(self.db_path, job_id), [])
        pattern_names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("Python", pattern_names)
        self.assertIn("skill blocked python", self.rebuild_calls)

    def test_block_skill_requires_skill(self):
        response = self.client.post("/api/skill/block", json={"skill": "   "})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
