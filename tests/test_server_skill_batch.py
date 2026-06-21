"""API tests for skill block/delete batch endpoints."""

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


def _insert_job(db_path: str, link: str, *, company: str = "Acme", title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": company,
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


class ServerSkillBatchApiTest(unittest.TestCase):
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

    def test_block_batch_cleans_db_and_queues_one_rebuild(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="test",
        )
        upsert_skill_pattern(
            self.db_path,
            name="SQL",
            pattern=r"\bSQL\b",
            source="test",
        )
        job_python = _insert_job(self.db_path, "https://example.com/block-python", company="Acme", title="Python Dev")
        job_sql = _insert_job(self.db_path, "https://example.com/block-sql", company="Beta", title="SQL Dev")
        set_job_skills(self.db_path, job_python, ["Python"])
        set_job_skills(self.db_path, job_sql, ["SQL"])

        response = self.client.post(
            "/api/skill/block-batch",
            json={"skills": ["Python", "python", "SQL"]},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["count"], 2)
        self.assertEqual(sorted(body["skills"]), ["python", "sql"])
        self.assertEqual(body["db_deleted"]["skill_rows_deleted"], 2)
        self.assertEqual(body["db_deleted"]["job_skill_links_deleted"], 2)
        self.assertIn("python", self.runtime_profile.blocked_skills)
        self.assertIn("sql", self.runtime_profile.blocked_skills)
        self.assertEqual(get_job_skills(self.db_path, job_python), [])
        self.assertEqual(get_job_skills(self.db_path, job_sql), [])
        pattern_names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("Python", pattern_names)
        self.assertNotIn("SQL", pattern_names)
        self.assertEqual(self.rebuild_calls, ["skill block batch (2)"])

    def test_delete_batch_cleans_db_and_queues_one_rebuild(self):
        upsert_skill_pattern(
            self.db_path,
            name="Docker",
            pattern=r"\bDocker\b",
            source="test",
        )
        upsert_skill_pattern(
            self.db_path,
            name="AWS",
            pattern=r"\bAWS\b",
            source="test",
        )
        job_docker = _insert_job(self.db_path, "https://example.com/delete-docker", company="Gamma", title="Docker Dev")
        job_aws = _insert_job(self.db_path, "https://example.com/delete-aws", company="Delta", title="AWS Dev")
        set_job_skills(self.db_path, job_docker, ["Docker"])
        set_job_skills(self.db_path, job_aws, ["AWS"])

        response = self.client.post(
            "/api/skill/delete-batch",
            json={"skills": ["Docker", "AWS"]},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["count"], 2)
        self.assertEqual(sorted(body["skills"]), ["aws", "docker"])
        self.assertEqual(body["db_deleted"]["skill_rows_deleted"], 2)
        self.assertEqual(get_job_skills(self.db_path, job_docker), [])
        self.assertEqual(get_job_skills(self.db_path, job_aws), [])
        self.assertEqual(self.rebuild_calls, ["skill delete batch (2)"])

    def test_batch_requires_skills(self):
        block_response = self.client.post("/api/skill/block-batch", json={"skills": []})
        delete_response = self.client.post("/api/skill/delete-batch", json={"skills": ["   "]})

        self.assertEqual(block_response.status_code, 400)
        self.assertFalse(block_response.json()["ok"])
        self.assertEqual(delete_response.status_code, 400)
        self.assertFalse(delete_response.json()["ok"])


if __name__ == "__main__":
    unittest.main()
