"""API tests for skill forgive endpoint."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db, get_bad_ngram_weights
from spejder.extractors.skill_extractor.bad_cloud import ingest_blocked_skills
from spejder.server import create_app


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


class SkillForgiveApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = self._tmpdir.name
        ensure_db(self.db_path)
        self.profile = AppConfig(
            blocked_skills=["sales crm", "python"],
            skill_bad_ngram_weight_cap=5,
        )
        ingest_blocked_skills(["sales crm", "sales crm"], self.db_path, max_weight=5)
        self.app, self.rebuild_calls = create_test_app(
            self.db_path, self.report_dir, self.profile
        )
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_forgive_decrements_cloud_and_unblocks(self):
        before = get_bad_ngram_weights(self.db_path, [("sales crm", 2)])[("sales crm", 2)]
        response = self.client.post("/api/skill/forgive", json={"skill": "sales crm"})
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["ok"])
        after = get_bad_ngram_weights(self.db_path, [("sales crm", 2)]).get(("sales crm", 2), 0)
        self.assertEqual(after, before - 1)
        self.assertEqual(
            [s.lower() for s in self.profile.blocked_skills],
            ["python"],
        )
        self.assertTrue(any("forgiven" in reason for reason in self.rebuild_calls))

    def test_forgive_requires_skill(self):
        response = self.client.post("/api/skill/forgive", json={"skill": "  "})
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
