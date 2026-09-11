"""API tests for skill unwanted / have / learn exclusivity."""

import os
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.managers.profile_manager import _toggle_exclusive_profile_skill
from spejder.server import create_app


def create_test_app(db_path: str, report_dir: str, runtime_profile: AppConfig, profile_path: str):
    rebuild_calls = []

    def queue_dashboard_rebuild(reason: str):
        rebuild_calls.append(reason)

    def persist_runtime_profile():
        runtime_profile.save(profile_path)

    def reload_runtime_profile():
        fresh = AppConfig.load(profile_path)
        for key, value in fresh.model_dump().items():
            setattr(runtime_profile, key, value)

    app = create_app(
        db_path=db_path,
        profile_path=profile_path,
        runtime_profile=runtime_profile,
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=persist_runtime_profile,
        reload_runtime_profile=reload_runtime_profile,
        queue_dashboard_rebuild=queue_dashboard_rebuild,
        cli_verbose=False,
    )
    return app, rebuild_calls


class ExclusiveProfileSkillHelperTest(unittest.TestCase):
    def test_enable_unwanted_drops_have_and_learn(self):
        profile = AppConfig(
            user_skills=["Python"],
            missing_skills_suggestions=["Python"],
        )
        changed, dropped = _toggle_exclusive_profile_skill(
            profile,
            "unwanted_skills",
            "Python",
            True,
            drop_from=("user_skills", "missing_skills_suggestions"),
        )
        self.assertTrue(changed)
        self.assertTrue(dropped)
        self.assertEqual(profile.unwanted_skills, ["Python"])
        self.assertEqual(profile.user_skills, [])
        self.assertEqual(profile.missing_skills_suggestions, [])

    def test_enable_have_drops_unwanted_keeps_learn(self):
        profile = AppConfig()
        profile.unwanted_skills = ["Python"]
        profile.missing_skills_suggestions = ["Python"]
        changed, dropped = _toggle_exclusive_profile_skill(
            profile,
            "user_skills",
            "Python",
            True,
            drop_from=("unwanted_skills",),
        )
        self.assertTrue(changed)
        self.assertTrue(dropped)
        self.assertEqual(profile.user_skills, ["Python"])
        self.assertEqual(profile.unwanted_skills, [])
        self.assertEqual(profile.missing_skills_suggestions, ["Python"])


class UnwantedSkillsPersistenceTest(unittest.TestCase):
    def test_load_missing_key_defaults_empty_and_penalty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "profile.json")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write('{"min_score": 3.0}')
            profile = AppConfig.load(path)
        self.assertEqual(profile.unwanted_skills, [])
        self.assertEqual(profile.skill_unwanted_penalty, 1.2)
        self.assertEqual(profile.min_score, 3.0)

    def test_sanitizer_unwanted_wins_on_construct(self):
        profile = AppConfig(
            user_skills=["Python", "Rust"],
            missing_skills_suggestions=["python", "Docker"],
            unwanted_skills=["python"],
        )
        self.assertEqual(profile.unwanted_skills, ["python"])
        self.assertEqual(profile.user_skills, ["Rust"])
        self.assertEqual(profile.missing_skills_suggestions, ["Docker"])


class ServerSkillUnwantedApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.profile_path = os.path.join(self.report_dir, "profile.json")
        self.runtime_profile = AppConfig(
            user_skills=["Python"],
            missing_skills_suggestions=["Python"],
        )
        self.runtime_profile.save(self.profile_path)
        self.app, self.rebuild_calls = create_test_app(
            self.db_path, self.report_dir, self.runtime_profile, self.profile_path
        )
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_unwanted_requires_skill(self):
        response = self.client.post("/api/skill/unwanted", json={"skill": "  ", "unwanted": True})
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])

    @patch("spejder.server.rescore_active_jobs", return_value=0)
    def test_enable_unwanted_drops_have_and_learn_and_rescores(self, mock_rescore):
        response = self.client.post(
            "/api/skill/unwanted",
            json={"skill": "Python", "unwanted": True},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["changed"])
        self.assertEqual(body["unwanted"], True)
        self.assertEqual(self.runtime_profile.unwanted_skills, ["python"])
        self.assertEqual(self.runtime_profile.user_skills, [])
        self.assertEqual(self.runtime_profile.missing_skills_suggestions, [])
        mock_rescore.assert_called_once()
        self.assertTrue(any("unwanted on python" in reason for reason in self.rebuild_calls))

    @patch("spejder.server.rescore_active_jobs", return_value=0)
    def test_enable_have_drops_unwanted_and_rescores(self, mock_rescore):
        self.runtime_profile.user_skills = []
        self.runtime_profile.unwanted_skills = ["python"]
        self.runtime_profile.missing_skills_suggestions = ["python"]
        response = self.client.post(
            "/api/skill/user",
            json={"skill": "Python", "has_skill": True},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["changed"])
        self.assertEqual(self.runtime_profile.user_skills, ["python"])
        self.assertEqual(self.runtime_profile.unwanted_skills, [])
        self.assertEqual(self.runtime_profile.missing_skills_suggestions, ["python"])
        mock_rescore.assert_called_once()

    @patch("spejder.server.rescore_active_jobs", return_value=0)
    def test_enable_learn_drops_unwanted_and_rescores(self, mock_rescore):
        self.runtime_profile.user_skills = []
        self.runtime_profile.unwanted_skills = ["python"]
        self.runtime_profile.missing_skills_suggestions = []
        response = self.client.post(
            "/api/skill/learn",
            json={"skill": "Python", "learn": True},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["changed"])
        self.assertEqual(self.runtime_profile.missing_skills_suggestions, ["python"])
        self.assertEqual(self.runtime_profile.unwanted_skills, [])
        mock_rescore.assert_called_once()

    @patch("spejder.server.rescore_active_jobs", return_value=0)
    def test_enable_learn_without_unwanted_does_not_rescore(self, mock_rescore):
        self.runtime_profile.user_skills = []
        self.runtime_profile.unwanted_skills = []
        self.runtime_profile.missing_skills_suggestions = []
        response = self.client.post(
            "/api/skill/learn",
            json={"skill": "Python", "learn": True},
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["changed"])
        self.assertEqual(self.runtime_profile.missing_skills_suggestions, ["python"])
        mock_rescore.assert_not_called()
        self.assertTrue(any("skill learn on python" in reason for reason in self.rebuild_calls))

    @patch("spejder.server.rescore_active_jobs", return_value=0)
    def test_disable_unwanted_removes_and_rescores(self, mock_rescore):
        self.runtime_profile.user_skills = []
        self.runtime_profile.missing_skills_suggestions = []
        self.runtime_profile.unwanted_skills = ["python"]
        self.runtime_profile.save(self.profile_path)
        self.assertEqual(AppConfig.load(self.profile_path).unwanted_skills, ["python"])
        response = self.client.post(
            "/api/skill/unwanted",
            json={"skill": "Python", "unwanted": False},
        )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["ok"])
        self.assertTrue(body["changed"])
        self.assertEqual(body["unwanted"], False)
        self.assertEqual(self.runtime_profile.unwanted_skills, [])
        mock_rescore.assert_called_once()
        self.assertTrue(any("unwanted off python" in reason for reason in self.rebuild_calls))
        reloaded = AppConfig.load(self.profile_path)
        self.assertEqual(reloaded.unwanted_skills, [])


if __name__ == "__main__":
    unittest.main()
