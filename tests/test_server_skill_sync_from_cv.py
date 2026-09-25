"""API tests for Skills tab sync-from-CV endpoint."""

import os
import tempfile
import unittest
from typing import Optional
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.server import create_app


def create_test_app(
    db_path: str,
    report_dir: str,
    runtime_profile: AppConfig,
    model_path: str = "",
    profile_path: Optional[str] = None,
    reload_runtime_profile=None,
    queue_dashboard_rebuild=None,
):
    rebuilds = []

    def _queue(reason=""):
        rebuilds.append(reason)

    app = create_app(
        db_path=db_path,
        profile_path=profile_path or os.path.join(report_dir, "profile.json"),
        runtime_profile=runtime_profile,
        model_path=model_path,
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=reload_runtime_profile or (lambda: None),
        queue_dashboard_rebuild=queue_dashboard_rebuild or _queue,
        cli_verbose=False,
    )
    app.state.test_rebuilds = rebuilds
    return app


class ServerSkillSyncFromCvTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.cv_path = os.path.join(self._tmpdir.name, "CV.txt")
        with open(self.cv_path, "w", encoding="utf-8") as handle:
            handle.write("Experience with Python and Kubernetes.")
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        self.runtime_profile = AppConfig(
            user_skills=["python"],
            default_cv_path=self.cv_path,
            default_db=self.db_path,
        )
        self.runtime_profile.save(self.profile_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_sync_from_cv_requires_model(self):
        app = create_test_app(
            self.db_path,
            self.report_dir,
            self.runtime_profile,
            profile_path=self.profile_path,
        )
        client = TestClient(app)
        response = client.post("/api/skill/sync-from-cv")
        self.assertEqual(response.status_code, 503)
        self.assertIn("default_model", response.json()["error"])

    @patch("spejder.server.routers.skills.LocalLLM")
    @patch("spejder.server.routers.skills.sync_user_skills")
    @patch("spejder.server.routers.skills.rescore_active_jobs", return_value=3)
    def test_sync_from_cv_success(self, mock_rescore, mock_sync, mock_llm):
        mock_llm.return_value = MagicMock()
        mock_sync.return_value = {
            "ok": True,
            "extracted": 2,
            "total_user_skills": 3,
            "top_extracted": ["Kubernetes", "Docker"],
        }
        reloads = []

        def reload_profile():
            reloads.append(True)
            self.runtime_profile.user_skills = ["python", "Kubernetes", "Docker"]

        app = create_test_app(
            self.db_path,
            self.report_dir,
            self.runtime_profile,
            model_path="/fake/model.gguf",
            profile_path=self.profile_path,
            reload_runtime_profile=reload_profile,
        )
        client = TestClient(app)
        response = client.post("/api/skill/sync-from-cv")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["extracted"], 2)
        self.assertEqual(payload["total_user_skills"], 3)
        self.assertEqual(payload["top_extracted"], ["Kubernetes", "Docker"])
        self.assertEqual(payload["rescored"], 3)
        self.assertEqual(reloads, [True])
        self.assertEqual(app.state.test_rebuilds, ["skills synced from CV"])
        mock_sync.assert_called_once()
        call_kwargs = mock_sync.call_args.kwargs
        self.assertEqual(call_kwargs["profile"], self.profile_path)
        self.assertEqual(call_kwargs["db"], self.db_path)
        self.assertEqual(call_kwargs["cv"], self.cv_path)
        mock_rescore.assert_called_once()

    @patch("spejder.server.routers.skills.LocalLLM")
    @patch("spejder.server.routers.skills.sync_user_skills")
    def test_sync_from_cv_empty_cv(self, mock_sync, mock_llm):
        mock_llm.return_value = MagicMock()
        mock_sync.return_value = {
            "ok": False,
            "error": "CV not found or empty: /missing",
            "extracted": 0,
            "total_user_skills": 0,
        }
        app = create_test_app(
            self.db_path,
            self.report_dir,
            self.runtime_profile,
            model_path="/fake/model.gguf",
            profile_path=self.profile_path,
        )
        client = TestClient(app)
        response = client.post("/api/skill/sync-from-cv")
        self.assertEqual(response.status_code, 400)
        self.assertFalse(response.json()["ok"])
        self.assertIn("CV not found", response.json()["error"])


if __name__ == "__main__":
    unittest.main()
