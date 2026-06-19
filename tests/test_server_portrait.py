"""API tests for portrait endpoints."""

import os
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_applied, upsert_job
from spejder.db.connection import _connect
from spejder.server import create_app


def _insert_applied_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Engineer",
            "position_link": link,
            "raw_text": "Python role",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    set_job_applied(db_path, job_id, True)
    return job_id


def create_test_app(db_path: str, report_dir: str, runtime_profile: AppConfig, model_path: str = ""):
    app = create_app(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=runtime_profile,
        model_path=model_path,
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=lambda reason="": None,
        cli_verbose=False,
    )
    return app


class ServerPortraitApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        self.portrait_path = os.path.join(self._tmpdir.name, "portrait.txt")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.runtime_profile = AppConfig(
            user_skills=["python"],
            default_portrait_path=self.portrait_path,
            default_cv_path=os.path.join(self._tmpdir.name, "missing-cv"),
        )
        self.app = create_test_app(self.db_path, self.report_dir, self.runtime_profile)
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_portrait_get_empty(self):
        response = self.client.get("/api/portrait")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["text"], "")
        self.assertNotIn("path", payload)

    def test_portrait_save_and_get(self):
        response = self.client.post("/api/portrait/save", json={"text": "Committed portrait"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["ok"])
        with open(self.portrait_path, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "Committed portrait")
        response = self.client.get("/api/portrait")
        self.assertEqual(response.json()["text"], "Committed portrait")

    def test_portrait_generate_requires_model(self):
        _insert_applied_job(self.db_path, "https://example.com/p1")
        response = self.client.post("/api/portrait/generate")
        self.assertEqual(response.status_code, 503)

    def test_portrait_generate_requires_context(self):
        empty_profile = AppConfig(default_portrait_path=self.portrait_path)
        app = create_test_app(self.db_path, self.report_dir, empty_profile, model_path="/fake/model.gguf")
        client = TestClient(app)
        response = client.post("/api/portrait/generate")
        self.assertEqual(response.status_code, 400)

    @patch("spejder.server.generate_portrait_draft")
    def test_portrait_generate_returns_draft_and_diff(self, mock_generate):
        mock_generate.return_value = "Updated portrait line"
        with open(self.portrait_path, "w", encoding="utf-8") as handle:
            handle.write("Old portrait line")
        _insert_applied_job(self.db_path, "https://example.com/p2")
        app = create_test_app(
            self.db_path,
            self.report_dir,
            self.runtime_profile,
            model_path="/fake/model.gguf",
        )
        client = TestClient(app)
        response = client.post("/api/portrait/generate")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["draft"], "Updated portrait line")
        self.assertEqual(payload["committed"], "Old portrait line")
        self.assertIn("diff-line", payload["diff_html"])
        with open(self.portrait_path, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "Old portrait line")

    @patch("spejder.server.generate_portrait_draft")
    def test_portrait_generate_returns_generic_error_on_failure(self, mock_generate):
        mock_generate.side_effect = RuntimeError("model exploded")
        _insert_applied_job(self.db_path, "https://example.com/p3")
        app = create_test_app(
            self.db_path,
            self.report_dir,
            self.runtime_profile,
            model_path="/fake/model.gguf",
        )
        client = TestClient(app)
        response = client.post("/api/portrait/generate")
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["error"], "portrait generation failed")
