"""API tests for manual report rebuild."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.server import create_app


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


class ServerReportRebuildApiTest(unittest.TestCase):
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

    def test_report_rebuild_queues_dashboard_rebuild(self):
        response = self.client.post("/api/report/rebuild")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "queued": True})
        self.assertIn("manual rebuild", self.rebuild_calls)
