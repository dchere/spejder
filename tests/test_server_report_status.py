"""API tests for report rebuild status."""

import os
import re
import tempfile
import time
import unittest
from email.utils import formatdate

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.managers.dashboard_manager import (
    _SPEJDER_REPORT_MTIME_PLACEHOLDER,
    _render_html_dashboard,
)
from spejder.server import create_app


def create_test_app(db_path: str, report_dir: str, *, idle: bool = True):
    app = create_app(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=AppConfig(),
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=lambda reason: None,
        cli_verbose=False,
        get_report_rebuild_idle=lambda: idle,
    )
    return app


class ServerReportStatusApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.report_path = os.path.join(self.report_dir, "report.html")
        with open(self.report_path, "w", encoding="utf-8") as f:
            f.write("<html></html>")
        time.sleep(0.01)
        self.expected_mtime = formatdate(os.path.getmtime(self.report_path), usegmt=True)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_report_status_returns_idle_and_mtime(self):
        app = create_test_app(self.db_path, self.report_dir, idle=True)
        client = TestClient(app)

        response = client.get("/api/report/status")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["ok"])
        self.assertTrue(data["idle"])
        self.assertEqual(data["last_modified"], self.expected_mtime)

    def test_report_status_reflects_rebuild_busy(self):
        app = create_test_app(self.db_path, self.report_dir, idle=False)
        client = TestClient(app)

        response = client.get("/api/report/status")

        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data["ok"])
        self.assertFalse(data["idle"])

    def test_report_status_empty_mtime_when_missing_file(self):
        os.remove(self.report_path)
        app = create_test_app(self.db_path, self.report_dir, idle=True)
        client = TestClient(app)

        response = client.get("/api/report/status")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["last_modified"], "")

    def test_embedded_report_mtime_matches_api_last_modified(self):
        report_path = os.path.join(self.report_dir, "report.html")
        _render_html_dashboard([], [], [], report_path, "Test report")

        app = create_test_app(self.db_path, self.report_dir, idle=True)
        client = TestClient(app)
        response = client.get("/api/report/status")
        self.assertEqual(response.status_code, 200)
        api_mtime = response.json()["last_modified"]
        self.assertTrue(api_mtime)

        with open(report_path, encoding="utf-8") as f:
            html = f.read()
        self.assertNotIn(_SPEJDER_REPORT_MTIME_PLACEHOLDER, html)
        match = re.search(
            r'<meta name="spejder-report-mtime" content="([^"]+)"',
            html,
        )
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), api_mtime)
        self.assertEqual(
            match.group(1),
            formatdate(os.path.getmtime(report_path), usegmt=True),
        )


if __name__ == "__main__":
    unittest.main()
