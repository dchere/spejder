"""API tests for manual inbox sync."""

import os
import tempfile
import unittest

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.server import create_app


def create_test_app(db_path: str, report_dir: str, *, running: bool = False):
    state = {
        "running": running,
        "stage_id": "ingest",
        "stage_message": "Ingesting inbox files",
        "status": "running" if running else "idle",
        "message": "Ingesting inbox files" if running else "",
    }

    def trigger_inbox_sync():
        if state["running"]:
            return {"ok": False, "error": "sync already running"}
        state["running"] = True
        state["status"] = "running"
        state["stage_id"] = "start"
        state["stage_message"] = "Sync started"
        state["message"] = "Sync started"
        return {"ok": True, "started": True}

    def get_inbox_sync_status():
        return {
            "running": state["running"],
            "stage_id": state["stage_id"],
            "stage_message": state["stage_message"],
            "status": state["status"],
            "message": state["message"],
        }

    app = create_app(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=AppConfig(),
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=lambda reason="": None,
        cli_verbose=False,
        trigger_inbox_sync=trigger_inbox_sync,
        get_inbox_sync_status=get_inbox_sync_status,
    )
    return app, state


class ServerInboxSyncApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.app, self.state = create_test_app(self.db_path, self.report_dir)
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_inbox_sync_starts_and_reports_status(self):
        response = self.client.post("/api/inbox/sync")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True, "started": True})
        self.assertTrue(self.state["running"])

        status = self.client.get("/api/inbox/sync/status")
        self.assertEqual(status.status_code, 200)
        payload = status.json()
        self.assertTrue(payload["running"])
        self.assertEqual(payload["status"], "running")
        self.assertEqual(payload["stage_id"], "start")
        self.assertEqual(payload["stage_message"], "Sync started")

    def test_inbox_sync_rejects_when_already_running(self):
        self.client.post("/api/inbox/sync")
        response = self.client.post("/api/inbox/sync")
        self.assertEqual(response.status_code, 409)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["error"], "sync already running")

    def test_inbox_sync_status_idle_when_not_configured(self):
        app = create_app(
            db_path=self.db_path,
            profile_path=os.path.join(self.report_dir, "profile.json"),
            runtime_profile=AppConfig(),
            model_path="",
            report_dir=self.report_dir,
            get_title_translation_llm=lambda: None,
            persist_runtime_profile=lambda: None,
            reload_runtime_profile=lambda: None,
            queue_dashboard_rebuild=lambda reason="": None,
            cli_verbose=False,
        )
        client = TestClient(app)
        response = client.get("/api/inbox/sync/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "running": False,
                "stage_id": "",
                "stage_message": "",
                "status": "idle",
                "message": "",
            },
        )

    def test_inbox_sync_unavailable_without_runner(self):
        app = create_app(
            db_path=self.db_path,
            profile_path=os.path.join(self.report_dir, "profile.json"),
            runtime_profile=AppConfig(),
            model_path="",
            report_dir=self.report_dir,
            get_title_translation_llm=lambda: None,
            persist_runtime_profile=lambda: None,
            reload_runtime_profile=lambda: None,
            queue_dashboard_rebuild=lambda reason="": None,
            cli_verbose=False,
        )
        client = TestClient(app)
        response = client.post("/api/inbox/sync")
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["error"], "inbox sync not available")


if __name__ == "__main__":
    unittest.main()
