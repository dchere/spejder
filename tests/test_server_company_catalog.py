"""API tests for company catalog merge / rename / delete and company page title."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    ensure_companies_from_jobs,
    ensure_db,
    get_jobs_by_company,
    list_company_catalog_rows,
    resolve_company,
    upsert_job,
)
from spejder.db.companies import company_alias_key
from spejder.server import create_app


def _insert(db_path: str, link: str, company: str, title: str = "Engineer") -> None:
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


def _create_app(db_path: str, report_dir: str, rebuild: MagicMock):
    profile_path = os.path.join(os.path.dirname(db_path), "profile.json")
    runtime_profile = AppConfig()
    runtime_profile.save(profile_path)

    def _reload():
        return None

    return create_app(
        db_path=db_path,
        profile_path=profile_path,
        runtime_profile=runtime_profile,
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=_reload,
        queue_dashboard_rebuild=rebuild,
        cli_verbose=False,
    )


class ServerCompanyCatalogTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.rebuild = MagicMock()
        self.app = _create_app(self.db_path, self.report_dir, self.rebuild)
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _company_id(self, name: str) -> int:
        ensure_companies_from_jobs(self.db_path)
        for row in list_company_catalog_rows(self.db_path):
            if row["display_name"] == name:
                return int(row["id"])
        raise AssertionError(name)

    def test_merge_validation_and_success(self):
        _insert(self.db_path, "https://example.com/a", "Alpha")
        _insert(self.db_path, "https://example.com/b", "Beta")
        id_a = self._company_id("Alpha")
        id_b = self._company_id("Beta")

        bad = self.client.post(
            "/api/company/merge",
            json={"company_ids": [id_a], "display_name": "X"},
        )
        self.assertEqual(bad.status_code, 400)
        self.assertFalse(bad.json()["ok"])

        ok = self.client.post(
            "/api/company/merge",
            json={"company_ids": [id_a, id_b], "display_name": "Merged Co"},
        )
        self.assertEqual(ok.status_code, 200)
        payload = ok.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["display_name"], "Merged Co")
        self.rebuild.assert_called()

    def test_rename_and_delete(self):
        _insert(self.db_path, "https://example.com/a", "Rename Me")
        cid = self._company_id("Rename Me")

        renamed = self.client.post(
            "/api/company/rename",
            json={"company_id": cid, "display_name": "Renamed"},
        )
        self.assertEqual(renamed.status_code, 200)
        self.assertTrue(renamed.json()["ok"])

        deleted = self.client.post(
            "/api/company/delete",
            json={"company_id": cid},
        )
        self.assertEqual(deleted.status_code, 200)
        self.assertTrue(deleted.json()["ok"])
        self.assertFalse(deleted.json()["already_gone"])
        self.rebuild.assert_called()
        self.rebuild.reset_mock()

        gone = self.client.post(
            "/api/company/delete",
            json={"company_id": cid},
        )
        self.assertEqual(gone.status_code, 200)
        self.assertTrue(gone.json()["already_gone"])
        self.rebuild.assert_not_called()

    def test_delete_batch_rebuilds_once_when_any_deleted(self):
        _insert(self.db_path, "https://example.com/a", "BatchA")
        _insert(self.db_path, "https://example.com/b", "BatchB")
        id_a = self._company_id("BatchA")
        id_b = self._company_id("BatchB")

        empty = self.client.post("/api/company/delete-batch", json={"company_ids": []})
        self.assertEqual(empty.status_code, 400)
        self.assertFalse(empty.json()["ok"])
        self.rebuild.assert_not_called()

        ok = self.client.post(
            "/api/company/delete-batch",
            json={"company_ids": [id_a, id_b, 999_999]},
        )
        self.assertEqual(ok.status_code, 200)
        payload = ok.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["deleted_ids"], [id_a, id_b])
        self.assertEqual(payload["already_gone_ids"], [999_999])
        self.assertEqual(payload["count"], 2)
        self.rebuild.assert_called_once_with(reason="company delete batch")

    def test_delete_batch_all_gone_skips_rebuild(self):
        response = self.client.post(
            "/api/company/delete-batch",
            json={"company_ids": [111, 222]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["deleted_ids"], [])
        self.assertEqual(payload["already_gone_ids"], [111, 222])
        self.assertEqual(payload["count"], 0)
        self.rebuild.assert_not_called()

    def test_rename_collision_returns_400_and_leaves_jobs(self):
        _insert(self.db_path, "https://example.com/a", "AlphaCo")
        _insert(self.db_path, "https://example.com/b", "BetaCo")
        id_a = self._company_id("AlphaCo")
        id_b = self._company_id("BetaCo")

        bad = self.client.post(
            "/api/company/rename",
            json={"company_id": id_a, "display_name": "BetaCo"},
        )
        self.assertEqual(bad.status_code, 400)
        payload = bad.json()
        self.assertFalse(payload["ok"])
        self.assertIn("already used", payload["error"])
        self.rebuild.assert_not_called()

        self.assertEqual(resolve_company(self.db_path, "AlphaCo").id, id_a)
        self.assertEqual(resolve_company(self.db_path, "BetaCo").id, id_b)
        self.assertEqual(
            [job["company"] for job in get_jobs_by_company(self.db_path, "AlphaCo")],
            ["AlphaCo"],
        )
        self.assertEqual(
            [job["company"] for job in get_jobs_by_company(self.db_path, "BetaCo")],
            ["BetaCo"],
        )
        self.assertIn(
            company_alias_key("BetaCo"),
            resolve_company(self.db_path, "BetaCo").keys,
        )

    def test_merge_collision_returns_400_and_leaves_jobs(self):
        _insert(self.db_path, "https://example.com/a", "AlphaCo")
        _insert(self.db_path, "https://example.com/b", "BetaCo")
        _insert(self.db_path, "https://example.com/c", "GammaCo")
        id_a = self._company_id("AlphaCo")
        id_b = self._company_id("BetaCo")
        id_c = self._company_id("GammaCo")

        bad = self.client.post(
            "/api/company/merge",
            json={"company_ids": [id_a, id_b], "display_name": "GammaCo"},
        )
        self.assertEqual(bad.status_code, 400)
        payload = bad.json()
        self.assertFalse(payload["ok"])
        self.assertIn("already used", payload["error"])
        self.assertIn("merge that company too", payload["error"])
        self.rebuild.assert_not_called()

        for name, cid in (
            ("AlphaCo", id_a),
            ("BetaCo", id_b),
            ("GammaCo", id_c),
        ):
            self.assertEqual(resolve_company(self.db_path, name).id, cid)
            jobs = get_jobs_by_company(self.db_path, name)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["company"], name)

    def test_company_page_title_uses_display_name_for_alias(self):
        _insert(self.db_path, "https://example.com/a", "OldAlias")
        cid = self._company_id("OldAlias")
        self.client.post(
            "/api/company/rename",
            json={"company_id": cid, "display_name": "Pretty Name"},
        )
        response = self.client.get("/company.html", params={"company": "OldAlias"})
        self.assertEqual(response.status_code, 200)
        self.assertIn("Pretty Name", response.text)
        self.assertIn("Company Positions: Pretty Name", response.text)


if __name__ == "__main__":
    unittest.main()
