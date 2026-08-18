"""Tests for IT-DAY portal fetch/parse and sync wiring."""

import json
import os
import tempfile
import unittest
from io import BytesIO
from typing import Optional
from unittest.mock import patch
from urllib.error import HTTPError

from spejder.config import AppConfig
from spejder.db import ensure_db, upsert_job
from spejder.db.connection import _connect
from spejder.parsers.itday_portal import (
    ITDAY_PORTAL_SOURCE,
    ITDAY_PORTAL_URL,
    fetch_itday_portal_entries,
    fetch_itday_portal_html,
    parse_itday_portal_html,
)
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync
from spejder.workflows.inbox_workflow import process_inbox
from spejder.workflows.portal_sync import sync_itday_portal


_FIXTURE_PATH = os.path.join(
    os.path.dirname(__file__),
    "fixtures",
    "itday_portal_sample.html",
)


class ItdayPortalParserTest(unittest.TestCase):
    def test_parse_itday_portal_html_extracts_cards(self):
        with open(_FIXTURE_PATH, encoding="utf-8") as handle:
            html_text = handle.read()

        entries = parse_itday_portal_html(html_text)
        links = [entry["position_link"] for entry in entries]
        by_link = {entry["position_link"]: entry for entry in entries}

        self.assertEqual(len(entries), 6)
        self.assertEqual(entries[0]["company"], "Eurowind Energy")
        self.assertEqual(entries[0]["title"], "Global Development Support")
        self.assertEqual(entries[0]["place"], "Hobro")
        self.assertEqual(entries[0]["work_type"], "Student Job")
        self.assertEqual(entries[0]["source"], ITDAY_PORTAL_SOURCE)
        self.assertEqual(entries[0]["position_link"], "https://example.com/jobs/one")
        self.assertIn("IT-DAY Job Portal", entries[0]["raw_text"])
        self.assertEqual(by_link["https://careers.example.com/intern"]["work_type"], "Intern")
        self.assertEqual(by_link["https://www.itday.dk/praktik/foo"]["title"], "Praktikant")
        self.assertIn("https://www.linkedin.com/jobs/view/123", links)
        self.assertEqual(
            by_link["https://jobs.example.com/apply/http-one"]["company"],
            "HttpCo",
        )
        self.assertEqual(links.count("https://example.com/jobs/one"), 1)
        self.assertNotIn("https://www.linkedin.com/company/itday", links)
        self.assertNotIn("https://www.linkedin.com/in/someone", links)

    @patch("spejder.parsers.itday_portal.fetch_itday_portal_html")
    def test_fetch_itday_portal_entries_stops_on_empty_page(self, mock_fetch):
        with open(_FIXTURE_PATH, encoding="utf-8") as handle:
            sample_html = handle.read()
        mock_fetch.side_effect = [sample_html, ""]

        entries = fetch_itday_portal_entries(max_pages=3)

        self.assertEqual(len(entries), 6)
        self.assertEqual(mock_fetch.call_count, 2)

    @patch("spejder.parsers.itday_portal.urlopen")
    def test_fetch_itday_portal_html_page_urls_and_http_error(self, mock_urlopen):
        response = mock_urlopen.return_value.__enter__.return_value
        response.headers.get_content_charset.return_value = "utf-8"
        response.read.return_value = b"<html></html>"

        fetch_itday_portal_html(page=1)
        fetch_itday_portal_html(page=2)
        urls = [call.args[0].full_url for call in mock_urlopen.call_args_list]
        self.assertEqual(urls[0], ITDAY_PORTAL_URL)
        self.assertEqual(urls[1], f"{ITDAY_PORTAL_URL}?dynamic_page=2")

        mock_urlopen.side_effect = HTTPError(
            ITDAY_PORTAL_URL,
            503,
            "Service Unavailable",
            hdrs=None,
            fp=BytesIO(),
        )
        with self.assertRaises(RuntimeError) as ctx:
            fetch_itday_portal_html(page=1)
        self.assertIn("IT-DAY portal fetch failed", str(ctx.exception))


class ItdayPortalSyncTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.workflows.portal_sync.fetch_itday_portal_entries")
    def test_sync_itday_portal_upserts_new_jobs(self, mock_fetch):
        mock_fetch.return_value = [
            {
                "position_link": "https://example.com/jobs/one",
                "company": "Eurowind Energy",
                "title": "Global Development Support",
                "place": "Hobro",
                "work_type": "Student Job",
                "raw_text": "sample",
                "source": ITDAY_PORTAL_SOURCE,
            }
        ]

        first = sync_itday_portal(self.db_path)
        second = sync_itday_portal(self.db_path)

        self.assertEqual(first["inserted_new"], 1)
        self.assertEqual(first["skipped_existing"], 0)
        self.assertEqual(second["inserted_new"], 0)
        self.assertEqual(second["skipped_existing"], 1)

        row = self._fetch_job("https://example.com/jobs/one")
        self.assertIsNotNone(row)
        self.assertEqual(row["company"], "Eurowind Energy")
        self.assertEqual(row["source"], ITDAY_PORTAL_SOURCE)

    @patch(
        "spejder.workflows.portal_sync.fetch_itday_portal_entries",
        side_effect=RuntimeError("portal down"),
    )
    def test_sync_itday_portal_returns_error_without_abort(self, mock_fetch):
        stats = sync_itday_portal(self.db_path)

        self.assertEqual(stats["inserted_new"], 0)
        self.assertEqual(stats["found"], 0)
        self.assertIn("error", stats)
        mock_fetch.assert_called_once()

    def _fetch_job(self, link: str) -> Optional[dict]:
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT company, source FROM jobs WHERE position_link=? LIMIT 1",
                (link,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {"company": row[0], "source": row[1]}


class RunInboxSyncPortalTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.rebuild_reasons: list[str] = []
        self.context = GuiSyncContext(
            db_path=os.path.join(self._tmpdir.name, "jobs.db"),
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[])
    def test_skips_when_portal_and_inbox_have_nothing_new(self, *_mocks):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "skipped")

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 5, "inserted_new": 0, "skipped_existing": 5, "processed": 5},
    )
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[])
    def test_skips_when_portal_only_finds_existing_jobs(self, *_mocks):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "skipped")

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 1, "inserted_new": 1, "skipped_existing": 0, "processed": 1},
    )
    @patch(
        "spejder.workflows.gui_sync.recalibrate_and_store_threshold",
        return_value=0.1,
    )
    @patch(
        "spejder.workflows.gui_sync.ensure_bad_cloud_initialized",
        return_value={"seeded": False, "pruned": []},
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
    )
    @patch(
        "spejder.workflows.gui_sync.cleanup_blocked_skills_from_db",
        return_value={
            "skills_processed": 0,
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
        },
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_refresh", return_value=[])
    def test_runs_pipeline_when_portal_inserts_new_jobs(self, *_mocks):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")


class ItdayPortalEnsureDbPruneTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_ensure_db_keeps_portal_source_urls_and_prunes_other(self):
        portal_links = (
            "https://bankdata.teamtailor.com/jobs/1",
            "https://www.itday.dk/praktik/foo",
        )
        for index, link in enumerate(portal_links):
            upsert_job(
                self.db_path,
                {
                    "source": ITDAY_PORTAL_SOURCE,
                    "company": f"PortalCo{index}",
                    "title": f"Portal Role {index}",
                    "position_link": link,
                    "raw_text": "raw",
                },
            )
        dropped_link = "https://other.teamtailor.com/jobs/99"
        upsert_job(
            self.db_path,
            {
                "source": "Teamtailor",
                "company": "Other",
                "title": "Should be pruned",
                "position_link": dropped_link,
                "raw_text": "raw",
            },
        )

        ensure_db(self.db_path)

        for link in portal_links:
            row = self._fetch_job(link)
            self.assertIsNotNone(row, msg=link)
            self.assertEqual(row["source"], ITDAY_PORTAL_SOURCE)
        self.assertIsNone(self._fetch_job(dropped_link))

    def _fetch_job(self, link: str) -> Optional[dict]:
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT source FROM jobs WHERE position_link=? LIMIT 1",
                (link,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return {"source": row[0]}


class ProcessInboxPortalTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.inbox = os.path.join(self._tmpdir.name, "inbox")
        os.makedirs(self.inbox)
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        with open(self.profile_path, "w", encoding="utf-8") as handle:
            json.dump({}, handle)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={"found": 1, "inserted_new": 1, "skipped_existing": 0, "processed": 1},
    )
    def test_empty_inbox_continues_when_portal_inserts(self, mock_sync):
        with self.assertRaises(SystemExit):
            process_inbox(
                inbox=self.inbox,
                db=self.db_path,
                profile=self.profile_path,
                model="",
            )
        mock_sync.assert_called_once()


if __name__ == "__main__":
    unittest.main()
