"""Regression: inbox dashboard report includes Hidden tab items."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_hidden, upsert_job
from spejder.db.connection import _connect
from spejder.workflows.inbox_report import write_inbox_dashboard_report


def _insert_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Parked Engineer",
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


class InboxReportHiddenTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.workflows.inbox_report.materialize_job_skills")
    def test_write_inbox_dashboard_report_includes_hidden(self, mock_materialize):
        mock_materialize.return_value = ("", "raw", False)
        job_id = _insert_job(self.db_path, "https://example.com/inbox-hidden")
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))

        path = write_inbox_dashboard_report(
            self.db_path,
            AppConfig(),
            MagicMock(),
            self.report_dir,
        )
        with open(path, encoding="utf-8") as handle:
            html = handle.read()

        self.assertIn("Hidden (1)", html)
        self.assertIn("Parked Engineer", html)
        self.assertIn(f'setHidden({job_id}', html)


if __name__ == "__main__":
    unittest.main()
