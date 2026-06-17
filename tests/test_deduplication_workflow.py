"""Tests for cross-source dedupe workflow helpers."""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from spejder.workflows.deduplication import dedupe_jobs, run_cross_source_dedupe


class RunCrossSourceDedupeTest(unittest.TestCase):
    @patch("spejder.workflows.deduplication.merge_duplicate_positions")
    def test_passthrough_and_stdout(self, mock_merge):
        mock_merge.return_value = {
            "groups_merged": 2,
            "rows_updated": 3,
            "rows_deleted": 1,
        }
        buf = io.StringIO()
        with redirect_stdout(buf):
            result = run_cross_source_dedupe("/tmp/jobs.db", log_prefix="Test dedupe")

        self.assertEqual(result, mock_merge.return_value)
        mock_merge.assert_called_once_with("/tmp/jobs.db")
        out = buf.getvalue()
        self.assertIn("Test dedupe:", out)
        self.assertIn("groups_merged=2", out)
        self.assertIn("rows_updated=3", out)
        self.assertIn("rows_deleted=1", out)
        self.assertNotIn("db=", out)


class DedupeJobsCLITest(unittest.TestCase):
    @patch("spejder.workflows.deduplication.merge_duplicate_positions")
    @patch("spejder.workflows.deduplication.ensure_db")
    @patch("spejder.workflows.deduplication.load_runtime_profile")
    def test_prints_job_dedupe_complete_with_db(self, mock_profile, mock_ensure, mock_merge):
        mock_profile.return_value.default_db = "./jobs.db"
        mock_merge.return_value = {
            "groups_merged": 0,
            "rows_updated": 0,
            "rows_deleted": 0,
        }
        buf = io.StringIO()
        with redirect_stdout(buf):
            dedupe_jobs(db="/custom/jobs.db")

        mock_ensure.assert_called_once_with("/custom/jobs.db")
        out = buf.getvalue()
        self.assertIn("Job dedupe complete:", out)
        self.assertIn("db=/custom/jobs.db", out)


if __name__ == "__main__":
    unittest.main()
