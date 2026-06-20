"""Smoke tests for split db query modules."""

import os
import tempfile
import unittest

from spejder.db import ensure_db, get_viewed_jobs_count
from spejder.db.queries import get_jobs_by_category
from spejder.db.queries_rows import _map_full_job_row


class DbQueriesTest(unittest.TestCase):
    def test_facade_reexports_and_viewed_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            self.assertEqual(get_viewed_jobs_count(db_path), 0)
            self.assertEqual(get_jobs_by_category(db_path, "relevant"), [])

    def test_map_full_job_row(self):
        row = (
            1,
            "linkedin",
            "Acme",
            "Engineer",
            "engineer",
            "Remote",
            "full-time",
            "https://example.com/job",
            "raw",
            0.75,
            "reason",
            "summary",
            0,
            0,
            0,
            0,
            "",
            "desc",
            "",
            0,
            "2024-01-01T00:00:00+00:00",
        )
        mapped = _map_full_job_row(row, "relevant")
        self.assertEqual(mapped["id"], 1)
        self.assertEqual(mapped["category"], "relevant")
        self.assertEqual(mapped["relevance_score"], 0.75)
        self.assertEqual(mapped["on_interview"], 0)
        self.assertEqual(mapped["interview_stopped"], 0)
        self.assertEqual(mapped["company_feedback"], "")
        self.assertEqual(mapped["cover_letter"], "")
        self.assertEqual(mapped["cover_letter_requested"], 0)
        self.assertEqual(mapped["applied_at"], "2024-01-01T00:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
