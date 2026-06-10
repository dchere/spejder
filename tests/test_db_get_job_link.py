"""Tests for get_job_link DB helper."""

import os
import tempfile
import unittest

from spejder.db.connection import ensure_db, get_job_link
from spejder.db.mutations import upsert_job


class GetJobLinkTest(unittest.TestCase):
    def test_returns_position_link_for_job_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            upsert_job(
                db_path,
                {
                    "title": "Engineer",
                    "company": "Acme",
                    "place": "",
                    "work_type": "Unknown",
                    "position_link": "https://djinni.co/jobs/1-example",
                    "raw_text": "body",
                    "source": "Djinni",
                },
            )
            row = get_job_link(db_path, 1)
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "https://djinni.co/jobs/1-example")


if __name__ == "__main__":
    unittest.main()
