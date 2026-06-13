"""DB tests for cover letter fields on applied jobs."""

import os
import tempfile
import unittest

from spejder.db import (
    ensure_db,
    set_job_applied,
    set_job_cover_letter,
    set_job_cover_letter_requested,
    upsert_job,
)
from spejder.db.connection import _connect


def _insert_applied_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Engineer",
            "position_link": link,
            "raw_text": "raw",
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


class DbCoverLetterTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _cover_letter_row(self, job_id: int):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT cover_letter, cover_letter_requested FROM jobs WHERE id=?",
                (job_id,),
            )
            return cur.fetchone()
        finally:
            conn.close()

    def test_cover_letter_requested_requires_applied(self):
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/not-applied",
                "raw_text": "raw",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM jobs WHERE position_link=?", ("https://example.com/not-applied",))
            job_id = int(cur.fetchone()[0])
        finally:
            conn.close()

        self.assertFalse(set_job_cover_letter_requested(self.db_path, job_id, True))

    def test_cover_letter_save_requires_requested(self):
        job_id = _insert_applied_job(self.db_path, "https://example.com/no-request")
        self.assertFalse(set_job_cover_letter(self.db_path, job_id, "Hello"))

        self.assertTrue(set_job_cover_letter_requested(self.db_path, job_id, True))
        self.assertTrue(set_job_cover_letter(self.db_path, job_id, "Hello"))
        cover_letter, requested = self._cover_letter_row(job_id)
        self.assertEqual(cover_letter, "Hello")
        self.assertEqual(requested, 1)

    def test_cover_letter_one_time_write(self):
        job_id = _insert_applied_job(self.db_path, "https://example.com/one-time")
        self.assertTrue(set_job_cover_letter_requested(self.db_path, job_id, True))
        self.assertTrue(set_job_cover_letter(self.db_path, job_id, "First"))
        self.assertTrue(set_job_cover_letter(self.db_path, job_id, "Second"))
        cover_letter, _ = self._cover_letter_row(job_id)
        self.assertEqual(cover_letter, "First")

    def test_unapply_clears_cover_letter_fields(self):
        job_id = _insert_applied_job(self.db_path, "https://example.com/unapply")
        self.assertTrue(set_job_cover_letter_requested(self.db_path, job_id, True))
        self.assertTrue(set_job_cover_letter(self.db_path, job_id, "Hello"))
        self.assertTrue(set_job_applied(self.db_path, job_id, False))
        cover_letter, requested = self._cover_letter_row(job_id)
        self.assertIsNone(cover_letter)
        self.assertEqual(requested, 0)


if __name__ == "__main__":
    unittest.main()
