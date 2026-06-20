"""Tests for applied_at column, retention exemptions, and dashboard display."""

import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from spejder.db import (
    ensure_db,
    get_applied_jobs,
    set_job_applied,
    set_job_feedback,
    set_job_on_interview,
    set_job_interview_stopped,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect

_LINKEDIN_LINK = "https://www.linkedin.com/jobs/view/9000000001"
_OLD_CREATED_AT = "2020-01-01T00:00:00+00:00"


def _insert_job(db_path: str, link: str, *, title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
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
    return job_id


def _applied_at(db_path: str, job_id: int):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT applied_at FROM jobs WHERE id=?", (job_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _job_exists(db_path: str, job_id: int) -> bool:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def _insert_old_job(
    db_path: str,
    link: str,
    *,
    applied: int = 0,
    on_interview: int = 0,
    interview_stopped: int = 0,
) -> int:
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (
                source, company, title, position_link, raw_text, created_at, updated_at,
                applied, on_interview, interview_stopped
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Test",
                "Acme",
                "Old Role",
                link,
                "raw",
                _OLD_CREATED_AT,
                now,
                applied,
                on_interview,
                interview_stopped,
            ),
        )
        job_id = int(cur.lastrowid)
        conn.commit()
        return job_id
    finally:
        conn.close()


class AppliedAtDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_set_job_applied_true_sets_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-set")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertIsNotNone(_applied_at(self.db_path, job_id))

    def test_set_job_applied_false_clears_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-clear")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_applied(self.db_path, job_id, False))
        self.assertIsNone(_applied_at(self.db_path, job_id))

    def test_set_job_viewed_false_clears_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-viewed-clear")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_viewed(self.db_path, job_id, False))
        self.assertIsNone(_applied_at(self.db_path, job_id))

    def test_not_relevant_feedback_clears_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-feedback")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_feedback(self.db_path, job_id, "not relevant"))
        self.assertIsNone(_applied_at(self.db_path, job_id))

    def test_reapply_after_unapply_gets_new_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-reapply")
        first = datetime(2024, 1, 1, tzinfo=timezone.utc)
        second = datetime(2024, 6, 1, tzinfo=timezone.utc)

        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = first
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_id, True))
        first_applied_at = _applied_at(self.db_path, job_id)

        self.assertTrue(set_job_applied(self.db_path, job_id, False))

        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = second
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_id, True))
        second_applied_at = _applied_at(self.db_path, job_id)

        self.assertNotEqual(first_applied_at, second_applied_at)
        self.assertEqual(second_applied_at, second.isoformat())

    def test_reapply_true_keeps_existing_applied_at(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-at-stable")
        first = datetime(2024, 3, 15, tzinfo=timezone.utc)

        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = first
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_id, True))
        first_applied_at = _applied_at(self.db_path, job_id)

        later = datetime(2025, 1, 1, tzinfo=timezone.utc)
        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = later
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_id, True))

        self.assertEqual(_applied_at(self.db_path, job_id), first_applied_at)

    def test_ensure_db_retains_old_interview_job(self):
        job_id = _insert_old_job(
            self.db_path,
            _LINKEDIN_LINK,
            applied=1,
            on_interview=1,
        )
        ensure_db(self.db_path)
        self.assertTrue(_job_exists(self.db_path, job_id))

    def test_ensure_db_retains_old_stopped_job(self):
        job_id = _insert_old_job(
            self.db_path,
            "https://www.linkedin.com/jobs/view/9000000002",
            applied=1,
            interview_stopped=1,
        )
        ensure_db(self.db_path)
        self.assertTrue(_job_exists(self.db_path, job_id))

    def test_ensure_db_prunes_old_plain_applied_job(self):
        job_id = _insert_old_job(
            self.db_path,
            "https://www.linkedin.com/jobs/view/9000000003",
            applied=1,
        )
        ensure_db(self.db_path)
        self.assertFalse(_job_exists(self.db_path, job_id))

    def test_ensure_db_backfills_applied_at(self):
        known_updated_at = "2023-07-15T10:00:00+00:00"
        recent_created_at = datetime.now(timezone.utc).isoformat()
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (
                    source, company, title, position_link, raw_text,
                    created_at, updated_at, applied, applied_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    "Test",
                    "Acme",
                    "Backfill Role",
                    "https://www.linkedin.com/jobs/view/9000000099",
                    "raw",
                    recent_created_at,
                    known_updated_at,
                    1,
                ),
            )
            job_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

        ensure_db(self.db_path)
        self.assertEqual(_applied_at(self.db_path, job_id), known_updated_at)

    def test_ensure_db_prunes_demoted_old_interview_job(self):
        job_id = _insert_old_job(
            self.db_path,
            "https://www.linkedin.com/jobs/view/9000000004",
            applied=1,
            on_interview=1,
        )
        ensure_db(self.db_path)
        self.assertTrue(_job_exists(self.db_path, job_id))

        self.assertTrue(set_job_on_interview(self.db_path, job_id, False))
        ensure_db(self.db_path)
        self.assertFalse(_job_exists(self.db_path, job_id))

    def test_ensure_db_prunes_demoted_old_stopped_job(self):
        job_id = _insert_old_job(
            self.db_path,
            "https://www.linkedin.com/jobs/view/9000000005",
            applied=1,
            interview_stopped=1,
        )
        ensure_db(self.db_path)
        self.assertTrue(_job_exists(self.db_path, job_id))

        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, False))
        ensure_db(self.db_path)
        self.assertFalse(_job_exists(self.db_path, job_id))

    def test_get_applied_jobs_sorts_by_applied_at_desc(self):
        earlier = datetime(2024, 1, 1, tzinfo=timezone.utc)
        later = datetime(2024, 6, 1, tzinfo=timezone.utc)
        job_early = _insert_job(
            self.db_path, "https://example.com/sort-early", title="Early Role"
        )
        job_late = _insert_job(
            self.db_path, "https://example.com/sort-late", title="Late Role"
        )

        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = earlier
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_early, True))
        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = later
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_late, True))

        rows = get_applied_jobs(self.db_path, limit=0)
        self.assertEqual([row["id"] for row in rows], [job_late, job_early])

    def test_get_applied_jobs_null_applied_at_sorts_after_dated(self):
        earlier = datetime(2024, 1, 1, tzinfo=timezone.utc)
        job_dated = _insert_job(
            self.db_path, "https://example.com/sort-dated", title="Dated Role"
        )
        job_null = _insert_job(
            self.db_path, "https://example.com/sort-null", title="Null Role"
        )

        with patch("spejder.db.mutations.datetime") as mock_dt:
            mock_dt.now.return_value = earlier
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            self.assertTrue(set_job_applied(self.db_path, job_dated, True))
            self.assertTrue(set_job_applied(self.db_path, job_null, True))

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE jobs SET applied_at=NULL WHERE id=?", (job_null,))
            conn.commit()
        finally:
            conn.close()

        rows = get_applied_jobs(self.db_path, limit=0)
        self.assertEqual([row["id"] for row in rows], [job_dated, job_null])


if __name__ == "__main__":
    unittest.main()
