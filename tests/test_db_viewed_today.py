"""Tests for Edited today listings (get_viewed_today_jobs)."""

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from spejder.db import (
    ensure_db,
    get_viewed_today_jobs,
    local_day_start_utc_iso,
    set_job_applied,
    set_job_feedback,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect


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
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _set_updated_at(db_path: str, job_id: int, updated_at: str) -> None:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET updated_at=? WHERE id=?", (updated_at, job_id))
        conn.commit()
    finally:
        conn.close()


class ViewedTodayDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.since_iso = local_day_start_utc_iso()

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_local_day_start_utc_iso_is_utc_midnight_local(self):
        value = local_day_start_utc_iso()
        parsed = datetime.fromisoformat(value)
        self.assertIsNotNone(parsed.tzinfo)
        local_midnight = datetime.now().astimezone().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        self.assertEqual(parsed, local_midnight.astimezone(timezone.utc))

    def test_get_viewed_today_filters_and_exclusions(self):
        today_id = _insert_job(self.db_path, "https://example.com/today", title="Today")
        applied_id = _insert_job(self.db_path, "https://example.com/applied", title="Applied")
        hidden_id = _insert_job(self.db_path, "https://example.com/hidden", title="Hidden")
        old_id = _insert_job(self.db_path, "https://example.com/old", title="Old")
        unviewed_id = _insert_job(self.db_path, "https://example.com/unviewed", title="Unviewed")

        self.assertTrue(set_job_feedback(self.db_path, today_id, "relevant"))
        self.assertTrue(set_job_viewed(self.db_path, today_id, True))

        self.assertTrue(set_job_viewed(self.db_path, applied_id, True))
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))

        self.assertTrue(set_job_viewed(self.db_path, hidden_id, True))
        # Bypass mutual-exclusion mutations to assert SQL still excludes hidden=1.
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE jobs SET viewed=1, hidden=1, applied=0, updated_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), hidden_id),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertTrue(set_job_viewed(self.db_path, old_id, True))
        old_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        _set_updated_at(self.db_path, old_id, old_ts)

        self.assertTrue(set_job_feedback(self.db_path, unviewed_id, "relevant"))

        rows = get_viewed_today_jobs(self.db_path, self.since_iso, limit=0)
        ids = {row["id"] for row in rows}
        self.assertEqual(ids, {today_id})
        self.assertNotIn(applied_id, ids)
        self.assertNotIn(hidden_id, ids)
        self.assertNotIn(old_id, ids)
        self.assertNotIn(unviewed_id, ids)

    def test_get_viewed_today_sorts_updated_at_desc(self):
        older_id = _insert_job(self.db_path, "https://example.com/older", title="Older")
        newer_id = _insert_job(self.db_path, "https://example.com/newer", title="Newer")
        self.assertTrue(set_job_viewed(self.db_path, older_id, True))
        self.assertTrue(set_job_viewed(self.db_path, newer_id, True))

        now = datetime.now(timezone.utc)
        _set_updated_at(self.db_path, older_id, (now - timedelta(hours=2)).isoformat())
        _set_updated_at(self.db_path, newer_id, (now - timedelta(minutes=5)).isoformat())

        rows = get_viewed_today_jobs(self.db_path, self.since_iso, limit=0)
        self.assertEqual([row["id"] for row in rows], [newer_id, older_id])


if __name__ == "__main__":
    unittest.main()
