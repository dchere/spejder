"""Tests for Hidden positions DB mutations and listings."""

import os
import tempfile
import unittest

from spejder.db import (
    batch_update_and_delete_jobs,
    ensure_db,
    get_applied_jobs,
    get_hidden_jobs,
    get_hidden_jobs_count,
    get_interview_jobs,
    get_jobs_by_category,
    get_relevant_jobs,
    get_stopped_interview_jobs,
    set_job_applied,
    set_job_company_feedback,
    set_job_feedback,
    set_job_hidden,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.workflows.dashboard import build_dashboard_record
from spejder.config import AppConfig


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


def _job_flags(db_path: str, job_id: int):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT hidden, viewed, applied, on_interview, interview_stopped,
                   company_feedback, category, cover_letter, cover_letter_requested
            FROM jobs WHERE id=?
            """,
            (job_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


class HiddenDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_ensure_db_adds_hidden_column(self):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(jobs)")
            cols = {row[1] for row in cur.fetchall()}
        finally:
            conn.close()
        self.assertIn("hidden", cols)

    def test_hide_clears_pipeline_keeps_category(self):
        job_id = _insert_job(self.db_path, "https://example.com/hide")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "No hire"))

        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        row = _job_flags(self.db_path, job_id)
        self.assertEqual(row[0], 1)  # hidden
        self.assertEqual(row[1], 0)  # viewed
        self.assertEqual(row[2], 0)  # applied
        self.assertEqual(row[3], 0)  # on_interview
        self.assertEqual(row[4], 0)  # interview_stopped
        self.assertIsNone(row[5])  # company_feedback
        self.assertEqual(row[6], "relevant")  # category preserved
        self.assertIsNone(row[7])  # cover_letter
        self.assertEqual(row[8], 0)  # cover_letter_requested

    def test_unhide_only_clears_hidden_flag(self):
        job_id = _insert_job(self.db_path, "https://example.com/unhide")
        self.assertTrue(set_job_feedback(self.db_path, job_id, "not relevant"))
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        self.assertTrue(set_job_hidden(self.db_path, job_id, False))
        row = _job_flags(self.db_path, job_id)
        self.assertEqual(row[0], 0)
        self.assertEqual(row[6], "not relevant")

    def test_apply_and_viewed_clear_hidden(self):
        job_id = _insert_job(self.db_path, "https://example.com/clear-hidden")
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        self.assertTrue(set_job_viewed(self.db_path, job_id, True))
        self.assertEqual(_job_flags(self.db_path, job_id)[0], 0)

        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertEqual(_job_flags(self.db_path, job_id)[0], 0)
        self.assertEqual(_job_flags(self.db_path, job_id)[2], 1)

    def test_listings_exclude_hidden(self):
        relevant_id = _insert_job(self.db_path, "https://example.com/rel", title="Relevant")
        applied_id = _insert_job(self.db_path, "https://example.com/app", title="Applied")
        self.assertTrue(set_job_feedback(self.db_path, relevant_id, "relevant"))
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))
        self.assertTrue(set_job_hidden(self.db_path, relevant_id, True))
        self.assertTrue(set_job_hidden(self.db_path, applied_id, True))

        self.assertEqual(get_jobs_by_category(self.db_path, "relevant", limit=0), [])
        self.assertEqual(get_relevant_jobs(self.db_path, limit=0), [])
        self.assertEqual(get_applied_jobs(self.db_path, limit=0), [])
        self.assertEqual(get_interview_jobs(self.db_path, limit=0), [])
        self.assertEqual(get_stopped_interview_jobs(self.db_path, limit=0), [])

        parked = get_jobs_by_category(
            self.db_path, "relevant", limit=0, exclude_hidden=False
        )
        self.assertEqual({row["id"] for row in parked}, {relevant_id, applied_id})
        self.assertEqual(get_hidden_jobs_count(self.db_path), 2)

    def test_get_hidden_jobs(self):
        job_id = _insert_job(self.db_path, "https://example.com/hidden-list")
        self.assertTrue(set_job_feedback(self.db_path, job_id, "relevant"))
        self.assertTrue(set_job_hidden(self.db_path, job_id, True))
        rows = get_hidden_jobs(self.db_path, limit=0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], job_id)
        self.assertEqual(rows[0]["hidden"], 1)
        self.assertEqual(rows[0]["category"], "relevant")
        self.assertEqual(get_hidden_jobs_count(self.db_path), 1)

    def test_build_dashboard_record_includes_hidden(self):
        record = build_dashboard_record(
            db_path=self.db_path,
            runtime_profile=AppConfig(),
            title_translation_cache={},
            row={"id": 1, "hidden": 1, "category": "relevant"},
            default_category="relevant",
            translate_title=False,
        )
        self.assertEqual(record["hidden"], 1)

    def test_dedupe_merge_clears_hidden_when_viewed(self):
        from spejder.jobs.deduplication import merge_duplicate_positions

        keeper_id = _insert_job(self.db_path, "https://example.com/keeper-hidden")
        self.assertTrue(set_job_hidden(self.db_path, keeper_id, True))

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (
                    source, company, title, place, work_type, position_link, raw_text,
                    viewed, applied, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Test",
                    "Acme",
                    "Engineer",
                    "",
                    "Unknown",
                    "https://example.com/dup-viewed",
                    "raw",
                    1,
                    0,
                    "2099-01-02T00:00:00+00:00",
                    "2099-01-02T00:00:00+00:00",
                ),
            )
            dup_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

        result = merge_duplicate_positions(self.db_path)
        self.assertEqual(result["groups_merged"], 1)
        self.assertEqual(result["rows_deleted"], 1)

        row = _job_flags(self.db_path, keeper_id)
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)  # hidden cleared
        self.assertEqual(row[1], 1)  # viewed kept
        self.assertIsNone(_job_flags(self.db_path, dup_id))

    def test_dedupe_merge_clears_hidden_when_applied(self):
        from spejder.jobs.deduplication import merge_duplicate_positions

        keeper_id = _insert_job(self.db_path, "https://example.com/keeper-hidden-app")
        self.assertTrue(set_job_hidden(self.db_path, keeper_id, True))

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (
                    source, company, title, place, work_type, position_link, raw_text,
                    viewed, applied, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Test",
                    "Acme",
                    "Engineer",
                    "",
                    "Unknown",
                    "https://example.com/dup-applied",
                    "raw",
                    1,
                    1,
                    "2099-01-02T00:00:00+00:00",
                    "2099-01-02T00:00:00+00:00",
                ),
            )
            dup_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

        merge_duplicate_positions(self.db_path)

        row = _job_flags(self.db_path, keeper_id)
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 0)  # hidden cleared
        self.assertEqual(row[2], 1)  # applied kept
        self.assertIsNone(_job_flags(self.db_path, dup_id))

    def test_dedupe_merge_preserves_hidden_from_duplicate(self):
        from spejder.jobs.deduplication import merge_duplicate_positions

        keeper_id = _insert_job(self.db_path, "https://example.com/keeper-visible")
        # Keeper stays visible (hidden=0); duplicate is parked.
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO jobs (
                    source, company, title, place, work_type, position_link, raw_text,
                    viewed, applied, hidden, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Test",
                    "Acme",
                    "Engineer",
                    "",
                    "Unknown",
                    "https://example.com/dup-hidden",
                    "raw",
                    0,
                    0,
                    1,
                    "2099-01-02T00:00:00+00:00",
                    "2099-01-02T00:00:00+00:00",
                ),
            )
            dup_id = int(cur.lastrowid)
            conn.commit()
        finally:
            conn.close()

        result = merge_duplicate_positions(self.db_path)
        self.assertEqual(result["groups_merged"], 1)
        self.assertEqual(result["rows_deleted"], 1)

        row = _job_flags(self.db_path, keeper_id)
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 1)  # hidden preserved from duplicate
        self.assertEqual(row[1], 0)
        self.assertEqual(row[2], 0)
        self.assertIsNone(_job_flags(self.db_path, dup_id))

    def test_batch_update_clears_hidden_when_viewed_or_applied(self):
        viewed_id = _insert_job(
            self.db_path, "https://example.com/batch-viewed", title="Batch Viewed"
        )
        applied_id = _insert_job(
            self.db_path, "https://example.com/batch-applied", title="Batch Applied"
        )
        keep_hidden_id = _insert_job(
            self.db_path, "https://example.com/batch-keep", title="Batch Keep"
        )
        self.assertTrue(set_job_hidden(self.db_path, viewed_id, True))
        self.assertTrue(set_job_hidden(self.db_path, applied_id, True))
        self.assertTrue(set_job_hidden(self.db_path, keep_hidden_id, True))

        now = "2099-01-02T00:00:00+00:00"
        batch_update_and_delete_jobs(
            self.db_path,
            [
                ("Acme", "Batch Viewed", "", "Unknown", "raw", 1, 0, 1, now, viewed_id),
                ("Acme", "Batch Applied", "", "Unknown", "raw", 0, 1, 1, now, applied_id),
                ("Acme", "Batch Keep", "", "Unknown", "raw", 0, 0, 1, now, keep_hidden_id),
            ],
            [],
        )

        self.assertEqual(_job_flags(self.db_path, viewed_id)[0], 0)
        self.assertEqual(_job_flags(self.db_path, viewed_id)[1], 1)
        self.assertEqual(_job_flags(self.db_path, applied_id)[0], 0)
        self.assertEqual(_job_flags(self.db_path, applied_id)[2], 1)
        self.assertEqual(_job_flags(self.db_path, keep_hidden_id)[0], 1)


if __name__ == "__main__":
    unittest.main()
