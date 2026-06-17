"""Tests for unified company+title position deduplication."""

import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from spejder.db.connection import ensure_db
from spejder.db.mutations import upsert_job
from spejder.jobs.deduplication import (
    DEDUPE_SNIPPET_MARKER,
    _merge_raw_text,
    _position_dedupe_key,
    merge_duplicate_positions,
)


def _utc_iso(offset_seconds: int = 0) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    ).isoformat()


def _insert_job_row(
    db_path: str,
    *,
    company: str,
    title: str,
    position_link: str,
    source: str,
    raw_text: str = "",
    place: str = "",
    work_type: str = "Unknown",
    viewed: int = 0,
    applied: int = 0,
    created_at: str,
) -> int:
    ensure_db(db_path)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        now = _utc_iso()
        cur.execute(
            """
            INSERT INTO jobs (
                source, company, title, place, work_type, position_link, raw_text,
                viewed, applied, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                company,
                title,
                place,
                work_type,
                position_link,
                raw_text,
                viewed,
                applied,
                created_at,
                now,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def _count_jobs(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM jobs")
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _fetch_job(db_path: str, job_id: int) -> tuple:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, company, title, place, work_type, position_link, raw_text, viewed, applied
            FROM jobs WHERE id=?
            """,
            (job_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


class PositionDedupeKeyTest(unittest.TestCase):
    def test_same_key_across_sources(self):
        company = "Cernel"
        title = "Product Operator"
        expected = "cernel|productoperator"
        self.assertEqual(_position_dedupe_key(company, title), expected)
        self.assertEqual(
            _position_dedupe_key(company, title),
            _position_dedupe_key(company, title),
        )
        self.assertEqual(
            _position_dedupe_key("Cernel A/S", "Product Operator"),
            expected,
        )


class MergeRawTextTest(unittest.TestCase):
    def test_appends_dissimilar_text(self):
        keeper = "Short digest snippet"
        duplicate = "Completely different job description body"
        merged = _merge_raw_text(keeper, duplicate)
        self.assertIn(DEDUPE_SNIPPET_MARKER, merged)
        self.assertIn(duplicate, merged)

    def test_skips_similar_text(self):
        keeper = "Product Operator at Cernel in Aarhus full time role"
        duplicate = "Product Operator at Cernel in Aarhus full-time role"
        merged = _merge_raw_text(keeper, duplicate)
        self.assertNotIn(DEDUPE_SNIPPET_MARKER, merged)


class MergeDuplicatePositionsTest(unittest.TestCase):
    def test_merges_thehub_and_jobindex_by_company_title(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            older_created = _utc_iso(-3600)
            newer_created = _utc_iso()

            keeper_id = _insert_job_row(
                db_path,
                company="Cernel",
                title="Product Operator",
                position_link="https://www.jobindex.dk/jobannonce/r13863385",
                source="Jobindex",
                raw_text="Jobindex listing with full description",
                place="Aarhus",
                applied=1,
                created_at=older_created,
            )
            duplicate_id = _insert_job_row(
                db_path,
                company="Cernel",
                title="Product Operator",
                position_link="https://thehub.io/jobs/6a2ff9c745360c3997b5350c",
                source="The Hub",
                raw_text="Product Operator Cernel Aarhus Full-time",
                place="Aarhus",
                created_at=newer_created,
            )
            self.assertEqual(_count_jobs(db_path), 2)

            result = merge_duplicate_positions(db_path)

            self.assertEqual(_count_jobs(db_path), 1)
            self.assertEqual(result["groups_merged"], 1)
            self.assertEqual(result["rows_deleted"], 1)
            self.assertEqual(result["rows_updated"], 1)

            row = _fetch_job(db_path, keeper_id)
            self.assertIsNotNone(row)
            self.assertEqual(row[0], keeper_id)
            self.assertEqual(row[1], "Jobindex")
            self.assertEqual(row[6], "https://www.jobindex.dk/jobannonce/r13863385")
            self.assertEqual(row[9], 1)
            self.assertIsNone(_fetch_job(db_path, duplicate_id))

    def test_keeper_is_oldest_not_newer_jobindex(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            linkedin_id = _insert_job_row(
                db_path,
                company="Acme",
                title="Engineer",
                position_link="https://www.linkedin.com/jobs/view/1",
                source="LinkedIn",
                raw_text="LinkedIn snippet",
                created_at=_utc_iso(-7200),
            )
            _insert_job_row(
                db_path,
                company="Acme",
                title="Engineer",
                position_link="https://www.jobindex.dk/jobannonce/r1",
                source="Jobindex",
                raw_text="Jobindex snippet",
                created_at=_utc_iso(-3600),
            )

            merge_duplicate_positions(db_path)

            self.assertEqual(_count_jobs(db_path), 1)
            row = _fetch_job(db_path, linkedin_id)
            self.assertIsNotNone(row)
            self.assertEqual(row[1], "LinkedIn")
            self.assertEqual(row[6], "https://www.linkedin.com/jobs/view/1")


class UpsertPositionDedupeTest(unittest.TestCase):
    def test_second_url_updates_oldest_row_without_insert(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            upsert_job(
                db_path,
                {
                    "company": "Cernel",
                    "title": "Product Operator",
                    "place": "Aarhus",
                    "work_type": "Unknown",
                    "position_link": "https://www.jobindex.dk/jobannonce/r13863385",
                    "raw_text": "Jobindex body",
                    "source": "Jobindex",
                },
            )
            is_new = upsert_job(
                db_path,
                {
                    "company": "Cernel",
                    "title": "Product Operator",
                    "place": "Aarhus",
                    "work_type": "Full-time",
                    "position_link": "https://thehub.io/jobs/6a2ff9c745360c3997b5350c",
                    "raw_text": "The Hub digest snippet",
                    "source": "The Hub",
                },
            )

            self.assertFalse(is_new)
            self.assertEqual(_count_jobs(db_path), 1)
            conn = sqlite3.connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT source, position_link FROM jobs WHERE company=? AND title=?",
                    ("Cernel", "Product Operator"),
                )
                row = cur.fetchone()
            finally:
                conn.close()
            self.assertEqual(row[0], "Jobindex")
            self.assertEqual(row[1], "https://www.jobindex.dk/jobannonce/r13863385")


if __name__ == "__main__":
    unittest.main()
