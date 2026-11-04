"""Tests for category triage-window listing (non-hidden LIMIT walk)."""

import os
import tempfile
import unittest

from spejder.db import ensure_db, get_jobs_by_category_triage_window, upsert_job
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


def _set_triage_row(
    db_path: str,
    job_id: int,
    *,
    score: float,
    hidden: int = 0,
    category: str = "relevant",
    viewed: int = 0,
    updated_at: str = "2024-01-01T00:00:00+00:00",
) -> None:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE jobs
            SET relevance_score=?, hidden=?, category=?, viewed=?, updated_at=?
            WHERE id=?
            """,
            (score, hidden, category, viewed, updated_at, job_id),
        )
        conn.commit()
    finally:
        conn.close()


class TriageWindowDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_triage_window_includes_in_range_soft_hidden_only(self):
        a = _insert_job(self.db_path, "https://example.com/a", title="A")
        b = _insert_job(self.db_path, "https://example.com/b", title="B")
        c = _insert_job(self.db_path, "https://example.com/c", title="C")
        d = _insert_job(self.db_path, "https://example.com/d", title="D")
        e = _insert_job(self.db_path, "https://example.com/e", title="E")
        _set_triage_row(self.db_path, a, score=100.0, hidden=0)
        _set_triage_row(self.db_path, b, score=90.0, hidden=1)
        _set_triage_row(self.db_path, c, score=80.0, hidden=0)
        _set_triage_row(self.db_path, d, score=70.0, hidden=1)
        _set_triage_row(self.db_path, e, score=60.0, hidden=0)

        rows = get_jobs_by_category_triage_window(
            self.db_path, "relevant", non_hidden_limit=2, unviewed_only=True
        )
        self.assertEqual([row["id"] for row in rows], [a, b, c])
        self.assertEqual([int(row["hidden"] or 0) for row in rows], [0, 1, 0])

    def test_triage_window_excludes_soft_hidden_after_limit(self):
        first = _insert_job(self.db_path, "https://example.com/first", title="First")
        soft = _insert_job(self.db_path, "https://example.com/soft", title="Soft")
        _set_triage_row(self.db_path, first, score=100.0, hidden=0)
        _set_triage_row(self.db_path, soft, score=90.0, hidden=1)

        rows = get_jobs_by_category_triage_window(
            self.db_path, "relevant", non_hidden_limit=1, unviewed_only=True
        )
        self.assertEqual([row["id"] for row in rows], [first])

    def test_triage_window_leading_soft_hidden_included(self):
        soft = _insert_job(self.db_path, "https://example.com/lead-soft", title="Soft")
        visible = _insert_job(self.db_path, "https://example.com/lead-vis", title="Vis")
        extra = _insert_job(self.db_path, "https://example.com/lead-extra", title="Extra")
        _set_triage_row(self.db_path, soft, score=100.0, hidden=1)
        _set_triage_row(self.db_path, visible, score=90.0, hidden=0)
        _set_triage_row(self.db_path, extra, score=80.0, hidden=0)

        rows = get_jobs_by_category_triage_window(
            self.db_path, "relevant", non_hidden_limit=1, unviewed_only=True
        )
        self.assertEqual([row["id"] for row in rows], [soft, visible])


if __name__ == "__main__":
    unittest.main()
