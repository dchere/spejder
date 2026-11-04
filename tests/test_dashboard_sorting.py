"""Tests for dashboard job list sorting."""

import os
import tempfile
import unittest

from spejder.core import MANUAL_APPLIED_RAW_MARKER
from spejder.db import ensure_db, get_jobs_by_category_triage_window, upsert_job
from spejder.db.connection import _connect
from spejder.managers.dashboard_sorting import (
    _applied_position_is_complete,
    _sort_applied_positions,
    _sort_positions_unviewed_then_score,
    slice_triage_by_non_hidden_limit,
)


class DashboardSortingTest(unittest.TestCase):
    def test_unviewed_first_then_higher_score(self):
        items = [
            {"viewed": 1, "relevance_score": 0.9, "id": 1},
            {"viewed": 0, "relevance_score": 0.1, "id": 2},
            {"viewed": 0, "relevance_score": 0.8, "id": 3},
        ]
        sorted_items = _sort_positions_unviewed_then_score(items)
        self.assertEqual([i["relevance_score"] for i in sorted_items], [0.8, 0.1, 0.9])
        self.assertEqual(sorted_items[0]["viewed"], 0)

    def test_equal_score_tiebreak_updated_at_then_id(self):
        items = [
            {
                "id": 3,
                "viewed": 0,
                "relevance_score": 0.5,
                "updated_at": "2024-01-01T00:00:00+00:00",
            },
            {
                "id": 1,
                "viewed": 0,
                "relevance_score": 0.5,
                "updated_at": "2024-06-01T00:00:00+00:00",
            },
            {
                "id": 2,
                "viewed": 0,
                "relevance_score": 0.5,
                "updated_at": "2024-06-01T00:00:00+00:00",
            },
            {
                "id": 4,
                "viewed": 0,
                "relevance_score": 0.5,
                "updated_at": "",
            },
        ]
        sorted_items = _sort_positions_unviewed_then_score(items)
        self.assertEqual([i["id"] for i in sorted_items], [1, 2, 3, 4])

    def test_equal_score_walk_membership_matches_triage_window(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)

            def _insert(link: str, title: str) -> int:
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

            a = _insert("https://example.com/a", "A")
            b = _insert("https://example.com/b", "B")
            c = _insert("https://example.com/c", "C")
            d = _insert("https://example.com/d", "D")
            conn = _connect(db_path)
            try:
                cur = conn.cursor()
                # Equal scores: membership hinges on updated_at DESC, id ASC.
                cur.execute(
                    """
                    UPDATE jobs SET relevance_score=50.0, category='relevant', viewed=0,
                      hidden=0, updated_at=? WHERE id=?
                    """,
                    ("2024-06-01T00:00:00+00:00", a),
                )
                cur.execute(
                    """
                    UPDATE jobs SET relevance_score=50.0, category='relevant', viewed=0,
                      hidden=1, updated_at=? WHERE id=?
                    """,
                    ("2024-05-01T00:00:00+00:00", b),
                )
                cur.execute(
                    """
                    UPDATE jobs SET relevance_score=50.0, category='relevant', viewed=0,
                      hidden=0, updated_at=? WHERE id=?
                    """,
                    ("2024-04-01T00:00:00+00:00", c),
                )
                cur.execute(
                    """
                    UPDATE jobs SET relevance_score=50.0, category='relevant', viewed=0,
                      hidden=1, updated_at=? WHERE id=?
                    """,
                    ("2024-03-01T00:00:00+00:00", d),
                )
                conn.commit()
            finally:
                conn.close()

            sql_rows = get_jobs_by_category_triage_window(
                db_path, "relevant", non_hidden_limit=2, unviewed_only=True
            )
            # Full category listing → Python sort → walk-slice must match SQL window.
            conn = _connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, relevance_score, viewed, hidden, updated_at FROM jobs "
                    "WHERE category='relevant' AND viewed=0"
                )
                py_items = [
                    {
                        "id": r[0],
                        "relevance_score": float(r[1] or 0),
                        "viewed": int(r[2] or 0),
                        "hidden": int(r[3] or 0),
                        "updated_at": r[4] or "",
                    }
                    for r in cur.fetchall()
                ]
            finally:
                conn.close()
            py_sorted = _sort_positions_unviewed_then_score(py_items)
            py_sliced = slice_triage_by_non_hidden_limit(py_sorted, 2)
            self.assertEqual(
                [row["id"] for row in sql_rows],
                [item["id"] for item in py_sliced],
            )
            # Soft-hidden after 2nd non-hidden must stay out.
            self.assertEqual([row["id"] for row in sql_rows], [a, b, c])
            self.assertNotIn(d, [row["id"] for row in sql_rows])

    def test_applied_sort_puts_missing_manual_before_complete(self):
        items = [
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 0,
                "cover_letter": "",
                "viewed": 1,
                "relevance_score": 0.9,
            },
            {"raw_text": "", "cover_letter_requested": 0, "cover_letter": "", "viewed": 0, "relevance_score": 0.1},
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertEqual(sorted_items[0]["raw_text"], "")

    def test_applied_sort_puts_pending_cover_letter_before_complete(self):
        items = [
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 0,
                "cover_letter": "",
                "viewed": 0,
                "relevance_score": 0.2,
            },
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 1,
                "cover_letter": "Dear hiring manager",
                "viewed": 1,
                "relevance_score": 0.9,
            },
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 1,
                "cover_letter": "",
                "viewed": 1,
                "relevance_score": 0.95,
            },
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertFalse(_applied_position_is_complete(sorted_items[0]))
        self.assertEqual(sorted_items[0]["cover_letter"], "")
        self.assertTrue(_applied_position_is_complete(sorted_items[-1]))

    def test_applied_sort_opted_out_cover_letter_uses_score(self):
        items = [
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 0,
                "cover_letter": "",
                "viewed": 0,
                "relevance_score": 0.8,
            },
            {
                "raw_text": MANUAL_APPLIED_RAW_MARKER,
                "cover_letter_requested": 0,
                "cover_letter": "",
                "viewed": 0,
                "relevance_score": 0.3,
            },
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertEqual(sorted_items[0]["relevance_score"], 0.8)

    def test_applied_sort_tiebreaks_by_applied_at_desc(self):
        base = {
            "raw_text": MANUAL_APPLIED_RAW_MARKER,
            "cover_letter_requested": 0,
            "cover_letter": "",
            "viewed": 1,
            "relevance_score": 0.5,
        }
        items = [
            {**base, "applied_at": "2024-01-01T00:00:00+00:00"},
            {**base, "applied_at": "2024-06-01T00:00:00+00:00"},
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertEqual(
            [i["applied_at"] for i in sorted_items],
            ["2024-06-01T00:00:00+00:00", "2024-01-01T00:00:00+00:00"],
        )

    def test_applied_sort_empty_applied_at_after_dated(self):
        base = {
            "raw_text": MANUAL_APPLIED_RAW_MARKER,
            "cover_letter_requested": 0,
            "cover_letter": "",
            "viewed": 1,
            "relevance_score": 0.5,
        }
        items = [
            {**base, "applied_at": ""},
            {**base, "applied_at": "2024-03-01T00:00:00+00:00"},
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertEqual(sorted_items[0]["applied_at"], "2024-03-01T00:00:00+00:00")
        self.assertEqual(sorted_items[1]["applied_at"], "")

    def test_slice_triage_includes_soft_hidden_before_limit(self):
        items = [
            {"id": 1, "hidden": 0},
            {"id": 2, "hidden": 1},
            {"id": 3, "hidden": 0},
            {"id": 4, "hidden": 1},
            {"id": 5, "hidden": 0},
        ]
        sliced = slice_triage_by_non_hidden_limit(items, 2)
        self.assertEqual([i["id"] for i in sliced], [1, 2, 3])

    def test_slice_triage_stops_before_soft_hidden_after_limit(self):
        items = [
            {"id": 1, "hidden": 0},
            {"id": 2, "hidden": 1},
        ]
        sliced = slice_triage_by_non_hidden_limit(items, 1)
        self.assertEqual([i["id"] for i in sliced], [1])

    def test_slice_triage_leading_soft_hidden_included(self):
        items = [
            {"id": 1, "hidden": 1},
            {"id": 2, "hidden": 0},
            {"id": 3, "hidden": 0},
        ]
        sliced = slice_triage_by_non_hidden_limit(items, 1)
        self.assertEqual([i["id"] for i in sliced], [1, 2])


if __name__ == "__main__":
    unittest.main()
