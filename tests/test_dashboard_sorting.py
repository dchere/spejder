"""Tests for dashboard job list sorting."""

import unittest

from spejder.core import MANUAL_APPLIED_RAW_MARKER
from spejder.managers.dashboard_sorting import (
    _applied_position_is_complete,
    _sort_applied_positions,
    _sort_positions_unviewed_then_score,
)


class DashboardSortingTest(unittest.TestCase):
    def test_unviewed_first_then_higher_score(self):
        items = [
            {"viewed": 1, "relevance_score": 0.9},
            {"viewed": 0, "relevance_score": 0.1},
            {"viewed": 0, "relevance_score": 0.8},
        ]
        sorted_items = _sort_positions_unviewed_then_score(items)
        self.assertEqual([i["relevance_score"] for i in sorted_items], [0.8, 0.1, 0.9])
        self.assertEqual(sorted_items[0]["viewed"], 0)

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


if __name__ == "__main__":
    unittest.main()
