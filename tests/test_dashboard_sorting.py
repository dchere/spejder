"""Tests for dashboard job list sorting."""

import unittest

from spejder.core import MANUAL_APPLIED_RAW_MARKER
from spejder.managers.dashboard_sorting import (
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

    def test_applied_sort_puts_missing_manual_before_done(self):
        items = [
            {"raw_text": MANUAL_APPLIED_RAW_MARKER, "viewed": 1, "relevance_score": 0.9},
            {"raw_text": "", "viewed": 0, "relevance_score": 0.1},
        ]
        sorted_items = _sort_applied_positions(items)
        self.assertEqual(sorted_items[0]["raw_text"], "")


if __name__ == "__main__":
    unittest.main()
