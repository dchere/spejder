"""Tests for dashboard rebuild queue behavior."""

import unittest

from spejder.workflows.dashboard import coalesce_rebuild_reasons


class DashboardRebuildQueueTest(unittest.TestCase):
    def test_coalesce_rebuild_reasons_uses_latest_with_queue_count(self):
        self.assertEqual(
            coalesce_rebuild_reasons(["first", "second", "third"]),
            "third (+2 queued)",
        )

    def test_coalesce_rebuild_reasons_empty_returns_blank(self):
        self.assertEqual(coalesce_rebuild_reasons([]), "")


if __name__ == "__main__":
    unittest.main()
