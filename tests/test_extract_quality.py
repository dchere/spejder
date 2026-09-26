"""Tests for extract quality helpers."""

import unittest

from spejder.jobs.parsing.extract_quality import (
    entry_weak_reasons,
    is_strong_entry,
    partition_entries,
    weak_reason_summary,
)


class ExtractQualityTest(unittest.TestCase):
    def test_strong_role_title(self):
        entry = {
            "title": "Senior Python Developer",
            "company": "Acme",
            "source": "LinkedIn",
            "position_link": "https://www.linkedin.com/jobs/view/1",
        }
        self.assertEqual(entry_weak_reasons(entry), [])
        self.assertTrue(is_strong_entry(entry))

    def test_empty_and_cta_titles(self):
        self.assertIn("empty_title", entry_weak_reasons({"title": "  ", "company": "X"}))
        self.assertIn(
            "cta_title",
            entry_weak_reasons({"title": "Apply here", "company": "X"}),
        )

    def test_boilerplate_title(self):
        reasons = entry_weak_reasons(
            {"title": "New jobs match your preferences", "company": "Acme"}
        )
        self.assertIn("boilerplate_title", reasons)

    def test_partition_and_summary(self):
        strong, weak = partition_entries(
            [
                {"title": "Engineer", "company": "A"},
                {"title": "Apply now", "company": "B"},
                {"title": "", "company": "C"},
            ]
        )
        self.assertEqual(len(strong), 1)
        self.assertEqual(len(weak), 2)
        summary = weak_reason_summary(weak)
        self.assertIn("cta_title", summary)
        self.assertIn("empty_title", summary)


if __name__ == "__main__":
    unittest.main()
