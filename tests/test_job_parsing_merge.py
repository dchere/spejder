"""Direct tests for first-wins job entry field merge."""

import unittest

from spejder.jobs.parsing.merge import _ENTRY_FIELD_KEYS, merge_entry_fields


class MergeEntryFieldsTest(unittest.TestCase):
    def test_first_non_empty_wins(self):
        merged = merge_entry_fields(
            {"title": "First title", "company": ""},
            {"title": "Later title", "company": "Acme"},
        )
        self.assertEqual(merged["title"], "First title")
        self.assertEqual(merged["company"], "Acme")

    def test_later_maps_do_not_overwrite(self):
        merged = merge_entry_fields(
            {"title": "Keep", "place": "Aarhus"},
            {"title": "Overwrite", "place": "Copenhagen", "source": "later"},
        )
        self.assertEqual(merged["title"], "Keep")
        self.assertEqual(merged["place"], "Aarhus")
        self.assertEqual(merged["source"], "later")

    def test_empty_and_missing_maps_are_skipped(self):
        merged = merge_entry_fields(
            None,
            {},
            {"title": ""},
            {"title": "Filled", "work_type": "full-time"},
        )
        self.assertEqual(merged["title"], "Filled")
        self.assertEqual(merged["work_type"], "full-time")

    def test_only_entry_field_keys_are_copied(self):
        merged = merge_entry_fields(
            {
                "title": "Role",
                "company": "Acme",
                "place": "Aarhus",
                "work_type": "full-time",
                "raw_text": "body",
                "source": "portal",
                "extra": "ignored",
                "link": "https://example.test/job",
            }
        )
        self.assertEqual(
            merged,
            {
                "title": "Role",
                "company": "Acme",
                "place": "Aarhus",
                "work_type": "full-time",
                "raw_text": "body",
                "source": "portal",
            },
        )
        self.assertEqual(set(merged), set(_ENTRY_FIELD_KEYS))


if __name__ == "__main__":
    unittest.main()
