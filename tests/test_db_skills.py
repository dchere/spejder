"""Tests for skill_patterns DB helpers."""

import os
import tempfile
import unittest

from spejder.db import ensure_db, get_skill_patterns, upsert_skill_pattern


class DbSkillsTest(unittest.TestCase):
    def test_get_skill_patterns_includes_created_at_after_upsert(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            upsert_skill_pattern(
                db_path,
                name="Python",
                pattern=r"\bPython\b",
                source="test",
            )

            rows = get_skill_patterns(db_path, enabled_only=False)
            self.assertEqual(len(rows), 1)
            created_at = rows[0].get("created_at", "")
            self.assertIn("created_at", rows[0])
            self.assertTrue(created_at)
            self.assertRegex(created_at, r"^\d{4}-\d{2}-\d{2}T")


if __name__ == "__main__":
    unittest.main()
