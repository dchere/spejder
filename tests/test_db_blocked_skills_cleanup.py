"""Tests for cleanup_blocked_skills_from_db."""

import os
import tempfile
import unittest

from spejder.db import (
    cleanup_blocked_skills_from_db,
    delete_skill_from_db,
    ensure_db,
    get_job_skills,
    get_skill_patterns,
    set_job_skills,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect


def _insert_job(db_path: str, link: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": "Engineer",
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


class BlockedSkillsCleanupTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_cleanup_removes_patterns_and_job_links(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="test",
        )
        upsert_skill_pattern(
            self.db_path,
            name="SQL",
            pattern=r"\bSQL\b",
            source="test",
        )
        job_id = _insert_job(self.db_path, "https://example.com/python-job")
        set_job_skills(self.db_path, job_id, ["Python", "SQL"])

        stats = cleanup_blocked_skills_from_db(self.db_path, ["Python"])

        self.assertEqual(stats["skills_processed"], 1)
        self.assertEqual(stats["skill_rows_deleted"], 1)
        self.assertEqual(stats["job_skill_links_deleted"], 1)
        self.assertEqual(get_job_skills(self.db_path, job_id), ["SQL"])
        pattern_names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("Python", pattern_names)
        self.assertIn("SQL", pattern_names)

    def test_cleanup_dedupes_blocked_skills(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="test",
        )
        job_id = _insert_job(self.db_path, "https://example.com/dedupe-job")
        set_job_skills(self.db_path, job_id, ["Python"])

        stats = cleanup_blocked_skills_from_db(
            self.db_path,
            ["Python", "python", "  PYTHON  "],
        )

        self.assertEqual(stats["skills_processed"], 1)
        self.assertEqual(stats["skill_rows_deleted"], 1)
        self.assertEqual(stats["job_skill_links_deleted"], 1)
        self.assertEqual(get_job_skills(self.db_path, job_id), [])

    def test_cleanup_empty_blocked_list(self):
        stats = cleanup_blocked_skills_from_db(self.db_path, [])
        self.assertEqual(
            stats,
            {
                "skills_processed": 0,
                "skill_rows_deleted": 0,
                "job_skill_links_deleted": 0,
                "affected_job_ids": [],
            },
        )

    def test_delete_skill_from_db_still_available_for_single_skill(self):
        upsert_skill_pattern(
            self.db_path,
            name="Rust",
            pattern=r"\bRust\b",
            source="test",
        )
        job_id = _insert_job(self.db_path, "https://example.com/rust-job")
        set_job_skills(self.db_path, job_id, ["Rust"])

        deleted = delete_skill_from_db(self.db_path, "Rust")

        self.assertEqual(deleted["skill_rows_deleted"], 1)
        self.assertEqual(deleted["job_skill_links_deleted"], 1)
        self.assertEqual(deleted["affected_job_ids"], [job_id])
        self.assertEqual(get_job_skills(self.db_path, job_id), [])


if __name__ == "__main__":
    unittest.main()
