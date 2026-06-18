"""Tests for skills tab row shaping."""

import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import (
    count_jobs_with_skill_links,
    ensure_db,
    set_job_skills,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect
from spejder.extractors.skill_extractor.ui import _build_skills_tab_items


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


class SkillsTabUiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.profile = AppConfig(user_skills=["Rust"])

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_count_jobs_with_skill_links_empty_db(self):
        self.assertEqual(count_jobs_with_skill_links(self.db_path), 0)

    def test_position_pct_zero_when_no_jobs_with_skills(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="learned",
            occurrences_inc=2,
        )

        rows = {row["name"]: row for row in _build_skills_tab_items(self.db_path, self.profile)}

        self.assertEqual(rows["python"]["jobs_with_skills"], 0)
        self.assertEqual(rows["python"]["position_count"], 0)
        self.assertEqual(rows["python"]["position_pct"], 0.0)

    def test_profile_only_skill_has_zero_job_share_with_denominator(self):
        job_id = _insert_job(self.db_path, "https://example.com/only-sql", title="Engineer A")
        set_job_skills(self.db_path, job_id, ["SQL"])

        rows = {row["name"]: row for row in _build_skills_tab_items(self.db_path, self.profile)}

        self.assertEqual(rows["rust"]["position_count"], 0)
        self.assertEqual(rows["rust"]["position_pct"], 0.0)
        self.assertEqual(rows["rust"]["jobs_with_skills"], 1)

    def test_position_pct_reflects_job_share(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="learned",
            occurrences_inc=5,
            weight_inc=5.0,
        )
        job_a = _insert_job(self.db_path, "https://example.com/a", title="Engineer A")
        job_b = _insert_job(self.db_path, "https://example.com/b", title="Engineer B")
        job_c = _insert_job(self.db_path, "https://example.com/c", title="Engineer C")
        set_job_skills(self.db_path, job_a, ["Python"])
        set_job_skills(self.db_path, job_b, ["Python"])
        set_job_skills(self.db_path, job_c, ["SQL"])

        self.assertEqual(count_jobs_with_skill_links(self.db_path), 3)

        rows = {row["name"]: row for row in _build_skills_tab_items(self.db_path, self.profile)}

        self.assertEqual(rows["python"]["position_count"], 2)
        self.assertEqual(rows["python"]["position_pct"], 66.7)
        self.assertEqual(rows["python"]["jobs_with_skills"], 3)
        self.assertEqual(rows["python"]["occurrences"], 5)
        self.assertEqual(rows["rust"]["position_pct"], 0.0)

    def test_rows_default_to_alphabetical_order(self):
        upsert_skill_pattern(self.db_path, name="Zebra", pattern=r"\bZebra\b", source="test")
        upsert_skill_pattern(self.db_path, name="Alpha", pattern=r"\bAlpha\b", source="test")

        names = [row["name"] for row in _build_skills_tab_items(self.db_path, self.profile)]

        self.assertEqual(names, sorted(names, key=str.lower))


if __name__ == "__main__":
    unittest.main()
