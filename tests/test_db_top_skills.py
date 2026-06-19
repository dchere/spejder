"""Tests for get_top_skills_by_job_links."""

import os
import tempfile
import unittest

from spejder.db import (
    ensure_db,
    get_top_skills_by_job_links,
    set_job_skills,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect


def _insert_job(db_path: str, link: str, title: str = "Engineer") -> int:
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


class TopSkillsByJobLinksTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self._seed()

    def tearDown(self):
        self._tmpdir.cleanup()

    def _seed(self):
        for name in ("Python", "SQL", "Java", "BlockedPhrase"):
            upsert_skill_pattern(
                self.db_path,
                name=name,
                pattern=rf"\b{name}\b",
                source="test",
                occurrences_inc=1,
            )
        upsert_skill_pattern(
            self.db_path,
            name="Rust",
            pattern=r"\bRust\b",
            source="test",
            occurrences_inc=5,
        )

        link_counts = {
            "Python": 3,
            "SQL": 2,
            "Java": 1,
            "BlockedPhrase": 2,
        }
        idx = 0
        for skill, count in link_counts.items():
            for _ in range(count):
                job_id = _insert_job(
                    self.db_path,
                    f"https://example.com/job-{idx}",
                    title=f"Engineer {idx}",
                )
                set_job_skills(self.db_path, job_id, [skill])
                idx += 1

    def test_limit_returns_top_by_link_count(self):
        result = get_top_skills_by_job_links(
            self.db_path,
            limit=3,
            exclude_keys={"BlockedPhrase"},
        )
        self.assertEqual(result, ["Python", "SQL", "Java"])

    def test_excludes_skills_without_job_links(self):
        result = get_top_skills_by_job_links(self.db_path, limit=10)
        self.assertNotIn("Rust", result)

    def test_exclude_keys_removes_named_skills(self):
        result = get_top_skills_by_job_links(
            self.db_path,
            limit=10,
            exclude_keys={"BlockedPhrase"},
        )
        self.assertNotIn("BlockedPhrase", result)
        self.assertEqual(result[:3], ["Python", "SQL", "Java"])

    def test_zero_limit_returns_empty(self):
        self.assertEqual(get_top_skills_by_job_links(self.db_path, limit=0), [])

    def test_excludes_skills_with_zero_occurrences_despite_job_links(self):
        upsert_skill_pattern(
            self.db_path,
            name="PhantomSkill",
            pattern=r"\bPhantomSkill\b",
            source="test",
            occurrences_inc=0,
        )
        job_id = _insert_job(
            self.db_path,
            "https://example.com/phantom",
            title="Phantom Engineer",
        )
        set_job_skills(self.db_path, job_id, ["PhantomSkill"])

        result = get_top_skills_by_job_links(self.db_path, limit=10)
        self.assertNotIn("PhantomSkill", result)

    def test_equal_link_count_orders_alphabetically(self):
        for name in ("Alpha", "Beta"):
            upsert_skill_pattern(
                self.db_path,
                name=name,
                pattern=rf"\b{name}\b",
                source="test",
                occurrences_inc=1,
            )
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/tie-{name.lower()}",
                title=f"Tie {name}",
            )
            set_job_skills(self.db_path, job_id, [name])

        result = get_top_skills_by_job_links(self.db_path, limit=10)
        alpha_idx = result.index("Alpha")
        beta_idx = result.index("Beta")
        self.assertLess(alpha_idx, beta_idx)


if __name__ == "__main__":
    unittest.main()
