"""Tests for skill-pattern learning score set-from-evidence (not additive)."""

import os
import tempfile
import unittest
from collections import Counter
from unittest import mock

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_skill_patterns,
    reconcile_skill_pattern_learning_scores,
    set_job_applied,
    set_job_skills,
    update_jobs_relevance,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect
from spejder.extractors.skill_extractor.learning import (
    _cap_learning_scores,
    _learn_skill_patterns_from_positions,
    _select_learning_rows,
)


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


def _occurrences(db_path: str, name: str) -> int:
    rows = {
        (row["name"] or "").strip().lower(): int(row.get("occurrences", 0) or 0)
        for row in get_skill_patterns(db_path, enabled_only=False)
    }
    return rows.get(name.strip().lower(), -1)


class ReconcileLearningScoresTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_reconcile_sets_absolute_and_zeros_others(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="learned",
            occurrences_inc=99,
            weight_inc=99.0,
        )
        upsert_skill_pattern(
            self.db_path,
            name="SQL",
            pattern=r"\bSQL\b",
            source="learned",
            occurrences_inc=40,
            weight_inc=40.0,
        )

        result = reconcile_skill_pattern_learning_scores(
            self.db_path, {"Python": 6, "Rust": 3}
        )

        self.assertEqual(_occurrences(self.db_path, "Python"), 6)
        self.assertEqual(_occurrences(self.db_path, "SQL"), 0)
        self.assertEqual(result["patterns_scored"], 1)

        rows = {
            (r["name"] or "").lower(): r
            for r in get_skill_patterns(self.db_path, enabled_only=False)
        }
        self.assertEqual(float(rows["python"]["weight"]), 6.0)
        self.assertEqual(float(rows["sql"]["weight"]), 0.0)


class SkillLearningSetFromEvidenceTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.profile = AppConfig(
            skill_learning_max_positions=180,
            skill_learning_min_occurrences=3,
            skill_learning_max_new_patterns=20,
            skill_learning_applied_weight=3,
            known_skill_patterns=[],
            blocked_skills=[],
        )
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="profile_seed",
            occurrences_inc=0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_second_pass_does_not_double_count(self):
        job_id = _insert_job(self.db_path, "https://example.com/applied-py", title="Applied Py")
        set_job_applied(self.db_path, job_id, True)
        set_job_skills(self.db_path, job_id, ["Python"])

        first = _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(first["considered_positions"], 1)
        self.assertEqual(_occurrences(self.db_path, "Python"), 3)

        second = _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(second["considered_positions"], 1)
        self.assertEqual(
            _occurrences(self.db_path, "Python"),
            3,
            "second pass must set current batch score, not add again",
        )

    def test_learned_reflects_current_batch_after_evidence_change(self):
        job_a = _insert_job(self.db_path, "https://example.com/a", title="Engineer A")
        job_b = _insert_job(self.db_path, "https://example.com/b", title="Engineer B")
        set_job_applied(self.db_path, job_a, True)
        set_job_applied(self.db_path, job_b, True)
        set_job_skills(self.db_path, job_a, ["Python"])
        set_job_skills(self.db_path, job_b, ["Python"])

        _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Python"), 6)

        # Remove Python from one applied job — current evidence drops.
        set_job_skills(self.db_path, job_b, ["SQL"])
        _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Python"), 3)

    def test_first_pass_resets_inflated_additive_totals(self):
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="learned",
            occurrences_inc=90,
            weight_inc=90.0,
        )
        job_id = _insert_job(self.db_path, "https://example.com/inflated", title="Inflated")
        set_job_applied(self.db_path, job_id, True)
        set_job_skills(self.db_path, job_id, ["Python"])

        _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Python"), 3)

    def test_sets_score_on_detected_pattern_without_double_count(self):
        # set_job_skills inserts a source=detected row; learning reconciles it
        # (does not count as a "new" pattern admission).
        job_id = _insert_job(
            self.db_path, "https://example.com/new-skill", title="Terraform Role"
        )
        set_job_applied(self.db_path, job_id, True)
        set_job_skills(self.db_path, job_id, ["Terraform"])

        result = _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(result["new_skill_patterns"], 0)
        self.assertEqual(_occurrences(self.db_path, "Terraform"), 3)

        _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Terraform"), 3)

    def test_admits_brand_new_name_at_current_batch_score(self):
        # Skill appears in counts via filtered read without a pre-existing row:
        # seed job skills as a name that we then delete the pattern for… instead
        # put the skill only on the job by mocking filtered skills is heavy;
        # use a pattern that learning can admit after we remove detected row.
        job_id = _insert_job(
            self.db_path, "https://example.com/brand-new", title="Brand New Role"
        )
        set_job_applied(self.db_path, job_id, True)
        set_job_skills(self.db_path, job_id, ["Ansible"])
        # Drop the auto-detected pattern row so learning admits it as new.
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", ("ansible",))
            skill_id = cur.fetchone()[0]
            cur.execute("DELETE FROM skill_patterns WHERE id=?", (skill_id,))
            # Keep job_skills link? learning reads filtered skills from job_skills
            # joined to patterns — link alone is not enough. Patch filtered read.
            conn.commit()
        finally:
            conn.close()

        with mock.patch(
            "spejder.extractors.skill_extractor.learning.get_job_skills_filtered",
            return_value=["Ansible"],
        ):
            result = _learn_skill_patterns_from_positions(self.db_path, self.profile)

        self.assertEqual(result["new_skill_patterns"], 1)
        self.assertEqual(_occurrences(self.db_path, "Ansible"), 3)

        with mock.patch(
            "spejder.extractors.skill_extractor.learning.get_job_skills_filtered",
            return_value=["Ansible"],
        ):
            _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Ansible"), 3)

    def test_relevant_weight_one_contributes(self):
        for i in range(3):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/rel-{i}",
                title=f"Relevant Engineer {i}",
            )
            update_jobs_relevance(
                self.db_path,
                [(job_id, 1.0, "test", 1, "relevant")],
            )
            set_job_skills(self.db_path, job_id, ["Kubernetes"])

        result = _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(result["new_skill_patterns"], 0)
        self.assertEqual(_occurrences(self.db_path, "Kubernetes"), 3)


class SelectLearningRowsTest(unittest.TestCase):
    def test_reserves_relevant_slice_when_applied_would_fill_cap(self):
        applied = [{"id": i} for i in range(1, 201)]
        relevant = [{"id": i} for i in range(1001, 1041)]
        rows = _select_learning_rows(applied, relevant, max_positions=180)
        weights = [w for _, w in rows]
        self.assertEqual(len(rows), 180)
        self.assertEqual(weights.count(1), 30)
        self.assertEqual(weights.count(3), 150)

    def test_uses_all_when_under_cap(self):
        applied = [{"id": 1}, {"id": 2}]
        relevant = [{"id": 3}]
        rows = _select_learning_rows(applied, relevant, max_positions=180)
        self.assertEqual([(r["id"], w) for r, w in rows], [(1, 3), (2, 3), (3, 1)])

    def test_applied_weight_is_configurable(self):
        applied = [{"id": 1}, {"id": 2}]
        relevant = [{"id": 3}]
        rows = _select_learning_rows(
            applied, relevant, max_positions=180, applied_weight=5
        )
        self.assertEqual([(r["id"], w) for r, w in rows], [(1, 5), (2, 5), (3, 1)])


class AppliedWeightConfigTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.profile = AppConfig(
            skill_learning_max_positions=180,
            skill_learning_min_occurrences=3,
            skill_learning_max_new_patterns=20,
            skill_learning_applied_weight=5,
            known_skill_patterns=[],
            blocked_skills=[],
        )
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="profile_seed",
            occurrences_inc=0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_learn_uses_profile_applied_weight(self):
        job_id = _insert_job(self.db_path, "https://example.com/aw", title="Applied W")
        set_job_applied(self.db_path, job_id, True)
        set_job_skills(self.db_path, job_id, ["Python"])

        _learn_skill_patterns_from_positions(self.db_path, self.profile)
        self.assertEqual(_occurrences(self.db_path, "Python"), 5)


class LearningScoreCapTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        upsert_skill_pattern(
            self.db_path,
            name="Python",
            pattern=r"\bPython\b",
            source="profile_seed",
            occurrences_inc=0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_cap_helper_clamps_when_enabled(self):
        raw = Counter({"Python": 12, "SQL": 2})
        self.assertEqual(dict(_cap_learning_scores(raw, 0)), {"Python": 12, "SQL": 2})
        self.assertEqual(dict(_cap_learning_scores(raw, 5)), {"Python": 5, "SQL": 2})
        self.assertEqual(dict(_cap_learning_scores(raw, -1)), {"Python": 12, "SQL": 2})

    def test_learn_caps_reconciled_score(self):
        profile = AppConfig(
            skill_learning_max_positions=180,
            skill_learning_min_occurrences=3,
            skill_learning_max_new_patterns=20,
            skill_learning_applied_weight=3,
            skill_learning_score_cap=4,
            known_skill_patterns=[],
            blocked_skills=[],
        )
        for i in range(3):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/cap-{i}",
                title=f"Cap Job {i}",
            )
            set_job_applied(self.db_path, job_id, True)
            set_job_skills(self.db_path, job_id, ["Python"])

        _learn_skill_patterns_from_positions(self.db_path, profile)
        # Uncapped batch would be 9 (3 applied × weight 3); cap clamps to 4.
        self.assertEqual(_occurrences(self.db_path, "Python"), 4)

    def test_zero_cap_disables_clamp(self):
        profile = AppConfig(
            skill_learning_max_positions=180,
            skill_learning_min_occurrences=3,
            skill_learning_max_new_patterns=20,
            skill_learning_applied_weight=3,
            skill_learning_score_cap=0,
            known_skill_patterns=[],
            blocked_skills=[],
        )
        for i in range(2):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/nocap-{i}",
                title=f"No Cap {i}",
            )
            set_job_applied(self.db_path, job_id, True)
            set_job_skills(self.db_path, job_id, ["Python"])

        _learn_skill_patterns_from_positions(self.db_path, profile)
        self.assertEqual(_occurrences(self.db_path, "Python"), 6)


if __name__ == "__main__":
    unittest.main()
