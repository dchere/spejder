"""Tests for applied-pipeline company keys and applied-company scoring bonus."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_applied_pipeline_company_keys,
    set_job_applied,
    set_job_interview_stopped,
    set_job_on_interview,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.db.deduplication_utils import (
    _canonicalize_company_for_dedupe,
    _normalize_company_key,
)
from spejder.jobs.scoring import apply_relevance, score_relevance


def _company_key(company: str) -> str:
    return _normalize_company_key(_canonicalize_company_for_dedupe(company))


def _insert_job(
    db_path: str,
    link: str,
    *,
    company: str = "Acme",
    title: str = "Engineer",
    applied: bool = False,
) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": company,
            "title": title,
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    if applied:
        set_job_applied(db_path, job_id, True)
    return job_id


def _job_relevance(db_path: str, job_id: int) -> tuple[float, str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT relevance_score, relevance_reason FROM jobs WHERE id=?",
            (job_id,),
        )
        row = cur.fetchone()
        return float(row[0] or 0.0), str(row[1] or "")
    finally:
        conn.close()


class AppliedPipelineCompanyKeysTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_applied_only_included(self):
        _insert_job(
            self.db_path,
            "https://example.com/applied",
            company="Foo Corp",
            applied=True,
        )
        keys = get_applied_pipeline_company_keys(self.db_path)
        self.assertEqual(keys, {_company_key("Foo Corp")})

    def test_interview_only_included(self):
        job_id = _insert_job(
            self.db_path,
            "https://example.com/interview",
            company="Bar Inc",
            applied=True,
        )
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        keys = get_applied_pipeline_company_keys(self.db_path)
        self.assertEqual(keys, {_company_key("Bar Inc")})

    def test_stopped_only_excluded(self):
        job_id = _insert_job(
            self.db_path,
            "https://example.com/stopped",
            company="Baz Ltd",
            applied=True,
        )
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertEqual(get_applied_pipeline_company_keys(self.db_path), set())

    def test_applied_plus_stopped_same_company_excluded(self):
        _insert_job(
            self.db_path,
            "https://example.com/applied-foo",
            company="Foo",
            title="Role A",
            applied=True,
        )
        stopped_id = _insert_job(
            self.db_path,
            "https://example.com/stopped-foo",
            company="Foo",
            title="Role B",
            applied=True,
        )
        self.assertTrue(set_job_interview_stopped(self.db_path, stopped_id, True))
        self.assertEqual(get_applied_pipeline_company_keys(self.db_path), set())

    def test_applied_plus_stopped_alias_companies_excluded(self):
        _insert_job(
            self.db_path,
            "https://example.com/applied-foo-as",
            company="Foo A/S",
            title="Role A",
            applied=True,
        )
        stopped_id = _insert_job(
            self.db_path,
            "https://example.com/stopped-foo",
            company="Foo",
            title="Role B",
            applied=True,
        )
        self.assertTrue(set_job_interview_stopped(self.db_path, stopped_id, True))
        self.assertEqual(get_applied_pipeline_company_keys(self.db_path), set())

    def test_applied_plus_stopped_alias_companies_excluded_reverse(self):
        _insert_job(
            self.db_path,
            "https://example.com/applied-foo",
            company="Foo",
            title="Role A",
            applied=True,
        )
        stopped_id = _insert_job(
            self.db_path,
            "https://example.com/stopped-foo-as",
            company="Foo A/S",
            title="Role B",
            applied=True,
        )
        self.assertTrue(set_job_interview_stopped(self.db_path, stopped_id, True))
        self.assertEqual(get_applied_pipeline_company_keys(self.db_path), set())

    def test_blank_company_ignored(self):
        _insert_job(
            self.db_path,
            "https://example.com/blank",
            company="   ",
            applied=True,
        )
        self.assertEqual(get_applied_pipeline_company_keys(self.db_path), set())

    def test_noise_tokens_collide(self):
        _insert_job(
            self.db_path,
            "https://example.com/foo-as",
            company="Foo A/S",
            applied=True,
        )
        keys = get_applied_pipeline_company_keys(self.db_path)
        self.assertEqual(keys, {_company_key("Foo")})
        self.assertEqual(_company_key("Foo A/S"), _company_key("Foo"))

    def test_part_of_parent_keys_as_parent(self):
        _insert_job(
            self.db_path,
            "https://example.com/part-of",
            company="Subco, part of Parent Group",
            applied=True,
        )
        keys = get_applied_pipeline_company_keys(self.db_path)
        self.assertEqual(keys, {_company_key("Parent Group")})
        self.assertEqual(
            _company_key("Subco, part of Parent Group"),
            _company_key("Parent Group"),
        )


class AppliedCompanyBonusScoreTest(unittest.TestCase):
    def _profile(self, **overrides) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = []
        profile.skill_match_weight = 1.0
        profile.skill_missing_penalty = 0.5
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.75
        for key, value in overrides.items():
            setattr(profile, key, value)
        return profile

    def test_company_in_set_adds_bonus(self):
        profile = self._profile()
        keys = {_company_key("Acme")}
        score, reason, _, _ = score_relevance(
            "title\nAcme\nbody",
            profile,
            skill_patterns=[],
            company="Acme",
            applied_company_keys=keys,
        )
        self.assertEqual(score, 0.75)
        self.assertIn("applied_company=True", reason)
        self.assertIn("applied_company_bonus=0.75", reason)

    def test_company_not_in_set_unchanged(self):
        profile = self._profile()
        score, reason, _, _ = score_relevance(
            "title\nOther\nbody",
            profile,
            skill_patterns=[],
            company="Other",
            applied_company_keys={_company_key("Acme")},
        )
        self.assertEqual(score, 0.0)
        self.assertIn("applied_company=False", reason)
        self.assertIn("applied_company_bonus=0", reason)

    def test_none_or_empty_keys_no_bonus(self):
        profile = self._profile()
        for keys in (None, set()):
            score, reason, _, _ = score_relevance(
                "title\nAcme\nbody",
                profile,
                skill_patterns=[],
                company="Acme",
                applied_company_keys=keys,
            )
            self.assertEqual(score, 0.0)
            self.assertIn("applied_company=False", reason)

    def test_bonus_zero_disables(self):
        profile = self._profile(applied_company_bonus=0.0)
        score, reason, _, _ = score_relevance(
            "title\nAcme\nbody",
            profile,
            skill_patterns=[],
            company="Acme",
            applied_company_keys={_company_key("Acme")},
        )
        self.assertEqual(score, 0.0)
        self.assertIn("applied_company=True", reason)
        self.assertIn("applied_company_bonus=0", reason)

    @patch("spejder.jobs.scoring._has_easy_apply_signal", return_value=True)
    def test_stacks_with_easy_apply(self, _mock_easy):
        profile = self._profile(easy_apply_bonus=0.75, applied_company_bonus=0.75)
        score, reason, _, _ = score_relevance(
            "title\nAcme\neasy apply body",
            profile,
            skill_patterns=[],
            source="LinkedIn",
            position_link="https://www.linkedin.com/jobs/view/1",
            company="Acme",
            applied_company_keys={_company_key("Acme")},
        )
        self.assertEqual(score, 1.5)
        self.assertIn("easy_apply=True", reason)
        self.assertIn("applied_company=True", reason)


class AppliedCompanyBonusWiringTest(unittest.TestCase):
    """DB-backed path: keys load → score_relevance gets company → persists bonus."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _profile(self) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = []
        profile.blocked_skills = []
        profile.known_skill_patterns = []
        profile.skill_match_weight = 0.0
        profile.skill_missing_penalty = 0.0
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.75
        return profile

    def test_apply_relevance_bumps_sibling_at_alias_company(self):
        _insert_job(
            self.db_path,
            "https://example.com/applied-foo-as",
            company="Foo A/S",
            title="Applied Role",
            applied=True,
        )
        sibling_id = _insert_job(
            self.db_path,
            "https://example.com/sibling-foo",
            company="Foo",
            title="Sibling Role",
            applied=False,
        )

        scored, _ = apply_relevance(self.db_path, self._profile())
        self.assertGreaterEqual(scored, 1)

        score, reason = _job_relevance(self.db_path, sibling_id)
        self.assertEqual(score, 0.75)
        self.assertIn("applied_company=True", reason)
        self.assertIn("applied_company_bonus=0.75", reason)


if __name__ == "__main__":
    unittest.main()
