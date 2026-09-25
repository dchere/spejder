"""Tests for scoring and skill materialization pipeline."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.jobs.scoring import score_relevance


class ScoreRelevanceCachedSkillsTest(unittest.TestCase):
    def _profile(self) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = ["python", "docker"]
        profile.unwanted_skills = []
        profile.skill_match_weight = 1.0
        profile.skill_missing_penalty = 0.5
        profile.skill_unwanted_penalty = 0.0
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.0
        return profile

    def test_uses_cached_skills_when_provided(self):
        profile = self._profile()
        _, reason, _, _ = score_relevance(
            "title\ncompany\nminimal inbox body",
            profile,
            skill_patterns=[],
            cached_required_skills=["python", "kubernetes"],
        )
        self.assertIn("skill_source=cached", reason)
        self.assertIn("python", reason)
        self.assertIn("kubernetes", reason)

    def test_falls_back_to_regex_without_cache(self):
        profile = self._profile()
        text = "Qualifications: python required."
        _, reason, _, _ = score_relevance(
            text,
            profile,
            skill_patterns=[],
            cached_required_skills=None,
        )
        self.assertIn("skill_source=regex", reason)


class SuggestMissingSkillsFromCacheTest(unittest.TestCase):
    @patch("spejder.jobs.suggestions.get_job_skills_filtered")
    @patch("spejder.jobs.suggestions.get_jobs_for_skill_suggestions")
    def test_aggregates_persisted_job_skills(self, mock_rows, mock_get_skills):
        from spejder.jobs.suggestions import _suggest_missing_skills_from_applied_jobs

        mock_rows.return_value = [(1,), (2,)]
        mock_get_skills.side_effect = lambda _db, job_id, _profile: (
            ["kubernetes"] if job_id == 1 else ["terraform"]
        )
        profile = MagicMock(spec=AppConfig)
        profile.user_skills = ["python"]
        profile.unwanted_skills = []
        profile.blocked_skills = []

        with patch("spejder.jobs.suggestions._blocked_skill_keys", return_value=set()):
            result = _suggest_missing_skills_from_applied_jobs("jobs.db", profile, max_items=10)

        self.assertIn("kubernetes", result)
        self.assertIn("terraform", result)
        self.assertNotIn("python", result)


class ScoreRelevanceUnwantedSkillsTest(unittest.TestCase):
    def _profile(self, **overrides) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = ["python"]
        profile.unwanted_skills = ["sales"]
        profile.skill_match_weight = 1.0
        profile.skill_missing_penalty = 0.15
        profile.skill_unwanted_penalty = 1.2
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.0
        for key, value in overrides.items():
            setattr(profile, key, value)
        return profile

    def test_unwanted_hit_subtracts_penalty(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(),
            skill_patterns=[],
            cached_required_skills=["sales"],
        )
        self.assertEqual(score, -1.2)
        self.assertIn("unwanted_skills=['sales']", reason)
        self.assertIn("skill_unwanted_penalty=1.2", reason)
        self.assertIn("missing_skills=[]", reason)

    def test_match_missing_and_unwanted_combined(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(),
            skill_patterns=[],
            cached_required_skills=["python", "sales", "k8s"],
        )
        self.assertAlmostEqual(score, -0.35)
        self.assertIn("matched_skills=['python']", reason)
        self.assertIn("missing_skills=['k8s']", reason)
        self.assertIn("unwanted_skills=['sales']", reason)

    def test_two_unwanted_hits_apply_penalty_twice(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(unwanted_skills=["sales", "cobol"]),
            skill_patterns=[],
            cached_required_skills=["sales", "cobol"],
        )
        self.assertEqual(score, -2.4)
        self.assertIn("unwanted_skills=['cobol', 'sales']", reason)

    def test_penalty_zero_is_noop(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(skill_unwanted_penalty=0.0),
            skill_patterns=[],
            cached_required_skills=["sales"],
        )
        self.assertEqual(score, 0.0)
        self.assertIn("unwanted_skills=['sales']", reason)
        self.assertIn("skill_unwanted_penalty=0", reason)

    def test_unwanted_fires_with_empty_user_skills(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(user_skills=[]),
            skill_patterns=[],
            cached_required_skills=["sales"],
        )
        self.assertEqual(score, -1.2)
        self.assertIn("unwanted_skills=['sales']", reason)

    def test_unwanted_wins_over_match(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nbody",
            self._profile(user_skills=["sales"], unwanted_skills=["sales"]),
            skill_patterns=[],
            cached_required_skills=["sales"],
        )
        self.assertEqual(score, -1.2)
        self.assertIn("matched_skills=[]", reason)
        self.assertIn("unwanted_skills=['sales']", reason)


class SuggestMissingSkillsSkipUnwantedTest(unittest.TestCase):
    @patch("spejder.jobs.suggestions.get_job_skills_filtered")
    @patch("spejder.jobs.suggestions.get_jobs_for_skill_suggestions")
    def test_skips_unwanted_skills(self, mock_rows, mock_get_skills):
        from spejder.jobs.suggestions import _suggest_missing_skills_from_applied_jobs

        mock_rows.return_value = [(1,)]
        mock_get_skills.return_value = ["kubernetes", "sales"]
        profile = MagicMock(spec=AppConfig)
        profile.user_skills = ["python"]
        profile.unwanted_skills = ["sales"]
        profile.blocked_skills = []

        with patch("spejder.jobs.suggestions._blocked_skill_keys", return_value=set()):
            result = _suggest_missing_skills_from_applied_jobs("jobs.db", profile, max_items=10)

        self.assertIn("kubernetes", result)
        self.assertNotIn("sales", result)


if __name__ == "__main__":
    unittest.main()
