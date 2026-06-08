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
        profile.skill_match_weight = 1.0
        profile.skill_missing_penalty = 0.5
        profile.easy_apply_bonus = 0.0
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
    @patch("spejder.jobs.suggestions.get_job_skills")
    @patch("spejder.jobs.suggestions.get_jobs_for_skill_suggestions")
    def test_aggregates_persisted_job_skills(self, mock_rows, mock_get_skills):
        from spejder.jobs.suggestions import _suggest_missing_skills_from_applied_jobs

        mock_rows.return_value = [(1,), (2,)]
        mock_get_skills.side_effect = lambda _db, job_id: (
            ["kubernetes"] if job_id == 1 else ["terraform"]
        )
        profile = MagicMock(spec=AppConfig)
        profile.user_skills = ["python"]
        profile.blocked_skills = []

        with patch("spejder.jobs.suggestions._blocked_skill_keys", return_value=set()):
            result = _suggest_missing_skills_from_applied_jobs("jobs.db", profile, max_items=10)

        self.assertIn("kubernetes", result)
        self.assertIn("terraform", result)
        self.assertNotIn("python", result)


if __name__ == "__main__":
    unittest.main()
