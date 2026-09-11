"""Shared skill-key membership across sanitizer, profile toggle, and scoring."""

import unittest
from unittest.mock import MagicMock

from spejder.config import AppConfig
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.jobs.scoring import score_relevance
from spejder.managers.profile_manager import (
    _block_skill_in_profile,
    _remove_skill_from_profile,
    _toggle_exclusive_profile_skill,
    _toggle_profile_skill,
)


class SkillKeyHelperTest(unittest.TestCase):
    def test_collapsed_whitespace_is_the_same_key(self):
        self.assertEqual(
            _normalize_skill_name_key("foo  bar"),
            _normalize_skill_name_key("foo bar"),
        )
        self.assertEqual(_normalize_skill_name_key("Foo  Bar"), "foo bar")


class SanitizerSkillKeyTest(unittest.TestCase):
    def test_unwanted_drops_user_skill_with_extra_spaces(self):
        profile = AppConfig(unwanted_skills=["foo  bar"], user_skills=["foo bar"])
        self.assertEqual(profile.user_skills, [])


class ToggleSkillKeyTest(unittest.TestCase):
    def test_enable_is_noop_when_collapsed_name_already_present(self):
        profile = AppConfig(user_skills=["foo bar"])
        changed = _toggle_profile_skill(profile, "user_skills", "foo  bar", True)
        self.assertFalse(changed)
        self.assertEqual(profile.user_skills, ["foo bar"])

    def test_disable_removes_collapsed_whitespace_match(self):
        profile = AppConfig(user_skills=["foo bar"])
        changed = _toggle_profile_skill(profile, "user_skills", "foo  bar", False)
        self.assertTrue(changed)
        self.assertEqual(profile.user_skills, [])

    def test_enable_stores_collapsed_display_preserving_case(self):
        profile = AppConfig(user_skills=[])
        changed = _toggle_profile_skill(profile, "user_skills", "Foo  Bar", True)
        self.assertTrue(changed)
        self.assertEqual(profile.user_skills, ["Foo Bar"])

    def test_exclusive_toggle_treats_collapsed_names_as_one_entry(self):
        profile = AppConfig(user_skills=["foo bar"], unwanted_skills=[])
        changed, dropped = _toggle_exclusive_profile_skill(
            profile,
            "unwanted_skills",
            "foo  bar",
            True,
            drop_from=("user_skills", "missing_skills_suggestions"),
        )
        self.assertTrue(changed)
        self.assertTrue(dropped)
        self.assertEqual(profile.user_skills, [])
        self.assertEqual(profile.unwanted_skills, ["foo bar"])


class RemoveAndBlockSkillKeyTest(unittest.TestCase):
    def test_remove_deletes_collapsed_whitespace_match(self):
        profile = AppConfig(user_skills=["foo bar"])
        info = _remove_skill_from_profile(profile, "foo  bar")
        self.assertEqual(info["removed"], 1)
        self.assertEqual(profile.user_skills, [])

    def test_block_matches_key_and_stores_collapsed_display(self):
        profile = AppConfig(user_skills=["foo bar"], blocked_skills=["Foo  Bar"])
        info = _block_skill_in_profile(profile, "Foo  Bar")
        self.assertEqual(info["removed"], 1)
        self.assertEqual(info["blocked_added"], 0)
        self.assertEqual(profile.user_skills, [])
        self.assertEqual(profile.blocked_skills, ["Foo Bar"])


class ScorerSkillKeyTest(unittest.TestCase):
    def _profile(self, **overrides) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = []
        profile.unwanted_skills = []
        profile.skill_match_weight = 1.0
        profile.skill_missing_penalty = 0.5
        profile.skill_unwanted_penalty = 0.0
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.0
        for key, value in overrides.items():
            setattr(profile, key, value)
        return profile

    def test_user_skill_with_extra_spaces_matches_cached_skill(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nminimal inbox body",
            self._profile(user_skills=["foo  bar"]),
            skill_patterns=[],
            cached_required_skills=["foo bar"],
        )
        self.assertGreater(score, 0)
        self.assertIn("foo bar", reason)

    def test_cached_skill_with_extra_spaces_matches_user_skill(self):
        score, reason, _, _ = score_relevance(
            "title\ncompany\nminimal inbox body",
            self._profile(user_skills=["foo bar"]),
            skill_patterns=[],
            cached_required_skills=["foo  bar"],
        )
        self.assertGreater(score, 0)
        self.assertIn("foo  bar", reason)


class ExtractorNormalizeStillCleansDisplayTest(unittest.TestCase):
    def test_experience_with_python_becomes_python(self):
        self.assertEqual(_normalize_skill_name("Experience with Python"), "python")

    def test_gates_still_apply_before_key_helper(self):
        self.assertEqual(_normalize_skill_name("a"), "")
        self.assertEqual(
            _normalize_skill_name("one two three four five six"),
            "",
        )


if __name__ == "__main__":
    unittest.main()
