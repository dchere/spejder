"""Behavioral tests for skill_extractor heuristics."""

import unittest

from spejder.extractors.skill_extractor.extraction import _extract_skills_fallback
from spejder.extractors.skill_extractor.filtering import (
    _filter_blocked_skill_names,
    _passes_phrase_quality,
    _skill_cleanup_reason,
)
from spejder.extractors.skill_extractor.utils import _profile_skill_pattern_fields


class SkillCleanupReasonTest(unittest.TestCase):
    def test_repeated_single_letter_is_generic_term(self):
        self.assertEqual(_skill_cleanup_reason("aa", "learned", set()), "generic term")

    def test_valid_skill_not_flagged(self):
        self.assertEqual(_skill_cleanup_reason("python", "learned", set()), "")

    def test_profile_source_protected(self):
        self.assertEqual(_skill_cleanup_reason("junk skill", "profile", set()), "")


class PassesPhraseQualityTest(unittest.TestCase):
    def test_rejects_pronoun_led_fragment(self):
        self.assertFalse(_passes_phrase_quality("our team culture"))

    def test_accepts_concrete_skill(self):
        self.assertTrue(_passes_phrase_quality("python"))

    def test_rejects_stopword_only_phrase(self):
        self.assertFalse(_passes_phrase_quality("team and company"))


class ExtractSkillsFallbackTest(unittest.TestCase):
    def test_matches_known_pattern(self):
        patterns = [("Python", r"\bpython\b")]
        text = "Requirements: Python and SQL experience required."
        skills = _extract_skills_fallback(text, patterns, limit=5)
        self.assertIn("Python", skills)

    def test_extracts_from_skills_section(self):
        patterns = []
        text = "Qualifications: kubernetes, docker, and terraform."
        skills = _extract_skills_fallback(text, patterns, limit=5)
        self.assertIn("kubernetes", skills)
        self.assertIn("docker", skills)

    def test_empty_text_returns_empty(self):
        self.assertEqual(_extract_skills_fallback("", []), [])


class FilterBlockedSkillNamesTest(unittest.TestCase):
    def test_removes_blocked_skills(self):
        from unittest.mock import MagicMock

        profile = MagicMock()
        profile.blocked_skills = ["sql"]
        result = _filter_blocked_skill_names(["python", "sql", "docker"], profile)
        self.assertEqual(result, ["python", "docker"])


class ProfileSkillPatternFieldsTest(unittest.TestCase):
    def test_reads_dict_entry(self):
        self.assertEqual(
            _profile_skill_pattern_fields({"name": "Python", "pattern": r"\bpython\b"}),
            ("Python", r"\bpython\b"),
        )

    def test_reads_object_entry(self):
        class Entry:
            name = "Go"
            pattern = r"\bgo\b"

        self.assertEqual(_profile_skill_pattern_fields(Entry()), ("Go", r"\bgo\b"))

    def test_unknown_entry_returns_empty(self):
        self.assertEqual(_profile_skill_pattern_fields("invalid"), ("", ""))


class ImportOrderTest(unittest.TestCase):
    def test_scoring_and_learning_import_without_cycle(self):
        import importlib

        importlib.import_module("spejder.jobs.scoring")
        importlib.import_module("spejder.extractors.skill_extractor.learning")


if __name__ == "__main__":
    unittest.main()
