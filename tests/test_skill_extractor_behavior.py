"""Behavioral tests for skill_extractor heuristics."""

import json
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.extraction_fallback import _extract_skills_fallback
from spejder.extractors.skill_extractor.extraction_llm import _extract_job_skills_llm_path
from spejder.extractors.skill_extractor.filtering import (
    _filter_blocked_skill_names,
    _passes_phrase_quality,
    _skill_cleanup_reason,
)
from spejder.extractors.skill_extractor.utils import _format_skills, _profile_skill_pattern_fields


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
        skills = _extract_skills_fallback(text, patterns)
        self.assertIn("Python", skills)

    def test_extracts_from_skills_section(self):
        patterns = []
        text = "Qualifications: kubernetes, docker, and terraform."
        skills = _extract_skills_fallback(text, patterns)
        self.assertIn("kubernetes", skills)
        self.assertIn("docker", skills)

    def test_empty_text_returns_empty(self):
        self.assertEqual(_extract_skills_fallback("", []), [])

    def test_fallback_returns_all_pattern_matches(self):
        patterns = [(f"Skill{i}", rf"\bskill{i}\b") for i in range(12)]
        text = " ".join(f"skill{i}" for i in range(12))
        skills = _extract_skills_fallback(text, patterns)
        self.assertEqual(len(skills), 12)


class FormatSkillsTest(unittest.TestCase):
    def test_format_skills_does_not_truncate(self):
        skills = [f"skill{i}" for i in range(15)]
        formatted = _format_skills(skills)
        self.assertEqual(len(formatted.split(", ")), 15)


class ExtractJobSkillsLlmPathTest(unittest.TestCase):
    def test_includes_all_strong_new_candidates(self):
        raw = "Requirements: rust, go, kotlin, and swift experience required."
        payload = {
            "matched_known": [],
            "new_candidates": [
                {"name": "rust", "confidence": 0.95, "evidence": "rust"},
                {"name": "go", "confidence": 0.95, "evidence": "go"},
                {"name": "kotlin", "confidence": 0.95, "evidence": "kotlin"},
                {"name": "swift", "confidence": 0.95, "evidence": "swift"},
            ],
        }
        llm = MagicMock()
        llm.generate.return_value = json.dumps(payload)

        with patch(
            "spejder.extractors.skill_extractor.extraction_llm._get_skill_patterns",
            return_value=[],
        ):
            result = _extract_job_skills_llm_path(
                "jobs.db",
                raw,
                llm=llm,
                profile=AppConfig(),
            )

        self.assertEqual(len([s for s in result.split(", ") if s.strip()]), 4)


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
