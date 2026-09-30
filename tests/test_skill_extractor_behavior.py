"""Behavioral tests for skill_extractor heuristics."""

import json
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.extraction_fallback import _extract_skills_fallback
from spejder.extractors.skill_extractor.extraction_llm import (
    _extract_job_skills_llm_path,
    _select_known_skills_for_prompt,
)
from spejder.extractors.skill_extractor.extraction_prompt import (
    KNOWN_SKILLS_PROMPT_LIMIT,
    _build_job_skill_extraction_prompt,
)
from spejder.extractors.skill_extractor.filtering import (
    _filter_blocked_skill_names,
    _passes_phrase_quality,
    _protected_skill_keys,
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

    def test_malformed_punctuation(self):
        self.assertEqual(_skill_cleanup_reason("sql?", "learned", set()), "malformed text")

    def test_pronoun_sentence_fragment(self):
        self.assertEqual(
            _skill_cleanup_reason("our team culture", "learned", set()),
            "sentence fragment",
        )

    def test_too_many_words(self):
        self.assertEqual(
            _skill_cleanup_reason("one two three four five", "learned", set()),
            "too many words",
        )

    def test_protected_key_skips_structural_checks(self):
        self.assertEqual(
            _skill_cleanup_reason("our team culture", "learned", {"our team culture"}),
            "",
        )

    def test_no_curated_phrase_or_stopword_lists(self):
        """Retired empty TODO sets must not invent reasons for ordinary tokens."""
        # Would have been "contains stopword" / "generic phrase" if lists were populated.
        self.assertEqual(_skill_cleanup_reason("experience", "learned", set()), "")
        self.assertEqual(_skill_cleanup_reason("soft skills", "learned", set()), "")


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

    def test_prompt_uses_weight_order_and_text_hit_preference(self):
        # Weight order: zebra first (late alphabet), then apple, then mango.
        patterns = [
            ("zebra", r"\bzebra\b"),
            ("apple", r"\bapple\b"),
            ("mango", r"\bmango\b"),
        ]
        raw = "We need mango expertise and cloud experience."
        llm = MagicMock()
        llm.generate.return_value = json.dumps(
            {"matched_known": [], "new_candidates": []}
        )

        with patch(
            "spejder.extractors.skill_extractor.extraction_llm._get_skill_patterns",
            return_value=patterns,
        ):
            _extract_job_skills_llm_path(
                "jobs.db",
                raw,
                llm=llm,
                profile=AppConfig(),
            )

        prompt = llm.generate.call_args.args[0]
        known_section = prompt.split("Known skills (prefer these): ", 1)[1]
        known_section = known_section.split("\n\n", 1)[0]
        known_names = [part.strip() for part in known_section.split(",") if part.strip()]
        # Text-hit mango first, then weight-ordered remainder (zebra, apple).
        self.assertEqual(known_names, ["mango", "zebra", "apple"])


class BuildJobSkillExtractionPromptTest(unittest.TestCase):
    def test_description_precedes_known_skills_vocabulary(self):
        prompt = _build_job_skill_extraction_prompt(
            known_list=["python", "sql"],
            user_skills=["docker"],
            cleaned="Need python and sql.",
        )
        desc_idx = prompt.index("Description:\n")
        known_idx = prompt.index("Known skills (prefer these):")
        user_idx = prompt.index("Candidate skills from user profile")
        json_idx = prompt.index("JSON:")
        self.assertLess(desc_idx, known_idx)
        self.assertLess(known_idx, user_idx)
        self.assertLess(user_idx, json_idx)
        self.assertLess(prompt.index("Hard rules:"), desc_idx)


class SelectKnownSkillsForPromptTest(unittest.TestCase):
    def test_preserves_weight_order_without_alpha_sort(self):
        # Intentionally reverse-alphabetical weight order.
        patterns = [(f"skill{i}", rf"\bskill{i}\b") for i in range(5, 0, -1)]
        _known_by_key, known_list = _select_known_skills_for_prompt(
            patterns, "no hits here", limit=3
        )
        self.assertEqual(known_list, ["skill5", "skill4", "skill3"])

    def test_text_hits_precede_weight_ordered_pad(self):
        patterns = [
            ("alpha", r"\balpha\b"),
            ("beta", r"\bbeta\b"),
            ("gamma", r"\bgamma\b"),
            ("delta", r"\bdelta\b"),
        ]
        _known_by_key, known_list = _select_known_skills_for_prompt(
            patterns, "Looking for gamma and delta engineers.", limit=3
        )
        self.assertEqual(known_list, ["gamma", "delta", "alpha"])

    def test_respects_prompt_limit_constant(self):
        patterns = [(f"Skill{i}", rf"\bskill{i}\b") for i in range(KNOWN_SKILLS_PROMPT_LIMIT + 20)]
        _known_by_key, known_list = _select_known_skills_for_prompt(
            patterns, "skill310 appears late", limit=KNOWN_SKILLS_PROMPT_LIMIT
        )
        self.assertEqual(len(known_list), KNOWN_SKILLS_PROMPT_LIMIT)
        # Text hit skill310 should be first despite weight-order position > 300.
        self.assertEqual(known_list[0], "skill310")
        # Low-weight tail beyond the pad window is dropped.
        self.assertNotIn("skill319", known_list)
        self.assertIn("skill0", known_list)


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


class ProtectedSkillKeysTest(unittest.TestCase):
    def test_includes_unwanted_skill_name(self):
        profile = AppConfig(unwanted_skills=["Sales"])
        self.assertIn("sales", _protected_skill_keys(profile))


if __name__ == "__main__":
    unittest.main()
