"""Tests for antipattern synthesis and prompt injection."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.antipattern_synthesis import (
    SYNTHESIS_PATTERN_COUNT,
    SYNTHESIS_SAMPLE_MAX,
    _blocked_skills_for_synthesis,
    _merge_antipatterns,
    _remove_from_blocked_skills,
    _rules_from_llm_output,
    _sample_blocked_skills_for_synthesis,
    _synthesize_antipatterns_via_llm,
)
from spejder.extractors.skill_extractor.extraction_prompt import (
    _build_job_skill_extraction_prompt,
    _prompt_antipatterns,
)


class BuildPromptAntipatternsTest(unittest.TestCase):
    def test_includes_antipatterns_section(self):
        prompt = _build_job_skill_extraction_prompt(
            known_list=["python"],
            user_skills=["sql"],
            cleaned="Requirements: python required.",
            antipatterns=["we are looking for", "our team culture"],
        )
        self.assertIn("Antipatterns (never return these or similar phrasing):", prompt)
        self.assertIn("- we are looking for", prompt)

    def test_omits_antipatterns_when_empty(self):
        prompt = _build_job_skill_extraction_prompt(
            known_list=["python"],
            user_skills=[],
            cleaned="Requirements: python required.",
            antipatterns=[],
        )
        self.assertNotIn("Antipatterns (never return these or similar phrasing):", prompt)


class PromptAntipatternsNewestFirstTest(unittest.TestCase):
    def test_prompt_antipatterns_newest_first(self):
        items = [f"pattern-{i}" for i in range(50)]
        profile = AppConfig(
            skill_extraction_antipatterns=items,
            skill_antipattern_prompt_max_items=5,
        )
        result = _prompt_antipatterns(profile)
        self.assertEqual(result, ["pattern-45", "pattern-46", "pattern-47", "pattern-48", "pattern-49"])


class BlockedSkillsForSynthesisTest(unittest.TestCase):
    def test_uses_all_blocked_skills(self):
        profile = AppConfig(
            blocked_skills=["we are looking for", "python", "our new colleague"],
            skill_extraction_antipatterns=[],
        )
        skills = _blocked_skills_for_synthesis(profile)
        self.assertIn("we are looking for", skills)
        self.assertIn("python", skills)
        self.assertIn("our new colleague", skills)

    def test_skips_already_in_antipatterns(self):
        profile = AppConfig(
            blocked_skills=["we are looking for"],
            skill_extraction_antipatterns=["we are looking for"],
        )
        skills = _blocked_skills_for_synthesis(profile)
        self.assertEqual(skills, [])


class MergeAntipatternsTest(unittest.TestCase):
    def test_dedupes_normalized_keys(self):
        profile = AppConfig(skill_extraction_antipatterns=["We Are Looking For"])
        added = _merge_antipatterns(profile, ["we are looking for", "our team culture"])
        self.assertEqual(added, ["our team culture"])
        self.assertEqual(len(profile.skill_extraction_antipatterns), 2)


class RemoveFromBlockedSkillsTest(unittest.TestCase):
    def test_removes_matching_entry(self):
        profile = AppConfig(blocked_skills=["python", "we are looking for"])
        removed = _remove_from_blocked_skills(profile, "We Are Looking For")
        self.assertTrue(removed)
        self.assertEqual(profile.blocked_skills, ["python"])


class SynthesizeAntipatternsViaLlmTest(unittest.TestCase):
    def test_requests_three_patterns(self):
        blocked = [f"blocked phrase {i}" for i in range(20)]
        llm = MagicMock()
        llm.generate.return_value = (
            '{"rules": ["hiring narrative", "pronoun fragments", "company fluff"]}'
        )

        result = _synthesize_antipatterns_via_llm(llm, blocked, pattern_count=SYNTHESIS_PATTERN_COUNT)

        self.assertEqual(
            result,
            ["hiring narrative", "pronoun fragments", "company fluff"],
        )
        prompt = llm.generate.call_args[0][0]
        self.assertIn("exactly 3", prompt)

    @patch("spejder.extractors.skill_extractor.antipattern_synthesis.random.sample")
    def test_samples_large_blocked_lists_for_prompt(self, mock_sample):
        blocked = [f"blocked phrase {i}" for i in range(SYNTHESIS_SAMPLE_MAX + 50)]
        mock_sample.return_value = blocked[:SYNTHESIS_SAMPLE_MAX]
        llm = MagicMock()
        llm.generate.return_value = '{"rules": ["hiring narrative"]}'

        _synthesize_antipatterns_via_llm(llm, blocked, pattern_count=1)

        mock_sample.assert_called_once_with(blocked, SYNTHESIS_SAMPLE_MAX)
        prompt = llm.generate.call_args[0][0]
        self.assertIn("blocked phrase 0", prompt)
        self.assertNotIn("blocked phrase 100", prompt)


class RulesFromLlmOutputTest(unittest.TestCase):
    def test_parses_json_array(self):
        rules = _rules_from_llm_output(
            'Here you go:\n["hiring narrative", "company fluff"]',
            pattern_count=3,
        )
        self.assertEqual(rules, ["hiring narrative", "company fluff"])

    def test_parses_alternate_object_keys(self):
        rules = _rules_from_llm_output(
            '{"antipatterns": ["pronoun fragments", "malformed text"]}',
            pattern_count=3,
        )
        self.assertEqual(rules, ["pronoun fragments", "malformed text"])


class SampleBlockedSkillsForSynthesisTest(unittest.TestCase):
    def test_returns_all_when_small(self):
        blocked = [f"phrase-{i}" for i in range(10)]
        sample, sampled = _sample_blocked_skills_for_synthesis(blocked)
        self.assertEqual(sample, blocked)
        self.assertFalse(sampled)

    @patch("spejder.extractors.skill_extractor.antipattern_synthesis.random.sample")
    def test_random_sample_when_large(self, mock_sample):
        blocked = [f"phrase-{i}" for i in range(100)]
        mock_sample.return_value = [f"phrase-{i}" for i in range(10)]
        sample, sampled = _sample_blocked_skills_for_synthesis(blocked, max_sample=10)
        mock_sample.assert_called_once_with(blocked, 10)
        self.assertTrue(sampled)
        self.assertEqual(len(sample), 10)


if __name__ == "__main__":
    unittest.main()
