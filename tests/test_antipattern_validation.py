"""Tests for antipattern validation (matching, synthetic jobs, multi-run checks)."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor.antipattern_synthesis import ANTIPATTERN_PROMPT_INPUT_MAX
from spejder.extractors.skill_extractor.antipattern_validation import (
    _generate_synthetic_job_posting,
    _match_blocked_skills_for_antipattern,
    _matches_from_llm_output,
    _stable_extracted_keys,
    _top_position_skills,
    _validate_antipattern_candidate,
)


class MatchBlockedSkillsForAntipatternTest(unittest.TestCase):
    def test_returns_matches_from_llm_json(self):
        blocked = ["we are looking for", "our new colleague", "python"]
        llm = MagicMock()
        llm.generate.return_value = (
            '{"matches": ["we are looking for", "our new colleague"]}'
        )
        matches = _match_blocked_skills_for_antipattern(llm, "hiring fluff", blocked)
        self.assertEqual(matches, ["we are looking for", "our new colleague"])

    def test_ignores_off_list_llm_matches(self):
        blocked = ["we are looking for", "our new colleague"]
        llm = MagicMock()
        llm.generate.return_value = (
            '{"matches": ["we are looking for", "totally invented phrase"]}'
        )
        matches = _match_blocked_skills_for_antipattern(llm, "hiring fluff", blocked)
        self.assertEqual(matches, ["we are looking for"])

    def test_chunks_large_blocked_lists(self):
        blocked = [f"blocked phrase {i}" for i in range(ANTIPATTERN_PROMPT_INPUT_MAX + 10)]
        llm = MagicMock()
        llm.generate.side_effect = [
            '{"matches": ["blocked phrase 0"]}',
            '{"matches": ["blocked phrase 150"]}',
        ]
        matches = _match_blocked_skills_for_antipattern(llm, "hiring fluff", blocked)
        self.assertEqual(llm.generate.call_count, 2)
        self.assertEqual(matches, ["blocked phrase 0", "blocked phrase 150"])

    @patch("builtins.print")
    def test_logs_debug_when_match_parse_empty(self, mock_print):
        blocked = ["we are looking for", "our new colleague"]
        llm = MagicMock()
        llm.generate.return_value = "not valid json at all"
        _match_blocked_skills_for_antipattern(llm, "hiring fluff", blocked)
        debug_calls = [
            call
            for call in mock_print.call_args_list
            if call.args and str(call.args[0]).startswith("Antipattern sync: match parse empty")
        ]
        self.assertEqual(len(debug_calls), 1)


class MatchesFromLlmOutputTest(unittest.TestCase):
    def test_rejects_off_list_keys(self):
        blocked_by_key = {
            "we are looking for": "we are looking for",
            "our new colleague": "our new colleague",
        }
        matches = _matches_from_llm_output(
            '{"matches": ["we are looking for", "invented phrase"]}',
            blocked_by_key,
        )
        self.assertEqual(matches, ["we are looking for"])


class TopPositionSkillsTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_validation.get_top_skills_by_job_links")
    def test_delegates_to_db_helper_with_normalized_exclude_keys(self, mock_get_top):
        mock_get_top.return_value = ["python", "sql"]
        profile = AppConfig(blocked_skills=["  Blocked   Phrase  "])
        skills = _top_position_skills("./jobs.db", profile, limit=2)
        self.assertEqual(skills, ["python", "sql"])
        mock_get_top.assert_called_once()
        call_args = mock_get_top.call_args
        self.assertEqual(call_args[0][0], "./jobs.db")
        self.assertEqual(call_args[0][1], 2)
        self.assertEqual(
            call_args[1]["exclude_keys"],
            {_normalize_skill_name_key("  Blocked   Phrase  ")},
        )


class StableExtractedKeysTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_validation._extract_job_skills_llm_path")
    def test_returns_intersection_across_runs(self, mock_extract):
        mock_extract.side_effect = [
            "python, sql, junk",
            "python, sql",
            "python, sql, extra",
        ]
        profile = AppConfig()
        llm = MagicMock()
        keys = _stable_extracted_keys(
            "./jobs.db",
            profile,
            llm,
            "job text",
            antipatterns_override=[],
            runs=3,
        )
        self.assertEqual(keys, {"python", "sql"})

    @patch("spejder.extractors.skill_extractor.antipattern_validation._extract_job_skills_llm_path")
    def test_skips_blocked_filter_during_validation(self, mock_extract):
        mock_extract.return_value = "python, we are looking for"
        profile = AppConfig(blocked_skills=["we are looking for"])
        _stable_extracted_keys(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            antipatterns_override=[],
            runs=1,
        )
        self.assertTrue(mock_extract.call_args.kwargs["skip_blocked_filter"])


class ValidateAntipatternCandidateTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_accepts_when_blocked_count_drops(self, mock_stable):
        mock_stable.side_effect = [
            {"python", "we are looking for", "our new colleague"},
            {"python", "our new colleague"},
        ]
        profile = AppConfig()
        llm = MagicMock()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            llm,
            "job text",
            "exclude hiring fluff",
            [],
            ["we are looking for", "our new colleague"],
            {"python"},
        )
        self.assertTrue(result["accepted"])
        self.assertEqual(result["pruned_blocked"], ["we are looking for"])

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_skips_when_baseline_missing_matched_blocked(self, mock_stable):
        mock_stable.return_value = {"python", "our new colleague"}
        profile = AppConfig()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            "exclude hiring fluff",
            [],
            ["we are looking for", "our new colleague"],
            {"python"},
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["skip_reason"], "baseline_missing_blocked")

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_skips_when_no_blocked_reduction(self, mock_stable):
        matched = ["we are looking for"]
        keys = {"python", "we are looking for"}
        mock_stable.side_effect = [keys, keys]
        profile = AppConfig()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            "useless rule",
            [],
            matched,
            {"python"},
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["skip_reason"], "no_blocked_reduction")

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_accepts_when_one_good_skill_lost_within_tolerance(self, mock_stable):
        mock_stable.side_effect = [
            {
                "python",
                "sql",
                "java",
                "we are looking for",
                "our new colleague",
            },
            {"sql", "java", "our new colleague"},
        ]
        profile = AppConfig()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            "moderate rule",
            [],
            ["we are looking for", "our new colleague"],
            {"python", "sql", "java"},
        )
        self.assertTrue(result["accepted"])
        self.assertEqual(result["pruned_blocked"], ["we are looking for"])

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_skips_when_two_good_skills_lost(self, mock_stable):
        mock_stable.side_effect = [
            {
                "python",
                "sql",
                "java",
                "we are looking for",
                "our new colleague",
            },
            {"java", "our new colleague"},
        ]
        profile = AppConfig()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            "too aggressive",
            [],
            ["we are looking for", "our new colleague"],
            {"python", "sql", "java"},
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["skip_reason"], "good_skills_lost")


class GenerateSyntheticJobPostingTest(unittest.TestCase):
    def test_returns_trimmed_text(self):
        llm = MagicMock()
        llm.generate.return_value = "  We are looking for a Python developer.\n\nJoin us.  "
        text, blocked_truncated, good_skills_truncated = _generate_synthetic_job_posting(
            llm, ["we are looking for"], ["python"]
        )
        self.assertEqual(text, "We are looking for a Python developer. Join us.")
        self.assertFalse(blocked_truncated)
        self.assertFalse(good_skills_truncated)
        self.assertIn("we are looking for", llm.generate.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
