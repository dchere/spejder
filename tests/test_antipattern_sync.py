"""Tests for blocked-skills antipattern sync."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.antipattern_synthesis import (
    SYNTHESIS_PATTERN_COUNT,
    _blocked_skills_for_synthesis,
    _merge_antipatterns,
    _remove_from_blocked_skills,
    _synthesize_antipatterns_via_llm,
)
from spejder.extractors.skill_extractor.antipattern_sync import (
    SYNC_MIN_BLOCKED,
    should_sync_skill_antipatterns,
    sync_skill_extraction_antipatterns,
)
from spejder.extractors.skill_extractor.antipattern_validation import (
    VALIDATION_RUNS,
    _generate_synthetic_job_posting,
    _stable_extracted_keys,
    _validate_antipattern_candidate,
)
from spejder.extractors.skill_extractor.extraction_prompt import (
    _build_job_skill_extraction_prompt,
    _prompt_antipatterns,
)


def _blocked_samples(count: int) -> list[str]:
    templates = [
        "we are looking for",
        "our new colleague",
        "what you will do develop",
        "will feel at home here",
        "your role includes develop",
        "working closely with r&d teams",
        "to support this journey",
        "the same time",
        "strengthen our team",
        "we are looking for managers",
    ]
    return [templates[i % len(templates)] for i in range(count)]


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
        skills, truncated = _blocked_skills_for_synthesis(profile)
        self.assertIn("we are looking for", skills)
        self.assertIn("python", skills)
        self.assertIn("our new colleague", skills)
        self.assertFalse(truncated)

    def test_skips_already_in_antipatterns(self):
        profile = AppConfig(
            blocked_skills=["we are looking for"],
            skill_extraction_antipatterns=["we are looking for"],
        )
        skills, _ = _blocked_skills_for_synthesis(profile)
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


class ShouldSyncGateTest(unittest.TestCase):
    def test_skips_without_llm(self):
        profile = AppConfig(blocked_skills=_blocked_samples(SYNC_MIN_BLOCKED))
        self.assertFalse(should_sync_skill_antipatterns(profile, llm=None))

    def test_skips_when_blocked_below_minimum(self):
        profile = AppConfig(blocked_skills=_blocked_samples(SYNC_MIN_BLOCKED - 1))
        self.assertFalse(should_sync_skill_antipatterns(profile, llm=MagicMock()))

    def test_runs_when_blocked_at_minimum(self):
        profile = AppConfig(blocked_skills=_blocked_samples(SYNC_MIN_BLOCKED))
        self.assertTrue(should_sync_skill_antipatterns(profile, llm=MagicMock()))


class SynthesizeAntipatternsViaLlmTest(unittest.TestCase):
    def test_requests_three_patterns(self):
        blocked = _blocked_samples(20)
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
        mock_stable.return_value = {"python", "our new colleague"}
        profile = AppConfig()
        llm = MagicMock()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            llm,
            "job text",
            "exclude hiring fluff",
            [],
            {"we are looking for", "our new colleague"},
            {"python"},
            baseline_keys={"python", "we are looking for", "our new colleague"},
        )
        self.assertTrue(result["accepted"])
        self.assertEqual(result["pruned_blocked"], ["we are looking for"])

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_skips_when_no_blocked_reduction(self, mock_stable):
        blocked = {"we are looking for"}
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
            blocked,
            {"python"},
            baseline_keys=keys,
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["skip_reason"], "no_blocked_reduction")

    @patch("spejder.extractors.skill_extractor.antipattern_validation._stable_extracted_keys")
    def test_skips_when_seen_skills_lost(self, mock_stable):
        mock_stable.return_value = {"we are looking for"}
        profile = AppConfig()
        result = _validate_antipattern_candidate(
            "./jobs.db",
            profile,
            MagicMock(),
            "job text",
            "too aggressive",
            [],
            {"we are looking for", "our new colleague"},
            {"python", "sql", "java"},
            baseline_keys={
                "python",
                "sql",
                "java",
                "we are looking for",
                "our new colleague",
            },
        )
        self.assertFalse(result["accepted"])
        self.assertEqual(result["skip_reason"], "seen_skills_lost")


class SyncSkillExtractionAntipatternsTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._stable_extracted_keys")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._skills_seen_at_least_once")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_commits_accepted_candidates(
        self,
        mock_synthesize,
        mock_seen,
        mock_job,
        mock_baseline,
        mock_validate,
        mock_delete,
        mock_save,
    ):
        mock_save.return_value = True
        mock_seen.return_value = ["python"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_baseline.return_value = {"python", "we are looking for"}
        mock_synthesize.return_value = ["rule one", "rule two", "rule three"]
        mock_validate.side_effect = [
            {
                "rule": "rule one",
                "accepted": True,
                "skip_reason": "",
                "baseline_blocked": ["we are looking for"],
                "with_blocked": [],
                "pruned_blocked": ["we are looking for"],
            },
            {
                "rule": "rule two",
                "accepted": False,
                "skip_reason": "no_blocked_reduction",
                "baseline_blocked": ["we are looking for"],
                "with_blocked": ["we are looking for"],
                "pruned_blocked": [],
            },
            {
                "rule": "rule three",
                "accepted": False,
                "skip_reason": "no_blocked_reduction",
                "baseline_blocked": ["we are looking for"],
                "with_blocked": ["we are looking for"],
                "pruned_blocked": [],
            },
        ]
        mock_delete.return_value = {"skill_rows_deleted": 1, "job_skill_links_deleted": 2}

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertFalse(stats["skipped"])
        self.assertTrue(stats["committed"])
        self.assertEqual(stats["candidates_accepted"], 1)
        self.assertEqual(stats["merged"], 1)
        self.assertEqual(stats["pruned_blocked"], 1)
        self.assertIn("rule one", profile.skill_extraction_antipatterns)
        mock_save.assert_called_once()
        mock_delete.assert_called_once()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._stable_extracted_keys")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._skills_seen_at_least_once")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_when_no_candidates_accepted(
        self,
        mock_synthesize,
        mock_seen,
        mock_job,
        mock_baseline,
        mock_validate,
    ):
        mock_seen.return_value = ["python"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_baseline.return_value = {"python", "we are looking for"}
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": False,
            "skip_reason": "no_blocked_reduction",
            "baseline_blocked": ["we are looking for"],
            "with_blocked": ["we are looking for"],
            "pruned_blocked": [],
        }

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(
            blocked_skills=blocked,
            skill_extraction_antipatterns=["keep me"],
        )
        original = list(profile.skill_extraction_antipatterns)

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["skipped"])
        self.assertEqual(stats["skip_reason"], "no_candidates_accepted")
        self.assertFalse(stats["committed"])
        self.assertEqual(profile.skill_extraction_antipatterns, original)

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._stable_extracted_keys")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._skills_seen_at_least_once")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_dry_run_reports_without_save(
        self,
        mock_synthesize,
        mock_seen,
        mock_job,
        mock_baseline,
        mock_validate,
    ):
        mock_seen.return_value = ["python"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_baseline.return_value = {"python", "we are looking for"}
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": True,
            "skip_reason": "",
            "baseline_blocked": ["we are looking for"],
            "with_blocked": [],
            "pruned_blocked": ["we are looking for"],
        }

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=list(blocked), skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
            dry_run=True,
        )

        self.assertFalse(stats["skipped"])
        self.assertFalse(stats["committed"])
        self.assertEqual(stats["would_prune_blocked"], 1)
        self.assertEqual(stats["pruned_blocked"], 0)
        self.assertEqual(len(profile.blocked_skills), len(blocked))


class GenerateSyntheticJobPostingTest(unittest.TestCase):
    def test_returns_trimmed_text(self):
        llm = MagicMock()
        llm.generate.return_value = "  We are looking for a Python developer.\n\nJoin us.  "
        text, blocked_truncated, seen_truncated = _generate_synthetic_job_posting(
            llm, ["we are looking for"], ["python"]
        )
        self.assertEqual(text, "We are looking for a Python developer. Join us.")
        self.assertFalse(blocked_truncated)
        self.assertFalse(seen_truncated)
        self.assertIn("we are looking for", llm.generate.call_args[0][0])


if __name__ == "__main__":
    unittest.main()
