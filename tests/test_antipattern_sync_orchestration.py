"""Tests for antipattern sync gate and orchestration."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.antipattern_synthesis import ANTIPATTERN_PROMPT_INPUT_MAX
from spejder.extractors.skill_extractor.antipattern_sync import (
    SYNC_MIN_BLOCKED,
    _good_skills_count,
    should_sync_skill_antipatterns,
    sync_skill_extraction_antipatterns,
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


class GoodSkillsCountTest(unittest.TestCase):
    def test_clamps_zero_to_one(self):
        profile = AppConfig.model_construct(skill_antipattern_good_skills_count=0)
        self.assertEqual(_good_skills_count(profile), 1)

    def test_clamps_negative_to_one(self):
        profile = AppConfig.model_construct(skill_antipattern_good_skills_count=-3)
        self.assertEqual(_good_skills_count(profile), 1)

    def test_uses_configured_value_when_positive(self):
        profile = AppConfig(skill_antipattern_good_skills_count=5)
        self.assertEqual(_good_skills_count(profile), 5)


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


class SyncSkillExtractionAntipatternsTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_commits_accepted_candidates(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
        mock_delete,
        mock_save,
    ):
        mock_save.return_value = True
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_synthesize.return_value = ["rule one", "rule two", "rule three"]
        mock_validate.side_effect = [
            {
                "rule": "rule one",
                "accepted": True,
                "skip_reason": "",
                "matched_blocked": ["we are looking for"],
                "baseline_blocked": ["we are looking for"],
                "with_blocked": [],
                "pruned_blocked": ["we are looking for"],
            },
            {
                "rule": "rule two",
                "accepted": False,
                "skip_reason": "no_blocked_reduction",
                "matched_blocked": ["we are looking for"],
                "baseline_blocked": ["we are looking for"],
                "with_blocked": ["we are looking for"],
                "pruned_blocked": [],
            },
            {
                "rule": "rule three",
                "accepted": False,
                "skip_reason": "no_blocked_reduction",
                "matched_blocked": ["we are looking for"],
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
        self.assertEqual(mock_match.call_count, 3)
        self.assertEqual(mock_job.call_count, 3)

    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    def test_skips_when_no_top_skills(self, mock_top_skills, mock_synthesize):
        mock_top_skills.return_value = []
        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["skipped"])
        self.assertEqual(stats["skip_reason"], "no_top_skills")
        mock_synthesize.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_truncates_matched_blocked_before_job_and_validation(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        matched = [f"blocked phrase {i}" for i in range(ANTIPATTERN_PROMPT_INPUT_MAX + 5)]
        mock_match.return_value = matched
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": False,
            "skip_reason": "no_blocked_reduction",
            "matched_blocked": [],
            "baseline_blocked": [],
            "with_blocked": [],
            "pruned_blocked": [],
        }

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        passed_blocked = mock_job.call_args[0][1]
        self.assertEqual(len(passed_blocked), ANTIPATTERN_PROMPT_INPUT_MAX)
        passed_validation_blocked = mock_validate.call_args[0][6]
        self.assertEqual(len(passed_validation_blocked), ANTIPATTERN_PROMPT_INPUT_MAX)

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_candidate_on_match_error(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.side_effect = RuntimeError("llm failed")
        mock_synthesize.return_value = ["rule one", "rule two"]

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertEqual(stats["candidates_skipped"], 2)
        self.assertEqual(stats["candidate_results"][0]["skip_reason"], "match_error")
        mock_job.assert_not_called()
        mock_validate.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_candidate_on_no_matched_blocked(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = []
        mock_synthesize.return_value = ["rule one"]

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertEqual(stats["candidate_results"][0]["skip_reason"], "no_matched_blocked")
        mock_job.assert_not_called()
        mock_validate.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_candidate_on_synthetic_job_error(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.side_effect = RuntimeError("job gen failed")
        mock_synthesize.return_value = ["rule one"]

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertEqual(stats["candidate_results"][0]["skip_reason"], "synthetic_job_error")
        mock_validate.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_candidate_on_synthetic_job_empty(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.return_value = ("", False, False)
        mock_synthesize.return_value = ["rule one"]

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertEqual(stats["candidate_results"][0]["skip_reason"], "synthetic_job_empty")
        self.assertEqual(stats["candidates_skipped"], 1)
        mock_validate.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_candidate_on_baseline_missing_blocked(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": False,
            "skip_reason": "baseline_missing_blocked",
            "matched_blocked": ["we are looking for"],
            "baseline_blocked": [],
            "with_blocked": [],
            "pruned_blocked": [],
        }

        blocked = _blocked_samples(SYNC_MIN_BLOCKED)
        profile = AppConfig(blocked_skills=blocked, skill_extraction_antipatterns=[])

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertEqual(stats["candidate_results"][0]["skip_reason"], "baseline_missing_blocked")
        self.assertEqual(stats["candidates_skipped"], 1)

    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_antipattern_candidate")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_skips_when_no_candidates_accepted(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": False,
            "skip_reason": "no_blocked_reduction",
            "matched_blocked": ["we are looking for"],
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
    @patch("spejder.extractors.skill_extractor.antipattern_sync._generate_synthetic_job_posting")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._match_blocked_skills_for_antipattern")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._top_position_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_dry_run_reports_without_save(
        self,
        mock_synthesize,
        mock_top_skills,
        mock_match,
        mock_job,
        mock_validate,
    ):
        mock_top_skills.return_value = ["python"]
        mock_match.return_value = ["we are looking for"]
        mock_job.return_value = ("Synthetic job posting text.", False, False)
        mock_synthesize.return_value = ["rule one"]
        mock_validate.return_value = {
            "rule": "rule one",
            "accepted": True,
            "skip_reason": "",
            "matched_blocked": ["we are looking for"],
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


if __name__ == "__main__":
    unittest.main()
