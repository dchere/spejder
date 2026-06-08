"""Tests for blocked-skills antipattern sync."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.antipattern_sync import (
    SYNTHESIS_INPUT_MAX,
    SYNTHESIS_MAX_TOKENS,
    SYNC_MIN_BLOCKED,
    SYNC_MIN_DELTA,
    SYNC_MIN_JUNK_CANDIDATES,
    _filter_synthesized_antipatterns,
    _merge_antipatterns,
    _remove_from_blocked_skills,
    _select_junk_blocked_candidates,
    _synthesize_antipatterns_via_llm,
    _validate_skill_filtered_by_prompt,
    should_sync_skill_antipatterns,
    sync_skill_extraction_antipatterns,
)
from spejder.extractors.skill_extractor.extraction import (
    _build_job_skill_extraction_prompt,
    _prompt_antipatterns,
)


def _junk_blocked_samples(count: int) -> list[str]:
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
        self.assertIn("- our team culture", prompt)

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


class SelectJunkBlockedCandidatesTest(unittest.TestCase):
    def test_selects_sentence_fragments(self):
        profile = AppConfig(
            blocked_skills=[
                "we are looking for",
                "python",
                "wms",
            ],
            skill_extraction_antipatterns=[],
        )
        candidates = _select_junk_blocked_candidates(profile)
        self.assertIn("we are looking for", candidates)
        self.assertNotIn("python", candidates)
        self.assertNotIn("wms", candidates)

    def test_skips_already_in_antipatterns(self):
        profile = AppConfig(
            blocked_skills=["we are looking for"],
            skill_extraction_antipatterns=["we are looking for"],
        )
        self.assertEqual(_select_junk_blocked_candidates(profile), [])

    def test_candidates_sorted_by_job_links(self):
        link_counts = {
            "we are looking for": 3,
            "our new colleague": 1,
            "what you will do develop": 2,
        }
        profile = AppConfig(
            blocked_skills=[
                "our new colleague",
                "what you will do develop",
                "we are looking for",
            ],
            skill_extraction_antipatterns=[],
        )
        candidates = _select_junk_blocked_candidates(
            profile, db_path="./jobs.db", link_counts=link_counts
        )
        self.assertEqual(
            candidates,
            ["we are looking for", "what you will do develop", "our new colleague"],
        )


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
        profile = AppConfig(blocked_skills=["we are looking for"] * 20)
        self.assertFalse(should_sync_skill_antipatterns(profile, llm=None))

    def test_skips_when_blocked_below_minimum(self):
        profile = AppConfig(blocked_skills=["we are looking for"] * (SYNC_MIN_BLOCKED - 1))
        self.assertFalse(should_sync_skill_antipatterns(profile, llm=MagicMock()))

    def test_skips_when_delta_below_threshold(self):
        blocked = ["we are looking for"] * SYNC_MIN_BLOCKED
        profile = AppConfig(
            blocked_skills=blocked,
            skill_antipattern_last_sync_blocked_count=SYNC_MIN_BLOCKED - 1,
        )
        self.assertFalse(should_sync_skill_antipatterns(profile, llm=MagicMock()))

    def test_runs_when_first_sync_and_enough_junk(self):
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=junk + ["python"] * SYNC_MIN_BLOCKED,
            skill_antipattern_last_sync_blocked_count=0,
        )
        self.assertTrue(should_sync_skill_antipatterns(profile, llm=MagicMock()))

    def test_runs_when_delta_large_enough(self):
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=junk + ["python"] * (SYNC_MIN_BLOCKED + SYNC_MIN_DELTA),
            skill_antipattern_last_sync_blocked_count=SYNC_MIN_BLOCKED,
        )
        self.assertTrue(should_sync_skill_antipatterns(profile, llm=MagicMock()))


class ValidateSkillFilteredByPromptTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_sync._extract_job_skills_llm_path")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_for_rescoring")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_ids_for_skill")
    def test_skips_missing_jobs_and_empty_text(
        self, mock_get_job_ids, mock_get_job, mock_llm_path
    ):
        mock_get_job_ids.return_value = [1, 2, 3]
        mock_get_job.side_effect = [
            None,
            {"raw_text": "   "},
            {"raw_text": "Requirements: python required."},
        ]
        mock_llm_path.return_value = "python"

        profile = AppConfig()
        llm = MagicMock()
        result = _validate_skill_filtered_by_prompt(
            "./jobs.db", profile, llm, "we are looking for"
        )

        self.assertTrue(result)
        mock_llm_path.assert_called_once()

    @patch("spejder.extractors.skill_extractor.antipattern_sync._extract_job_skills_llm_path")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_for_rescoring")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_ids_for_skill")
    def test_returns_false_when_all_llm_failures(
        self, mock_get_job_ids, mock_get_job, mock_llm_path
    ):
        mock_get_job_ids.return_value = [1, 2]
        mock_get_job.return_value = {"raw_text": "Requirements: python required."}
        mock_llm_path.return_value = None

        profile = AppConfig()
        llm = MagicMock()
        result = _validate_skill_filtered_by_prompt(
            "./jobs.db", profile, llm, "we are looking for"
        )

        self.assertFalse(result)
        self.assertEqual(mock_llm_path.call_count, 2)

    @patch("spejder.extractors.skill_extractor.antipattern_sync._extract_job_skills_llm_path")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_for_rescoring")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_ids_for_skill")
    def test_returns_false_when_skill_in_output(
        self, mock_get_job_ids, mock_get_job, mock_llm_path
    ):
        mock_get_job_ids.return_value = [1]
        mock_get_job.return_value = {"raw_text": "We are looking for engineers."}
        mock_llm_path.return_value = "we are looking for"

        profile = AppConfig()
        llm = MagicMock()
        result = _validate_skill_filtered_by_prompt(
            "./jobs.db", profile, llm, "we are looking for"
        )

        self.assertFalse(result)

    @patch("spejder.extractors.skill_extractor.antipattern_sync.get_job_ids_for_skill")
    def test_returns_false_when_no_usable_jobs(self, mock_get_job_ids):
        mock_get_job_ids.return_value = []

        profile = AppConfig()
        llm = MagicMock()
        result = _validate_skill_filtered_by_prompt(
            "./jobs.db", profile, llm, "we are looking for"
        )

        self.assertFalse(result)


class SynthesizeAntipatternsViaLlmTest(unittest.TestCase):
    def test_caps_llm_input_and_token_budget(self):
        candidates = [f"junk phrase {i}" for i in range(SYNTHESIS_INPUT_MAX + 20)]
        llm = MagicMock()
        llm.generate.return_value = '{"rules": ["hiring narrative"], "examples": []}'

        result = _synthesize_antipatterns_via_llm(llm, candidates)

        self.assertEqual(result, ["hiring narrative"])
        prompt = llm.generate.call_args[0][0]
        for item in candidates[SYNTHESIS_INPUT_MAX:]:
            self.assertNotIn(item, prompt)
        self.assertEqual(llm.generate.call_args.kwargs["max_tokens"], SYNTHESIS_MAX_TOKENS)


class FilterSynthesizedAntipatternsTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._pick_probe_skill")
    def test_per_rule_filter_drops_useless_rule(self, mock_pick_probe, mock_validate):
        mock_pick_probe.return_value = "we are looking for"
        mock_validate.side_effect = [False, False]

        profile = AppConfig(skill_extraction_antipatterns=[])
        llm = MagicMock()
        kept, stats = _filter_synthesized_antipatterns(
            "./jobs.db",
            profile,
            llm,
            [],
            ["exclude useless hiring fluff"],
            ["we are looking for"],
        )

        self.assertEqual(kept, [])
        self.assertEqual(stats["rules_filtered"], 1)
        self.assertEqual(stats["rules_kept"], 0)


class SyncSkillExtractionAntipatternsTest(unittest.TestCase):
    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_prunes_validated_skills(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        mock_save.return_value = True
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = True
        mock_delete.return_value = {"skill_rows_deleted": 1, "job_skill_links_deleted": 2}

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertFalse(stats["skipped"])
        self.assertFalse(stats["batch_rejected"])
        self.assertGreater(stats["merged"], 0)
        self.assertGreater(stats["pruned_blocked"], 0)
        self.assertIn(junk[0], profile.skill_extraction_antipatterns)
        mock_save.assert_called_once()
        mock_delete.assert_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_keeps_skills_when_validation_fails(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=["existing rule"],
            skill_antipattern_last_sync_blocked_count=0,
        )
        original_antipatterns = list(profile.skill_extraction_antipatterns)
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = False

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["batch_rejected"])
        self.assertEqual(stats["validated"], 0)
        self.assertEqual(stats["pruned_blocked"], 0)
        self.assertEqual(len(profile.blocked_skills), len(junk))
        self.assertEqual(profile.skill_extraction_antipatterns, original_antipatterns)
        mock_delete.assert_not_called()
        mock_save.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_batch_rollback_when_zero_validated(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=["keep me"],
            skill_antipattern_last_sync_blocked_count=0,
        )
        original_antipatterns = list(profile.skill_extraction_antipatterns)
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = False

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["batch_rejected"])
        self.assertEqual(stats["validated"], 0)
        self.assertEqual(profile.skill_extraction_antipatterns, original_antipatterns)
        self.assertEqual(profile.skill_antipattern_last_sync_blocked_count, 0)
        mock_save.assert_not_called()
        mock_delete.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_dry_run_reports_would_prune_without_save(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = True

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
            dry_run=True,
        )

        self.assertFalse(stats["skipped"])
        self.assertGreater(stats["would_prune_blocked"], 0)
        self.assertEqual(stats["pruned_blocked"], 0)
        self.assertEqual(len(profile.blocked_skills), len(junk))
        mock_save.assert_not_called()
        mock_delete.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_partial_batch_validates_subset(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )
        mock_synthesize.return_value = [junk[0]]
        mock_validate.side_effect = [True, False, True] + [False] * (SYNC_MIN_JUNK_CANDIDATES - 3)
        mock_delete.return_value = {"skill_rows_deleted": 1, "job_skill_links_deleted": 1}
        mock_save.return_value = True

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertFalse(stats["batch_rejected"])
        self.assertEqual(stats["validated"], 2)
        self.assertEqual(stats["pruned_blocked"], 2)
        mock_save.assert_called_once()
        self.assertEqual(mock_delete.call_count, 2)

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_profile_save_skipped_on_mtime_change(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        mock_save.return_value = False
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )
        original_blocked = list(profile.blocked_skills)
        original_antipatterns = list(profile.skill_extraction_antipatterns)
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = True

        llm = MagicMock()
        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["profile_save_skipped"])
        self.assertEqual(stats["db_skill_rows_deleted"], 0)
        self.assertEqual(stats["db_job_links_deleted"], 0)
        self.assertEqual(profile.blocked_skills, original_blocked)
        self.assertEqual(profile.skill_extraction_antipatterns, original_antipatterns)
        self.assertEqual(stats["merged"], 0)
        self.assertEqual(stats["pruned_blocked"], 0)
        mock_save.assert_called_once()
        mock_delete.assert_not_called()

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._save_antipattern_sync_profile")
    @patch("spejder.extractors.skill_extractor.antipattern_sync.delete_skill_from_db")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._validate_skill_filtered_by_prompt")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_db_deletes_after_successful_save(
        self,
        mock_synthesize,
        mock_validate,
        mock_delete,
        mock_save,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        mock_save.return_value = True
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )
        mock_synthesize.return_value = [junk[0]]
        mock_validate.return_value = True
        call_order: list[str] = []

        def track_save(*_args, **_kwargs):
            call_order.append("save")
            return True

        def track_delete(*_args, **_kwargs):
            call_order.append("delete")
            return {"skill_rows_deleted": 1, "job_skill_links_deleted": 1}

        mock_save.side_effect = track_save
        mock_delete.side_effect = track_delete

        llm = MagicMock()
        sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            llm,
            profile_path="./profile.json",
            force=True,
        )

        self.assertGreater(len(call_order), 1)
        self.assertEqual(call_order[0], "save")
        self.assertTrue(all(step == "delete" for step in call_order[1:]))

    @patch("spejder.extractors.skill_extractor.antipattern_sync.count_job_links_for_skills")
    @patch("spejder.extractors.skill_extractor.antipattern_sync._synthesize_antipatterns_via_llm")
    def test_reports_skip_reason_when_synthesis_empty(
        self,
        mock_synthesize,
        mock_link_counts,
    ):
        mock_link_counts.return_value = {}
        mock_synthesize.return_value = []
        junk = _junk_blocked_samples(SYNC_MIN_JUNK_CANDIDATES)
        profile = AppConfig(
            blocked_skills=list(junk),
            skill_extraction_antipatterns=[],
            skill_antipattern_last_sync_blocked_count=0,
        )

        stats = sync_skill_extraction_antipatterns(
            "./jobs.db",
            profile,
            MagicMock(),
            profile_path="./profile.json",
            force=True,
        )

        self.assertTrue(stats["skipped"])
        self.assertEqual(stats["skip_reason"], "synthesis_empty")


if __name__ == "__main__":
    unittest.main()
