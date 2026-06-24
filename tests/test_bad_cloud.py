"""Tests for bad bigram cloud scoring and extraction filtering."""

import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import count_bad_ngrams, ensure_db, replace_job_skills, upsert_job, upsert_skill_pattern
from spejder.db.connection import _connect
from spejder.extractors.skill_extractor.bad_cloud import (
    _ngrams_for_cloud,
    _tokenize_for_cloud,
    calibrate_threshold,
    ensure_bad_cloud_initialized,
    ingest_blocked_skill,
    ingest_blocked_skills,
    on_skills_blocked,
    prune_blocked_skills_by_cloud,
    toxicity_score,
    toxicity_scores_by_key,
)
from spejder.extractors.skill_extractor.filtering import (
    _filter_extracted_skills,
    _whitelist_skill_keys,
)
from spejder.extractors.skill_extractor.extraction import _get_or_extract_job_skills


class TokenizeAndNgramsTest(unittest.TestCase):
    def test_bigrams_from_multi_word_skill(self):
        tokens = _tokenize_for_cloud("Managing Office Expectations")
        ngrams = _ngrams_for_cloud(tokens)
        self.assertEqual(tokens, ["managing", "office", "expectations"])
        self.assertEqual(
            ngrams,
            [("managing office", 2), ("office expectations", 2)],
        )

    def test_unigram_for_single_word(self):
        ngrams = _ngrams_for_cloud(["python"])
        self.assertEqual(ngrams, [("python", 1)])


class ToxicityScoreTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_unknown_ngrams_score_zero(self):
        self.assertEqual(toxicity_score("brand new skill", self.db_path), 0.0)

    def test_ingest_raises_score(self):
        ingest_blocked_skill("we are looking", self.db_path)
        score = toxicity_score("we are looking", self.db_path)
        self.assertGreater(score, 0.0)

    def test_batch_scores_match_single_scores(self):
        ingest_blocked_skill("we are looking", self.db_path)
        skills = ["we are looking", "python"]
        batch = toxicity_scores_by_key(skills, self.db_path)
        for skill in skills:
            self.assertAlmostEqual(
                batch[skill],
                toxicity_score(skill, self.db_path),
            )

    def test_bulk_ingest_aggregates_shared_ngrams(self):
        ingest_blocked_skills(
            ["we are looking", "we are hiring"],
            self.db_path,
        )
        shared_score = toxicity_score("we are", self.db_path)
        self.assertGreater(shared_score, 0.0)


class PruneBlockedSkillsTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_prunes_high_score_blocked_entries(self):
        ingest_blocked_skill("we are looking", self.db_path)
        profile = AppConfig(
            blocked_skills=["we are looking", "python"],
            skill_bigram_toxicity_threshold=0.1,
        )
        pruned = prune_blocked_skills_by_cloud(profile, self.db_path)
        self.assertIn("we are looking", pruned)
        self.assertEqual(profile.blocked_skills, ["python"])


class FilterExtractedSkillsTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_detected_zero_occurrence_not_whitelisted(self):
        upsert_skill_pattern(
            self.db_path,
            "we are looking",
            r"\bwe are looking\b",
            source="detected",
        )
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        ingest_blocked_skill("we are looking", self.db_path)
        known_keys = _whitelist_skill_keys(profile, self.db_path)
        self.assertNotIn("we are looking", known_keys)

    def test_known_skill_passes_without_cloud_check(self):
        profile = AppConfig(blocked_skills=["blocked phrase"])
        ingest_blocked_skill("we are looking", self.db_path)
        profile.skill_bigram_toxicity_threshold = 0.1
        known_keys = {"python"}
        result = _filter_extracted_skills(
            ["python", "blocked phrase", "we are looking"],
            profile,
            self.db_path,
            known_keys,
        )
        self.assertEqual(result, ["python"])

    def test_exact_blocked_rejected(self):
        profile = AppConfig(blocked_skills=["sql"], skill_bigram_toxicity_threshold=0.01)
        result = _filter_extracted_skills(
            ["sql"],
            profile,
            self.db_path,
            set(),
        )
        self.assertEqual(result, [])

    def test_toxic_unseen_skill_rejected(self):
        ingest_blocked_skill("we are looking", self.db_path)
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        result = _filter_extracted_skills(
            ["we are looking"],
            profile,
            self.db_path,
            set(),
        )
        self.assertEqual(result, [])


class EnsureBadCloudInitializedTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_first_seed_protects_blocked_list_from_prune(self):
        profile = AppConfig(
            blocked_skills=["we are looking", "python"],
            bad_cloud_seeded=False,
        )
        stats = ensure_bad_cloud_initialized(profile, self.db_path)
        self.assertTrue(stats["seeded"])
        self.assertEqual(stats["pruned"], [])
        self.assertEqual(len(profile.blocked_skills), 2)

    def test_seeds_once_from_blocked_skills(self):
        profile = AppConfig(
            blocked_skills=["we are looking", "our team culture"],
            bad_cloud_seeded=False,
        )
        stats = ensure_bad_cloud_initialized(profile, self.db_path)
        self.assertTrue(stats["seeded"])
        self.assertTrue(profile.bad_cloud_seeded)
        self.assertGreater(count_bad_ngrams(self.db_path), 0)

        before = count_bad_ngrams(self.db_path)
        again = ensure_bad_cloud_initialized(profile, self.db_path)
        self.assertFalse(again["seeded"])
        self.assertEqual(count_bad_ngrams(self.db_path), before)


class CalibrateThresholdTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_calibrate_returns_floor_when_cloud_empty(self):
        profile = AppConfig()
        self.assertEqual(calibrate_threshold(self.db_path, profile), float("inf"))

    def test_calibrate_separates_good_and_bad(self):
        ingest_blocked_skill("we are looking", self.db_path)
        upsert_skill_pattern(self.db_path, "python", r"\bpython\b", source="seed")
        profile = AppConfig(blocked_skills=["we are looking"])
        threshold = calibrate_threshold(self.db_path, profile)
        self.assertGreaterEqual(threshold, 0.1)
        self.assertGreater(
            toxicity_score("we are looking", self.db_path),
            toxicity_score("python", self.db_path),
        )


class OnSkillsBlockedTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_ingests_without_double_count_on_repeat_block_call(self):
        profile = AppConfig(blocked_skills=["we are looking"])
        first = on_skills_blocked(profile, self.db_path, ["we are looking"])
        score_after_first = toxicity_score("we are looking", self.db_path)
        on_skills_blocked(profile, self.db_path, ["we are looking"])
        score_after_second = toxicity_score("we are looking", self.db_path)
        self.assertGreater(score_after_second, score_after_first)
        self.assertGreater(first["ngram_keys_upserted"], 0)


class CachedJobSkillsFilterTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/cache-toxic",
                "raw_text": "Requirements: python.",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM jobs WHERE position_link=?",
                ("https://example.com/cache-toxic",),
            )
            self.job_id = int(cur.fetchone()[0])
        finally:
            conn.close()
        replace_job_skills(self.db_path, self.job_id, ["python", "we are looking"])
        ingest_blocked_skill("we are looking", self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_cached_skills_filtered_by_bad_cloud(self):
        profile = AppConfig(skill_bigram_toxicity_threshold=0.1)
        upsert_skill_pattern(self.db_path, "python", r"\bpython\b", source="seed")
        skills_text, changed = _get_or_extract_job_skills(
            self.db_path,
            self.job_id,
            raw_text="Requirements: python.",
            profile=profile,
        )
        self.assertFalse(changed)
        self.assertEqual(skills_text, "python")


if __name__ == "__main__":
    unittest.main()
