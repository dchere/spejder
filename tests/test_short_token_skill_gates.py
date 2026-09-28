"""Tests for ambiguous short-token skill extraction gates (A′)."""

import json
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.extraction_fallback import _extract_skills_fallback
from spejder.extractors.skill_extractor.extraction_llm import _extract_job_skills_llm_path
from spejder.extractors.skill_extractor.filtering import _passes_phrase_quality
from spejder.extractors.skill_extractor.short_token_gates import _short_token_evidence_ok


_GO_PATTERN = ("Go", r"\bgolang\b|\bgo\b")
_IT_PATTERN = ("IT", r"\binformation\s+technology\b|\bit\b")


class ShortTokenEvidenceOkTest(unittest.TestCase):
    def test_non_allowlisted_always_passes(self):
        self.assertTrue(_short_token_evidence_ok("python", "we love python"))

    def test_go_rejects_to_go_and_go_above(self):
        self.assertFalse(
            _short_token_evidence_ok("go", "Candidates ready to go above and beyond.")
        )
        self.assertFalse(
            _short_token_evidence_ok("Go", "You will go to customer sites weekly.")
        )

    def test_go_accepts_golang_alias(self):
        self.assertTrue(
            _short_token_evidence_ok("go", "Experience with golang microservices.")
        )

    def test_go_accepts_capital_programming_context(self):
        self.assertTrue(
            _short_token_evidence_ok(
                "Go", "Strong Go programming and backend experience required."
            )
        )

    def test_go_accepts_lowercase_in_skills_list(self):
        self.assertTrue(
            _short_token_evidence_ok(
                "go", "Requirements: python, go, rust, and kotlin."
            )
        )

    def test_it_rejects_pronoun_it_is(self):
        self.assertFalse(
            _short_token_evidence_ok("it", "It is a fast-paced environment.")
        )
        self.assertFalse(
            _short_token_evidence_ok("IT", "Make sure it is documented.")
        )

    def test_it_accepts_information_technology(self):
        self.assertTrue(
            _short_token_evidence_ok(
                "IT", "Degree in information technology preferred."
            )
        )

    def test_it_accepts_all_caps_with_tech_collocate(self):
        self.assertTrue(
            _short_token_evidence_ok("IT", "Looking for an IT support specialist.")
        )

    def test_it_accepts_all_caps_in_skills_section(self):
        self.assertTrue(
            _short_token_evidence_ok(
                "it", "Skills: networking, IT, security, and Linux."
            )
        )

    def test_it_rejects_lowercase_near_skills_without_collocate(self):
        # Cue alone is insufficient for pronoun-heavy "it".
        self.assertFalse(
            _short_token_evidence_ok(
                "it", "Requirements: you will own it from design to delivery."
            )
        )


class ShortTokenFallbackExtractionTest(unittest.TestCase):
    def test_fallback_rejects_english_go(self):
        text = "We need people ready to go above and beyond for customers."
        skills = _extract_skills_fallback(text, [_GO_PATTERN])
        self.assertNotIn("Go", skills)

    def test_fallback_accepts_go_programming(self):
        text = "Requirements: experience with Go programming and Docker."
        skills = _extract_skills_fallback(text, [_GO_PATTERN])
        self.assertIn("Go", skills)

    def test_fallback_accepts_golang(self):
        text = "Built services in golang and kubernetes."
        skills = _extract_skills_fallback(text, [_GO_PATTERN])
        self.assertIn("Go", skills)

    def test_fallback_rejects_it_is(self):
        text = "It is important that candidates communicate clearly."
        skills = _extract_skills_fallback(text, [_IT_PATTERN])
        self.assertNotIn("IT", skills)

    def test_fallback_accepts_it_support(self):
        text = "We are hiring an IT support engineer for our helpdesk."
        skills = _extract_skills_fallback(text, [_IT_PATTERN])
        self.assertIn("IT", skills)

    def test_fallback_go_and_python_together(self):
        text = "Skills: Python, Go, and SQL."
        skills = _extract_skills_fallback(
            text, [("Python", r"\bpython\b"), _GO_PATTERN]
        )
        self.assertIn("Python", skills)
        self.assertIn("Go", skills)


class ShortTokenLlmPathTest(unittest.TestCase):
    def test_matched_known_go_rejected_without_evidence(self):
        raw = "Candidates who can go to client sites and deliver."
        payload = {
            "matched_known": [{"name": "go", "confidence": 1.0, "evidence": "go"}],
            "new_candidates": [],
        }
        llm = MagicMock()
        llm.generate.return_value = json.dumps(payload)

        with patch(
            "spejder.extractors.skill_extractor.extraction_llm._get_skill_patterns",
            return_value=[_GO_PATTERN],
        ):
            result = _extract_job_skills_llm_path(
                "jobs.db",
                raw,
                llm=llm,
                profile=AppConfig(),
            )

        self.assertTrue(result is None or "go" not in result.lower())

    def test_matched_known_go_accepted_with_programming_evidence(self):
        raw = "Requirements: Go programming experience required."
        payload = {
            "matched_known": [{"name": "go", "confidence": 1.0, "evidence": "Go"}],
            "new_candidates": [],
        }
        llm = MagicMock()
        llm.generate.return_value = json.dumps(payload)

        with patch(
            "spejder.extractors.skill_extractor.extraction_llm._get_skill_patterns",
            return_value=[_GO_PATTERN],
        ):
            result = _extract_job_skills_llm_path(
                "jobs.db",
                raw,
                llm=llm,
                profile=AppConfig(),
            )

        self.assertIsNotNone(result)
        self.assertIn("go", result.lower())

    def test_matched_known_it_accepted_with_support_context(self):
        raw = "Join our IT support team managing desktop systems."
        payload = {
            "matched_known": [{"name": "it", "confidence": 1.0, "evidence": "IT"}],
            "new_candidates": [],
        }
        llm = MagicMock()
        llm.generate.return_value = json.dumps(payload)

        with patch(
            "spejder.extractors.skill_extractor.extraction_llm._get_skill_patterns",
            return_value=[_IT_PATTERN],
        ):
            result = _extract_job_skills_llm_path(
                "jobs.db",
                raw,
                llm=llm,
                profile=AppConfig(),
            )

        self.assertIsNotNone(result)
        self.assertIn("it", result.lower())


class ShortTokenPhraseQualityTest(unittest.TestCase):
    def test_exact_it_allowed_for_evidence_gate(self):
        # Phrase quality used to kill "it" as a pronoun-led name; allowlist exempts it.
        self.assertTrue(_passes_phrase_quality("it"))
        self.assertTrue(_passes_phrase_quality("IT"))
        self.assertTrue(_passes_phrase_quality("go"))


if __name__ == "__main__":
    unittest.main()
