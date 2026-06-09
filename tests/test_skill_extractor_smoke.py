"""Smoke tests for skill_extractor package structure and public API."""

import unittest

import spejder.extractors.skill_extractor as skill_extractor


class SkillExtractorSmokeTest(unittest.TestCase):
    def test_public_api_symbols_exist(self):
        for name in skill_extractor.__all__:
            self.assertTrue(hasattr(skill_extractor, name), msg=f"missing export: {name}")

    def test_submodules_import(self):
        from spejder.extractors.skill_extractor import (  # noqa: F401
            antipattern_sync,
            antipattern_synthesis,
            antipattern_validation,
            cleanup,
            constants,
            extraction,
            extraction_fallback,
            extraction_llm,
            extraction_prompt,
            filtering,
            learning,
            normalization,
            patterns,
            ui,
            user_sync,
            utils,
        )

    def test_normalize_skill_name_strips_prefixes(self):
        from spejder.extractors.skill_extractor import _normalize_skill_name

        self.assertEqual(_normalize_skill_name("Experience with Python"), "python")
        self.assertEqual(_normalize_skill_name(""), "")

    def test_skill_to_regex(self):
        from spejder.extractors.skill_extractor.utils import _skill_to_regex

        pattern = _skill_to_regex("Python 3")
        self.assertIn(r"\b", pattern)
        self.assertIn("Python", pattern)


if __name__ == "__main__":
    unittest.main()
