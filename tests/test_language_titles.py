"""Tests for title normalization and translation helpers."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.managers.language_manager.titles import (
    get_title_english_for_row,
    normalize_title_compare_key,
    normalize_title_text,
)


class NormalizeTitleTextTest(unittest.TestCase):
    def test_collapses_whitespace(self):
        self.assertEqual(normalize_title_text("foo   bar"), "foo bar")

    def test_strips_garbage_markers_case_insensitively(self):
        self.assertEqual(
            normalize_title_text("Translated Title: Software Engineer"),
            "Software Engineer",
        )


class NormalizeTitleCompareKeyTest(unittest.TestCase):
    def test_preserves_cyrillic_letters(self):
        key = normalize_title_compare_key("Системний адміністратор")
        self.assertTrue(key)
        self.assertNotEqual(key, normalize_title_compare_key("System Administrator"))


class GetTitleEnglishForRowFallbackTest(unittest.TestCase):
    @patch("spejder.managers.language_manager.titles.set_job_title_english")
    @patch("spejder.managers.language_manager.titles.translate_text_to_english_if_needed")
    @patch("spejder.managers.language_manager.titles.translate_title_to_english")
    def test_title_translator_runtime_error_falls_back_to_text_translator(
        self,
        mock_translate_title,
        mock_translate_text,
        _mock_set_title,
    ):
        mock_translate_title.side_effect = RuntimeError("title translator failed")
        mock_translate_text.return_value = "System Administrator"
        row = {"id": 0, "title": "Системний адміністратор", "title_english": ""}

        result = get_title_english_for_row("jobs.db", row, runtime_profile=AppConfig())

        self.assertEqual(result, "System Administrator")
        mock_translate_text.assert_called_once()


if __name__ == "__main__":
    unittest.main()
