"""Tests for graceful translation fallback when a model is not configured."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.managers.language_manager.text_translation import (
    translate_text_chunks_to_english,
    translate_text_to_english_if_needed,
)
from spejder.managers.language_manager.titles import translate_title_to_english


class TranslationFallbackTests(unittest.TestCase):
    def _profile(self) -> AppConfig:
        return AppConfig(
            language_translation_model_1="/fake/opus-mt-da-en",
            language_translation_source_1="da",
            language_checker_engine="fasttext",
            language_checker_model_path="/fake/lid.176.ftz",
            language_checker_threshold=0.8,
            language_checker_min_letters=4,
        )

    @patch("spejder.managers.language_manager.text_translation.get_translation_runtime", return_value=None)
    @patch(
        "spejder.managers.language_manager.text_translation.translation_source_language",
        return_value="uk",
    )
    def test_translate_text_to_english_if_needed_keeps_source_when_model_missing(
        self, _mock_source_lang, _mock_runtime
    ):
        source = "Системний адміністратор для нашої команди"
        result = translate_text_to_english_if_needed(source, runtime_profile=self._profile())
        self.assertEqual(result, source)

    @patch("spejder.managers.language_manager.text_translation.get_translation_runtime", return_value=None)
    def test_translate_text_chunks_to_english_keeps_chunks_when_model_missing(self, _mock_runtime):
        chunks = ["Første afsnit.", "Andet afsnit."]
        result = translate_text_chunks_to_english(chunks, runtime_profile=self._profile(), source_lang="da")
        self.assertEqual(result, chunks)

    @patch("spejder.managers.language_manager.titles.get_translation_runtime", return_value=None)
    @patch(
        "spejder.managers.language_manager.titles.translation_source_language",
        return_value="uk",
    )
    def test_translate_title_to_english_keeps_source_when_model_missing(
        self, _mock_source_lang, _mock_runtime
    ):
        title = "Системний адміністратор"
        result = translate_title_to_english(title, runtime_profile=self._profile())
        self.assertEqual(result, title)


if __name__ == "__main__":
    unittest.main()
