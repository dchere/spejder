"""Tests for language detection helpers."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.managers.language_manager.detection import (
    is_danish_text,
    is_ukrainian_text,
    translation_source_language,
)


class LanguageDetectionTest(unittest.TestCase):
    def _profile(self) -> AppConfig:
        return AppConfig(
            language_checker_engine="fasttext",
            language_checker_model_path="/fake/lid.176.ftz",
            language_checker_threshold=0.8,
            language_checker_min_letters=4,
            language_translation_model_1="/fake/opus-mt-da-en",
            language_translation_source_1="da",
            language_translation_model_2="/fake/opus-mt-uk-en",
            language_translation_source_2="uk",
        )

    def _mock_detector(self, label: str, probability: float):
        detector = MagicMock()
        detector.predict.return_value = ([f"__label__{label}"], [probability])
        return detector

    @patch("spejder.managers.language_manager.detection._get_language_checker_detector")
    def test_is_danish_text_positive(self, mock_get_detector):
        mock_get_detector.return_value = self._mock_detector("da", 0.95)
        self.assertTrue(is_danish_text("Vi søger en softwareudvikler", runtime_profile=self._profile()))

    @patch("spejder.managers.language_manager.detection._get_language_checker_detector")
    def test_is_ukrainian_text_positive(self, mock_get_detector):
        mock_get_detector.return_value = self._mock_detector("uk", 0.91)
        self.assertTrue(
            is_ukrainian_text("Системний адміністратор", runtime_profile=self._profile())
        )

    @patch("spejder.managers.language_manager.detection._get_language_checker_detector")
    def test_translation_source_language_returns_uk(self, mock_get_detector):
        mock_get_detector.return_value = self._mock_detector("uk", 0.91)
        self.assertEqual(
            translation_source_language(
                "Системний адміністратор для нашої команди",
                runtime_profile=self._profile(),
            ),
            "uk",
        )

    @patch("spejder.managers.language_manager.detection._get_language_checker_detector")
    def test_translation_source_language_ignores_english(self, mock_get_detector):
        mock_get_detector.return_value = self._mock_detector("en", 0.99)
        self.assertIsNone(
            translation_source_language(
                "Senior Software Engineer",
                runtime_profile=self._profile(),
            )
        )

    @patch("spejder.managers.language_manager.detection._get_language_checker_detector")
    def test_translation_source_language_rejects_low_confidence(self, mock_get_detector):
        mock_get_detector.return_value = self._mock_detector("uk", 0.4)
        self.assertIsNone(
            translation_source_language(
                "Системний адміністратор",
                runtime_profile=self._profile(),
            )
        )


if __name__ == "__main__":
    unittest.main()
