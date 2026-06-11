"""Tests for generic translation model slot configuration."""

import json
import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.managers.language_manager.translation_config import (
    configured_translation_slots,
    configured_translation_source_languages,
    primary_translation_slot,
    translation_model_path_for_language,
    translation_slot_configuration_errors,
)


class TranslationConfigTests(unittest.TestCase):
    def test_configured_slots_reads_numbered_fields(self):
        profile = AppConfig(
            language_translation_model_1="/fake/da-en",
            language_translation_source_1="da",
            language_translation_model_2="/fake/uk-en",
            language_translation_source_2="uk",
            language_translation_model_3="/fake/fr-en",
            language_translation_source_3="fr",
        )
        slots = configured_translation_slots(profile)
        self.assertEqual(
            slots,
            [
                ("da", os.path.abspath("/fake/da-en")),
                ("uk", os.path.abspath("/fake/uk-en")),
                ("fr", os.path.abspath("/fake/fr-en")),
            ],
        )
        self.assertEqual(configured_translation_source_languages(profile), frozenset({"da", "uk", "fr"}))
        self.assertEqual(primary_translation_slot(profile), ("da", os.path.abspath("/fake/da-en")))
        self.assertEqual(
            translation_model_path_for_language(profile, "uk"),
            os.path.abspath("/fake/uk-en"),
        )

    def test_load_migrates_legacy_danish_and_ukrainian_paths(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
            json.dump(
                {
                    "danish_translation_model_path": "/fake/da-en",
                    "ukrainian_translation_model_path": "/fake/uk-en",
                },
                handle,
            )
            profile_path = handle.name
        try:
            profile = AppConfig.load(profile_path)
            self.assertEqual(profile.language_translation_model_1, "/fake/da-en")
            self.assertEqual(profile.language_translation_source_1, "da")
            self.assertEqual(profile.language_translation_model_2, "/fake/uk-en")
            self.assertEqual(profile.language_translation_source_2, "uk")
            self.assertFalse(hasattr(profile, "danish_translation_model_path"))
        finally:
            os.unlink(profile_path)

    def test_load_migrates_translation_model_path_to_slot_1(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
            json.dump({"translation_model_path": "/fake/da-en"}, handle)
            profile_path = handle.name
        try:
            profile = AppConfig.load(profile_path)
            self.assertEqual(profile.language_translation_model_1, "/fake/da-en")
            self.assertEqual(profile.language_translation_source_1, "da")
        finally:
            os.unlink(profile_path)

    def test_load_migrates_ukrainian_only_legacy_path_to_slot_2(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as handle:
            json.dump({"ukrainian_translation_model_path": "/fake/uk-en"}, handle)
            profile_path = handle.name
        try:
            profile = AppConfig.load(profile_path)
            self.assertEqual(profile.language_translation_model_1, "")
            self.assertEqual(profile.language_translation_model_2, "/fake/uk-en")
            self.assertEqual(profile.language_translation_source_2, "uk")
            slots = configured_translation_slots(profile)
            self.assertEqual(slots, [("uk", os.path.abspath("/fake/uk-en"))])
        finally:
            os.unlink(profile_path)

    def test_configuration_errors_when_model_missing_source(self):
        profile = AppConfig(
            language_translation_model_1="/fake/da-en",
            language_translation_source_1="",
        )
        errors = translation_slot_configuration_errors(profile)
        self.assertEqual(len(errors), 1)
        self.assertIn("language_translation_source_1 is missing", errors[0])
        self.assertEqual(configured_translation_slots(profile), [])

    def test_configuration_errors_on_duplicate_source_languages(self):
        profile = AppConfig(
            language_translation_model_1="/fake/da-en-a",
            language_translation_source_1="da",
            language_translation_model_2="/fake/da-en-b",
            language_translation_source_2="da",
        )
        errors = translation_slot_configuration_errors(profile)
        self.assertEqual(len(errors), 1)
        self.assertIn("duplicate translation source 'da'", errors[0])


if __name__ == "__main__":
    unittest.main()
