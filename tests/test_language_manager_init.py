"""Tests for language manager initialization."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig


class InitializationModuleImportsTest(unittest.TestCase):
    def test_initialization_module_imports(self):
        from spejder.managers.language_manager import initialization

        self.assertTrue(hasattr(initialization, "LANGUAGE_CHECKER_SELF_TESTS"))
        self.assertTrue(hasattr(initialization, "TRANSLATION_SELF_TEST"))
        self.assertTrue(hasattr(initialization, "initialize_language_checker_or_exit"))
        self.assertTrue(hasattr(initialization, "initialize_translation_or_exit"))


class InitializeLanguageCheckerOrExitTest(unittest.TestCase):
    def _profile(self) -> AppConfig:
        return AppConfig(
            language_checker_engine="fasttext",
            language_checker_model_path="/fake/lid.176.ftz",
        )

    def _checker_patches(self, is_danish_side_effect):
        return (
            patch(
                "spejder.managers.language_manager.initialization.load_profile",
                return_value=self._profile(),
            ),
            patch(
                "spejder.managers.language_manager.initialization._get_language_checker_detector"
            ),
            patch(
                "spejder.managers.language_manager.initialization.is_danish_text",
                side_effect=is_danish_side_effect,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.isfile",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.exists",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization._language_checker_model_looks_valid",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.getsize",
                return_value=500_000,
            ),
            patch(
                "spejder.managers.language_manager.initialization.fasttext",
                MagicMock(),
            ),
        )

    def test_initialize_language_checker_or_exit_runs_self_tests(self):
        from spejder.managers.language_manager.initialization import (
            LANGUAGE_CHECKER_SELF_TESTS,
            initialize_language_checker_or_exit,
        )

        expected_values = [expected for _, _, expected in LANGUAGE_CHECKER_SELF_TESTS]
        patches = self._checker_patches(expected_values)

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
            initialize_language_checker_or_exit("/fake/profile.json")

    def test_initialize_language_checker_self_test_failure(self):
        from spejder.managers.language_manager.initialization import (
            initialize_language_checker_or_exit,
        )

        patches = self._checker_patches([False, False, False])

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
            with self.assertRaises(SystemExit):
                initialize_language_checker_or_exit("/fake/profile.json")


class InitializeTranslationOrExitTest(unittest.TestCase):
    def test_initialize_translation_or_exit_happy_path(self):
        from spejder.managers.language_manager.initialization import (
            initialize_translation_or_exit,
        )

        profile = AppConfig(translation_model_path="/fake/translation-model")

        with (
            patch(
                "spejder.managers.language_manager.initialization.load_profile",
                return_value=profile,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.isfile",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.exists",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.isdir",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization._translation_model_looks_valid",
                return_value=True,
            ),
            patch(
                "spejder.managers.language_manager.initialization.MarianTokenizer",
                MagicMock(),
            ),
            patch(
                "spejder.managers.language_manager.initialization.MarianMTModel",
                MagicMock(),
            ),
            patch(
                "spejder.managers.language_manager.initialization.torch",
                MagicMock(),
            ),
            patch(
                "spejder.managers.language_manager.initialization.get_translation_runtime"
            ),
            patch(
                "spejder.managers.language_manager.initialization.is_danish_text",
                side_effect=[True, False],
            ),
            patch(
                "spejder.managers.language_manager.initialization.translate_title_to_english",
                return_value="Software Developer",
            ),
            patch(
                "spejder.managers.language_manager.initialization.normalize_title_compare_key",
                side_effect=["softwareudvikler", "software developer"],
            ),
        ):
            initialize_translation_or_exit("/fake/profile.json")
