"""Tests for language manager initialization."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig


class InitializationModuleImportsTest(unittest.TestCase):
    def test_initialization_module_imports(self):
        from spejder.managers.language_manager import initialization

        self.assertTrue(hasattr(initialization, "LANGUAGE_CHECKER_SELF_TESTS"))
        self.assertTrue(hasattr(initialization, "TRANSLATION_SELF_TEST_SAMPLES"))
        self.assertTrue(hasattr(initialization, "initialize_language_checker_or_exit"))
        self.assertTrue(hasattr(initialization, "initialize_translation_or_exit"))


class InitializeLanguageCheckerOrExitTest(unittest.TestCase):
    def _profile(self) -> AppConfig:
        return AppConfig(
            language_checker_engine="fasttext",
            language_checker_model_path="/fake/lid.176.ftz",
        )

    def _checker_patches(self, is_danish_side_effect, is_ukrainian_side_effect):
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
                "spejder.managers.language_manager.initialization.is_ukrainian_text",
                side_effect=is_ukrainian_side_effect,
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
            initialize_language_checker_or_exit,
        )

        patches = self._checker_patches(
            is_danish_side_effect=[True, False, False],
            is_ukrainian_side_effect=[True, False, False],
        )

        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4],
            patches[5],
            patches[6],
            patches[7],
            patches[8],
        ):
            initialize_language_checker_or_exit("/fake/profile.json")

    def test_initialize_language_checker_self_test_failure(self):
        from spejder.managers.language_manager.initialization import (
            initialize_language_checker_or_exit,
        )

        patches = self._checker_patches(
            is_danish_side_effect=[False],
            is_ukrainian_side_effect=[True],
        )

        with (
            patches[0],
            patches[1],
            patches[2],
            patches[3],
            patches[4],
            patches[5],
            patches[6],
            patches[7],
            patches[8],
        ):
            with self.assertRaises(SystemExit):
                initialize_language_checker_or_exit("/fake/profile.json")


class InitializeTranslationOrExitTest(unittest.TestCase):
    def test_initialize_translation_or_exit_rejects_model_without_source(self):
        from spejder.managers.language_manager.initialization import (
            initialize_translation_or_exit,
        )

        profile = AppConfig(
            language_translation_model_1="/fake/translation-model",
            language_translation_source_1="",
        )

        with (
            patch(
                "spejder.managers.language_manager.initialization.load_profile",
                return_value=profile,
            ),
            patch(
                "spejder.managers.language_manager.initialization.os.path.isfile",
                return_value=True,
            ),
        ):
            with self.assertRaises(SystemExit):
                initialize_translation_or_exit("/fake/profile.json")

    def test_initialize_translation_or_exit_happy_path(self):
        from spejder.managers.language_manager.initialization import (
            initialize_translation_or_exit,
        )

        profile = AppConfig(
            language_translation_model_1="/fake/translation-model",
            language_translation_source_1="da",
        )

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
                "spejder.managers.language_manager.initialization._is_language_text",
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

    def test_initialize_translation_or_exit_with_ukrainian_model(self):
        from spejder.managers.language_manager.initialization import (
            initialize_translation_or_exit,
        )

        profile = AppConfig(
            language_translation_model_1="/fake/translation-model-da",
            language_translation_source_1="da",
            language_translation_model_2="/fake/translation-model-uk",
            language_translation_source_2="uk",
        )

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
                "spejder.managers.language_manager.initialization._is_language_text",
                side_effect=[True, False, True, False],
            ),
            patch(
                "spejder.managers.language_manager.initialization.translate_title_to_english",
                side_effect=["Software Developer", "System Administrator"],
            ),
            patch(
                "spejder.managers.language_manager.initialization.normalize_title_compare_key",
                side_effect=[
                    "softwareudvikler",
                    "software developer",
                    "системнийадміністратор",
                    "systemadministrator",
                ],
            ),
        ):
            initialize_translation_or_exit("/fake/profile.json")


