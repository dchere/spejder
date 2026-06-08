# pylint: disable=all
import re
import sys
import os
import json
import logging
from typing import Optional

from spejder.core import load_profile, AppConfig, DEFAULT_PROFILE_PATH


try:
    from ctranslate2 import Translator
    import sentencepiece as spm
    TRANSLATION_AVAILABLE = True
except ImportError:
    Translator = None
    spm = None
    TRANSLATION_AVAILABLE = False

try:
    import fasttext
    FASTTEXT_AVAILABLE = True
except ImportError:
    fasttext = None
    FASTTEXT_AVAILABLE = False

logger = logging.getLogger(__name__)

# Global singletons
_language_checker_detector = None
_translation_instance = None


from .utils import (
    _language_checker_model_path,
    _language_checker_model_looks_valid,
    _print_language_checker_step,
    _fail_language_checker_init,
    _translation_model_path,
    _translation_model_looks_valid,
    _print_translation_step,
    _fail_translation_init,
    language_checker_engine,
)
from .engines import (
    _get_language_checker_detector,
    get_translation_runtime,
    torch,
    MarianTokenizer,
    MarianMTModel,
)
from .detection import is_danish_text
from .titles import translate_title_to_english, normalize_title_text, normalize_title_compare_key

LANGUAGE_CHECKER_SELF_TESTS: list[tuple[str, str, bool]] = [
    ("danish_job_title", "Vi søger en erfaren softwareudvikler til vores team i København", True),
    ("english_job_title", "Senior Software Engineer at Acme Corp", False),
    ("too_short", "ab", False),
]

TRANSLATION_SELF_TEST: tuple[str, bool] = ("Softwareudvikler", True)


def initialize_language_checker_or_exit(profile_path: str) -> None:
    _print_language_checker_step(f"loading profile from {profile_path}")
    if not profile_path or not os.path.isfile(profile_path):
        _fail_language_checker_init("profile file is missing")

    runtime_profile = load_profile(profile_path) if profile_path else None
    if not runtime_profile and profile_path is None:
        runtime_profile = load_profile(DEFAULT_PROFILE_PATH)
    engine = language_checker_engine(runtime_profile)
    _print_language_checker_step(f"engine configured: {engine}")
    if engine != "fasttext":
        _fail_language_checker_init("unsupported engine configured; expected fasttext")
    if fasttext is None:
        _fail_language_checker_init("fasttext runtime is not installed")

    model_path = _language_checker_model_path(runtime_profile)
    if not model_path:
        _fail_language_checker_init("language_checker_model_path is not configured in profile")
    _print_language_checker_step(f"model path configured: {model_path}")

    if not os.path.exists(model_path):
        _fail_language_checker_init("configured model path does not exist")
    if not os.path.isfile(model_path):
        _fail_language_checker_init("configured model path is not a file")
    _print_language_checker_step("model file found")

    if not _language_checker_model_looks_valid(model_path):
        _fail_language_checker_init("model file failed basic validation")
    _print_language_checker_step(
        f"model file looks valid (size={os.path.getsize(model_path)} bytes)"
    )

    try:
        _get_language_checker_detector(runtime_profile)
    except Exception as exc:
        _fail_language_checker_init(f"model initialization failed: {exc}")
    _print_language_checker_step("model initialized")

    for label, sample_text, expected in LANGUAGE_CHECKER_SELF_TESTS:
        try:
            actual = is_danish_text(sample_text, runtime_profile)
        except Exception as exc:
            _fail_language_checker_init(f"self-test {label} crashed: {exc}")
        _print_language_checker_step(
            f"self-test {label}: expected={expected} actual={actual}"
        )
        if actual != expected:
            _fail_language_checker_init(f"self-test {label} failed")

    _print_language_checker_step("self-test passed")


def initialize_translation_or_exit(profile_path: str) -> None:
    _print_translation_step(f"loading profile from {profile_path}")
    if not profile_path or not os.path.isfile(profile_path):
        _fail_translation_init("profile file is missing")

    runtime_profile = load_profile(profile_path) if profile_path else None
    if not runtime_profile and profile_path is None:
        runtime_profile = load_profile(DEFAULT_PROFILE_PATH)
    model_path = _translation_model_path(runtime_profile)
    if not model_path:
        _fail_translation_init("translation_model_path is not configured in profile")
    _print_translation_step(f"model path configured: {model_path}")

    if not os.path.exists(model_path):
        _fail_translation_init("configured model path does not exist")
    if not os.path.isdir(model_path):
        _fail_translation_init("configured model path is not a directory")
    _print_translation_step("model directory found")

    if not _translation_model_looks_valid(model_path):
        _fail_translation_init("model directory failed basic validation")
    _print_translation_step("model directory looks valid")

    if MarianTokenizer is None or MarianMTModel is None or torch is None:
        _fail_translation_init("translation runtime dependencies are not installed")

    try:
        get_translation_runtime(runtime_profile)
    except Exception as exc:
        _fail_translation_init(f"model initialization failed: {exc}")
    _print_translation_step("model initialized")

    sample_text, expected_is_danish = TRANSLATION_SELF_TEST
    detected_before = is_danish_text(sample_text, runtime_profile)
    _print_translation_step(
        f"pre-translation language self-test: expected={expected_is_danish} actual={detected_before}"
    )
    if detected_before != expected_is_danish:
        _fail_translation_init("pre-translation language self-test failed")

    try:
        translated = translate_title_to_english(
            sample_text,
            runtime_profile=runtime_profile,
            title_translation_cache={},
        )
    except Exception as exc:
        _fail_translation_init(f"translation self-test crashed: {exc}")

    translated_clean = normalize_title_text(translated)
    if not translated_clean:
        _fail_translation_init("translation self-test returned empty text")
    _print_translation_step(f"translation self-test output: {translated_clean}")

    detected_after = is_danish_text(translated_clean, runtime_profile)
    _print_translation_step(
        f"post-translation language self-test: expected=False actual={detected_after}"
    )
    if detected_after:
        _fail_translation_init("translated text still looks Danish")
    if normalize_title_compare_key(sample_text) == normalize_title_compare_key(translated_clean):
        _fail_translation_init("translation self-test did not change the source text")

    _print_translation_step("self-test passed")


