
import logging
import os
from typing import Optional

from spejder.core import DEFAULT_PROFILE_PATH, AppConfig, load_profile


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


from .utils import (
    _language_checker_model_path,
    _language_checker_model_looks_valid,
    _print_language_checker_step,
    _fail_language_checker_init,
    _danish_translation_model_path,
    _ukrainian_translation_model_path,
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
from .detection import is_danish_text, is_ukrainian_text
from .titles import translate_title_to_english, normalize_title_text, normalize_title_compare_key

DANISH_JOB_TITLE_SAMPLE = "Vi søger en erfaren softwareudvikler til vores team i København"

LANGUAGE_CHECKER_SELF_TESTS: list[tuple[str, str, bool]] = [
    ("danish_job_title", DANISH_JOB_TITLE_SAMPLE, True),
    ("ukrainian_job_title", "Системний адміністратор для нашої команди", True),
    ("english_job_title", "Senior Software Engineer at Acme Corp", False),
    ("too_short", "ab", False),
]

TRANSLATION_SELF_TEST: tuple[str, bool] = (DANISH_JOB_TITLE_SAMPLE, True)
UKRAINIAN_TRANSLATION_SELF_TEST: tuple[str, bool] = ("Системний адміністратор", True)


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
            if label.startswith("ukrainian"):
                actual = is_ukrainian_text(sample_text, runtime_profile)
            elif label.startswith("danish"):
                actual = is_danish_text(sample_text, runtime_profile)
            else:
                actual = (
                    is_danish_text(sample_text, runtime_profile)
                    or is_ukrainian_text(sample_text, runtime_profile)
                )
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
    model_path = _danish_translation_model_path(runtime_profile)
    if not model_path:
        _fail_translation_init("danish_translation_model_path is not configured in profile")
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

    uk_model_path = _ukrainian_translation_model_path(runtime_profile)
    if uk_model_path:
        _print_translation_step(f"ukrainian model path configured: {uk_model_path}")
        if not os.path.exists(uk_model_path):
            _fail_translation_init("configured ukrainian model path does not exist")
        if not os.path.isdir(uk_model_path):
            _fail_translation_init("configured ukrainian model path is not a directory")
        if not _translation_model_looks_valid(uk_model_path):
            _fail_translation_init("ukrainian model directory failed basic validation")

        uk_sample_text, uk_expected_is_ukrainian = UKRAINIAN_TRANSLATION_SELF_TEST
        detected_uk_before = is_ukrainian_text(uk_sample_text, runtime_profile)
        _print_translation_step(
            "pre-translation ukrainian language self-test: "
            f"expected={uk_expected_is_ukrainian} actual={detected_uk_before}"
        )
        if detected_uk_before != uk_expected_is_ukrainian:
            _fail_translation_init("pre-translation ukrainian language self-test failed")

        try:
            uk_translated = translate_title_to_english(
                uk_sample_text,
                runtime_profile=runtime_profile,
                title_translation_cache={},
            )
        except Exception as exc:
            _fail_translation_init(f"ukrainian translation self-test crashed: {exc}")

        uk_translated_clean = normalize_title_text(uk_translated)
        if not uk_translated_clean:
            _fail_translation_init("ukrainian translation self-test returned empty text")
        _print_translation_step(
            f"ukrainian translation self-test output: {uk_translated_clean}"
        )

        detected_uk_after = is_ukrainian_text(uk_translated_clean, runtime_profile)
        _print_translation_step(
            f"post-translation ukrainian language self-test: expected=False actual={detected_uk_after}"
        )
        if detected_uk_after:
            _fail_translation_init("ukrainian translated text still looks Ukrainian")
        if normalize_title_compare_key(uk_sample_text) == normalize_title_compare_key(
            uk_translated_clean
        ):
            _fail_translation_init("ukrainian translation self-test did not change the source text")

    _print_translation_step("self-test passed")


