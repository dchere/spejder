
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


from .translation_config import (
    configured_translation_slots,
    translation_slot_configuration_errors,
)
from .utils import (
    _language_checker_model_path,
    _language_checker_model_looks_valid,
    _print_language_checker_step,
    _fail_language_checker_init,
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
from .detection import _is_language_text, is_danish_text, is_ukrainian_text
from .titles import translate_title_to_english, normalize_title_text, normalize_title_compare_key

DANISH_JOB_TITLE_SAMPLE = "Vi søger en erfaren softwareudvikler til vores team i København"

LANGUAGE_CHECKER_SELF_TESTS: list[tuple[str, str, bool]] = [
    ("danish_job_title", DANISH_JOB_TITLE_SAMPLE, True),
    ("ukrainian_job_title", "Системний адміністратор для нашої команди", True),
    ("english_job_title", "Senior Software Engineer at Acme Corp", False),
    ("too_short", "ab", False),
]

TRANSLATION_SELF_TEST_SAMPLES: dict[str, tuple[str, str]] = {
    "da": (DANISH_JOB_TITLE_SAMPLE, "danish"),
    "uk": ("Системний адміністратор для нашої команди", "ukrainian"),
}


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


def _run_translation_slot_self_test(
    runtime_profile: AppConfig,
    slot_index: int,
    source_lang: str,
    model_path: str,
) -> None:
    _print_translation_step(
        f"slot {slot_index} configured: source={source_lang} model={model_path}"
    )
    if not os.path.exists(model_path):
        _fail_translation_init(f"slot {slot_index} model path does not exist")
    if not os.path.isdir(model_path):
        _fail_translation_init(f"slot {slot_index} model path is not a directory")
    _print_translation_step(f"slot {slot_index} model directory found")
    if not _translation_model_looks_valid(model_path):
        _fail_translation_init(f"slot {slot_index} model directory failed basic validation")
    _print_translation_step(f"slot {slot_index} model directory looks valid")

    try:
        get_translation_runtime(runtime_profile, source_lang=source_lang)
    except Exception as exc:
        _fail_translation_init(f"slot {slot_index} model initialization failed: {exc}")
    _print_translation_step(f"slot {slot_index} model initialized")

    sample = TRANSLATION_SELF_TEST_SAMPLES.get(source_lang)
    if not sample:
        _print_translation_step(
            f"slot {slot_index} translation output self-test skipped (no built-in sample for {source_lang})"
        )
        return

    sample_text, _label = sample
    detected_before = _is_language_text(sample_text, source_lang, runtime_profile)
    _print_translation_step(
        f"slot {slot_index} pre-translation language self-test: "
        f"expected=True actual={detected_before}"
    )
    if not detected_before:
        _fail_translation_init(f"slot {slot_index} pre-translation language self-test failed")

    try:
        translated = translate_title_to_english(
            sample_text,
            runtime_profile=runtime_profile,
            title_translation_cache={},
        )
    except Exception as exc:
        _fail_translation_init(f"slot {slot_index} translation self-test crashed: {exc}")

    translated_clean = normalize_title_text(translated)
    if not translated_clean:
        _fail_translation_init(f"slot {slot_index} translation self-test returned empty text")
    _print_translation_step(f"slot {slot_index} translation self-test output: {translated_clean}")

    detected_after = _is_language_text(translated_clean, source_lang, runtime_profile)
    _print_translation_step(
        f"slot {slot_index} post-translation language self-test: "
        f"expected=False actual={detected_after}"
    )
    if detected_after:
        _fail_translation_init(f"slot {slot_index} translated text still looks like source language")
    if normalize_title_compare_key(sample_text) == normalize_title_compare_key(translated_clean):
        _fail_translation_init(f"slot {slot_index} translation self-test did not change the source text")


def initialize_translation_or_exit(profile_path: str) -> None:
    _print_translation_step(f"loading profile from {profile_path}")
    if not profile_path or not os.path.isfile(profile_path):
        _fail_translation_init("profile file is missing")

    runtime_profile = load_profile(profile_path) if profile_path else None
    if not runtime_profile and profile_path is None:
        runtime_profile = load_profile(DEFAULT_PROFILE_PATH)

    config_errors = translation_slot_configuration_errors(runtime_profile)
    if config_errors:
        _fail_translation_init(config_errors[0])

    slots = configured_translation_slots(runtime_profile)
    if not slots:
        _fail_translation_init(
            "at least one paired language_translation_model_N and "
            "language_translation_source_N must be configured in profile"
        )

    if MarianTokenizer is None or MarianMTModel is None or torch is None:
        _fail_translation_init("translation runtime dependencies are not installed")

    for slot_index, (source_lang, model_path) in enumerate(slots, start=1):
        _run_translation_slot_self_test(
            runtime_profile,
            slot_index,
            source_lang,
            model_path,
        )

    _print_translation_step("self-test passed")


