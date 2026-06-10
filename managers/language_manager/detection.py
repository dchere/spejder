
import logging
from typing import Literal, Optional, cast

from spejder.core import AppConfig
from spejder.managers.language_manager.engines import _get_language_checker_detector
from .utils import (
    _language_checker_letter_count,
    _language_checker_min_letters,
    _language_checker_threshold,
)

logger = logging.getLogger(__name__)

TranslationSourceLanguage = Literal["da", "uk"]
TRANSLATION_SOURCE_LANGUAGES = frozenset({"da", "uk"})


def _top_language_label(
    text: str, runtime_profile: Optional[AppConfig] = None
) -> tuple[str, float]:
    sample = " ".join((text or "").split()).strip()
    if not sample:
        return "", 0.0
    if _language_checker_letter_count(sample) < _language_checker_min_letters(runtime_profile):
        return "", 0.0

    detector = _get_language_checker_detector(runtime_profile)
    if detector is None:
        return "", 0.0

    try:
        labels, probabilities = detector.predict(sample.replace("\n", " "), k=1)
    except Exception:
        return "", 0.0

    if not labels or not probabilities:
        return "", 0.0

    top_label = str(labels[0] or "").strip().lower()
    if top_label.startswith("__label__"):
        top_label = top_label[len("__label__") :]
    top_probability = float(probabilities[0] or 0.0)
    return top_label, top_probability


def _is_language_text(
    text: str,
    language_code: str,
    runtime_profile: Optional[AppConfig] = None,
) -> bool:
    label, probability = _top_language_label(text, runtime_profile=runtime_profile)
    if not label:
        return False
    return (
        label == language_code
        and probability >= _language_checker_threshold(runtime_profile)
    )


def is_danish_text(text: str, runtime_profile: Optional[AppConfig] = None) -> bool:
    return _is_language_text(text, "da", runtime_profile=runtime_profile)


def is_ukrainian_text(text: str, runtime_profile: Optional[AppConfig] = None) -> bool:
    return _is_language_text(text, "uk", runtime_profile=runtime_profile)


def translation_source_language(
    text: str, runtime_profile: Optional[AppConfig] = None
) -> Optional[TranslationSourceLanguage]:
    label, probability = _top_language_label(text, runtime_profile=runtime_profile)
    if label not in TRANSLATION_SOURCE_LANGUAGES:
        return None
    if probability < _language_checker_threshold(runtime_profile):
        return None
    return cast(TranslationSourceLanguage, label)
