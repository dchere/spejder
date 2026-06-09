
import re
import sys
import os
import json
import logging
from typing import Optional

import sys

from spejder.core import load_profile, load_runtime_profile, AppConfig, DEFAULT_PROFILE_PATH
from spejder.managers.language_manager.engines import _get_language_checker_detector


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


from .utils import _language_checker_min_letters, _language_checker_letter_count, _language_checker_threshold, _language_checker_cache_key
from .engines import language_checker_engine
def is_danish_text(text: str, runtime_profile: Optional[AppConfig] = None) -> bool:
    sample = " ".join((text or "").split()).strip()
    if not sample:
        return False
    if _language_checker_letter_count(sample) < _language_checker_min_letters(runtime_profile):
        return False

    detector = _get_language_checker_detector(runtime_profile)
    if detector is None:
        return False

    try:
        labels, probabilities = detector.predict(sample.replace("\n", " "), k=1)
    except Exception:
        return False

    if not labels or not probabilities:
        return False

    top_label = str(labels[0] or "").strip().lower()
    top_probability = float(probabilities[0] or 0.0)
    return top_label == "__label__da" and top_probability >= _language_checker_threshold(
        runtime_profile
    )


