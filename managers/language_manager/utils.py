
import logging
import os
import re
from typing import Optional

from spejder.core import AppConfig

from .translation_config import (
    configured_translation_slots,
    primary_translation_slot,
    translation_model_path_for_language as _translation_model_path_for_language,
)

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


def _print_translation_step(message: str) -> None:
    print(f"[translation init] {message}")


def _fail_translation_init(message: str) -> None:
    _print_translation_step(message)
    raise SystemExit(1)


def _language_checker_model_path(runtime_profile: Optional[AppConfig]) -> str:
    profile = runtime_profile or {}
    value = str(profile.language_checker_model_path or "")
    return os.path.abspath(os.path.expanduser(value.strip())) if value.strip() else ""


def _language_checker_threshold(runtime_profile: Optional[AppConfig]) -> float:
    profile = runtime_profile or {}
    try:
        value = float(profile.language_checker_threshold or 0.8)
    except Exception:
        value = 0.8
    return max(0.0, min(1.0, value))


def _language_checker_min_letters(runtime_profile: Optional[AppConfig]) -> int:
    profile = runtime_profile or {}
    try:
        value = int(profile.language_checker_min_letters or 4)
    except Exception:
        value = 4
    return max(1, value)


def _language_checker_letter_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"[^\W\d_]", text, flags=re.UNICODE))

def language_checker_engine(runtime_profile: Optional[AppConfig]) -> Optional[str]:
    return runtime_profile.language_checker_engine if runtime_profile else None

def _language_checker_cache_key(runtime_profile: Optional[AppConfig]) -> str:
    return f"{language_checker_engine(runtime_profile)}:{_language_checker_model_path(runtime_profile)}"


def _language_checker_model_looks_valid(model_path: str) -> bool:
    if not model_path or not os.path.isfile(model_path):
        return False
    ext = os.path.splitext(model_path)[1].lower()
    if ext not in {".bin", ".ftz"}:
        return False
    try:
        return os.path.getsize(model_path) >= 100_000
    except OSError:
        return False


def _translation_model_looks_valid(model_path: str) -> bool:
    if not model_path or not os.path.isdir(model_path):
        return False
    required_files = ["config.json", "source.spm", "target.spm"]
    if not all(os.path.isfile(os.path.join(model_path, name)) for name in required_files):
        return False
    return any(
        os.path.isfile(os.path.join(model_path, name))
        for name in ["pytorch_model.bin", "model.safetensors"]
    )


def _print_language_checker_step(message: str) -> None:
    print(f"Language checker init: {message}")


def _fail_language_checker_init(message: str) -> None:
    _print_language_checker_step(message)
    raise SystemExit(1)


