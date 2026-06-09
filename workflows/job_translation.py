from typing import Callable

from spejder.config import AppConfig
from spejder.managers.language_manager import (
    finalize_title_english as _finalize_title_english,
)
from spejder.managers.language_manager import (
    normalize_title_compare_key as _normalize_title_compare_key,
)
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)
from spejder.managers.language_manager import (
    translate_title_to_english as _translate_title_to_english,
)


def make_translate_job_entry_for_storage(
    runtime_profile: AppConfig,
    text_translation_cache: dict[str, str],
    title_translation_cache: dict[str, str],
) -> Callable[[dict], dict]:
    def _translate(entry: dict) -> dict:
        entry = dict(entry)
        entry["raw_text"] = _translate_text_to_english_if_needed(
            str(entry.get("raw_text", "") or ""),
            runtime_profile=runtime_profile,
            translation_cache=text_translation_cache,
        )
        title_value = str(entry.get("title", "") or "")
        try:
            title_english = _translate_title_to_english(
                title_value,
                runtime_profile=runtime_profile,
                title_translation_cache=title_translation_cache,
            )
        except Exception:
            try:
                title_english = _translate_text_to_english_if_needed(
                    title_value,
                    runtime_profile=runtime_profile,
                    translation_cache=text_translation_cache,
                )
            except Exception:
                title_english = title_value
        final_title_english = _finalize_title_english(title_english, title_value)
        if _normalize_title_compare_key(final_title_english) == _normalize_title_compare_key(title_value):
            final_title_english = ""
        entry["title_english"] = final_title_english
        return entry

    return _translate
