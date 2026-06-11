import os
from typing import Optional

from spejder.core import AppConfig

TRANSLATION_MODEL_SLOT_COUNT = 3


def _translation_slot_fields(slot_index: int) -> tuple[str, str]:
    return (
        f"language_translation_model_{slot_index}",
        f"language_translation_source_{slot_index}",
    )


def configured_translation_slots(
    runtime_profile: Optional[AppConfig],
) -> list[tuple[str, str]]:
    profile = runtime_profile or AppConfig()
    slots: list[tuple[str, str]] = []
    for slot_index in range(1, TRANSLATION_MODEL_SLOT_COUNT + 1):
        model_key, source_key = _translation_slot_fields(slot_index)
        source = str(getattr(profile, source_key, "") or "").strip().lower()
        model_path = str(getattr(profile, model_key, "") or "").strip()
        if not model_path:
            continue
        model_path = os.path.abspath(os.path.expanduser(model_path))
        if source:
            slots.append((source, model_path))
    return slots


def configured_translation_source_languages(
    runtime_profile: Optional[AppConfig],
) -> frozenset[str]:
    return frozenset(source for source, _ in configured_translation_slots(runtime_profile))


def primary_translation_slot(
    runtime_profile: Optional[AppConfig],
) -> tuple[str, str]:
    slots = configured_translation_slots(runtime_profile)
    if not slots:
        return "", ""
    return slots[0]


def translation_model_path_for_language(
    runtime_profile: Optional[AppConfig],
    source_lang: str,
) -> str:
    normalized = str(source_lang or "").strip().lower()
    for source, model_path in configured_translation_slots(runtime_profile):
        if source == normalized:
            return model_path
    return ""
