from typing import Mapping

_ENTRY_FIELD_KEYS = ("title", "company", "place", "work_type", "raw_text", "source")


def merge_entry_fields(*maps: Mapping) -> dict:
    """First non-empty value per `_ENTRY_FIELD_KEYS` wins; later maps never overwrite."""
    merged: dict = {}
    for mapping in maps:
        if not mapping:
            continue
        for key in _ENTRY_FIELD_KEYS:
            if merged.get(key):
                continue
            value = mapping.get(key)
            if value:
                merged[key] = value
    return merged
