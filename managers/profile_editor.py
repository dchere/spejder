"""Profile editor: field metadata and save/validate helpers for the dashboard UI."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

from spejder.config import AppConfig
from spejder.managers.profile_editor_fields import (
    GROUP_ORDER,
    GROUP_TITLES,
    PROFILE_FIELD_META,
    READONLY_FIELDS,
)

__all__ = [
    "GROUP_ORDER",
    "GROUP_TITLES",
    "PROFILE_FIELD_META",
    "READONLY_FIELDS",
    "assert_field_meta_complete",
    "build_profile_get_response",
    "field_metadata_list",
    "groups_metadata",
    "merge_profile_updates",
    "save_profile_updates",
    "validation_errors_by_field",
]


def assert_field_meta_complete() -> None:
    meta_keys = set(PROFILE_FIELD_META)
    model_keys = set(AppConfig.model_fields)
    if meta_keys != model_keys:
        missing = sorted(model_keys - meta_keys)
        extra = sorted(meta_keys - model_keys)
        raise AssertionError(
            f"PROFILE_FIELD_META mismatch: missing={missing!r} extra={extra!r}"
        )


assert_field_meta_complete()


def field_metadata_list() -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for name, meta in PROFILE_FIELD_META.items():
        entry = {"name": name, **meta}
        fields.append(entry)
    return fields


def groups_metadata() -> list[dict[str, str]]:
    return [{"id": group_id, "title": GROUP_TITLES[group_id]} for group_id in GROUP_ORDER]


def build_profile_get_response(runtime_profile: AppConfig) -> dict[str, Any]:
    return {
        "ok": True,
        "values": runtime_profile.model_dump(),
        "fields": field_metadata_list(),
        "groups": groups_metadata(),
    }


def validation_errors_by_field(exc: ValidationError) -> dict[str, str]:
    errors: dict[str, str] = {}
    for err in exc.errors():
        loc = err.get("loc") or ()
        key = str(loc[0]) if loc else "_form"
        msg = str(err.get("msg") or "invalid")
        if key not in errors:
            errors[key] = msg
    return errors


def merge_profile_updates(
    runtime_profile: AppConfig,
    updates: dict[str, Any],
) -> AppConfig:
    if not isinstance(updates, dict):
        raise TypeError("profile save body must be a JSON object")

    unknown = sorted(set(updates) - set(AppConfig.model_fields))
    if unknown:
        raise ValueError(f"unknown profile fields: {', '.join(unknown)}")

    data = runtime_profile.model_dump()
    for key, value in updates.items():
        if key in READONLY_FIELDS:
            continue
        data[key] = value

    for key in READONLY_FIELDS:
        data[key] = getattr(runtime_profile, key)

    return AppConfig.model_validate(data)


def save_profile_updates(
    runtime_profile: AppConfig,
    updates: dict[str, Any],
    profile_path: str,
    reload_runtime_profile: Callable[[], None],
) -> AppConfig:
    """Validate updates, write profile_path, then reload the live runtime object.

    Ordering: merge against the current in-memory profile → persist to disk →
    reload into the live object. Partial updates only overwrite keys present in
    ``updates`` (readonly keys are ignored). If disk write succeeds and reload
    fails, the file is ahead of memory until the next successful reload.
    """
    validated = merge_profile_updates(runtime_profile, updates)
    validated.save(profile_path)
    reload_runtime_profile()
    return validated
