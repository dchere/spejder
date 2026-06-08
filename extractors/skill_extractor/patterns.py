"""Skill pattern registry and profile-to-DB migration."""

from contextlib import suppress
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    get_skill_patterns as get_db_skill_patterns,
)
from spejder.db import (
    migrate_profile_skill_patterns_to_db,
)

from .normalization import _normalize_skill_name
from .utils import _profile_skill_pattern_fields


def _get_skill_patterns(
    db_path: str, profile: Optional[AppConfig] = None
) -> list[tuple[str, str]]:
    from .filtering import _blocked_skill_keys

    blocked_keys = _blocked_skill_keys(profile)
    db_rows = get_db_skill_patterns(db_path, enabled_only=True)
    if db_rows:
        patterns = []
        for row in db_rows:
            name = str(row.get("name", "")).strip()
            pattern = str(row.get("pattern", "")).strip()
            if name and pattern and _normalize_skill_name(name).lower() not in blocked_keys:
                patterns.append((name, pattern))
        if patterns:
            return patterns

    raw = profile.known_skill_patterns if profile else []
    if not isinstance(raw, list) or not raw:
        raw = []

    patterns: list[tuple[str, str]] = []
    for item in raw:
        name, pattern = _profile_skill_pattern_fields(item)
        if not name or not pattern or _normalize_skill_name(name).lower() in blocked_keys:
            continue
        patterns.append((name, pattern))

    return patterns


def _ensure_skill_pattern_seed_migration(db_path: str, profile_path: str):
    """Ensure that skill patterns are migrated from the profile to the db."""
    with suppress(Exception):
        migrate_profile_skill_patterns_to_db(db_path, profile_path)
