"""Profile + DB orchestration for automatic skill retention cleanup."""

from spejder.config import AppConfig
from spejder.db import cleanup_stale_low_share_skills_from_db
from spejder.db.utils import _normalize_skill_name_key
from spejder.managers.profile_manager import _remove_skill_from_profile

_PROTECTED_FLAG_FIELDS = (
    "user_skills",
    "missing_skills_suggestions",
    "unwanted_skills",
    "blocked_skills",
)


def _stale_cleanup_protected_keys(profile: AppConfig) -> set[str]:
    """Normalized keys from flag lists (not known_skill_patterns)."""
    protected: set[str] = set()
    for field in _PROTECTED_FLAG_FIELDS:
        values = getattr(profile, field, None) or []
        if not isinstance(values, list):
            continue
        for item in values:
            key = _normalize_skill_name_key(str(item))
            if key:
                protected.add(key)
    return protected


def run_stale_skill_cleanup(db_path: str, profile: AppConfig) -> dict:
    """Delete stale low-share DB skills and prune matching profile entries.

    Does not append ``blocked_skills`` or call bad-cloud hooks.
    """
    stats = cleanup_stale_low_share_skills_from_db(
        db_path,
        _stale_cleanup_protected_keys(profile),
    )
    deleted_names = list(stats.get("deleted_skill_names") or [])
    profile_removed = 0
    for name in deleted_names:
        info = _remove_skill_from_profile(profile, name)
        profile_removed += int(info.get("removed", 0) or 0)

    return {
        **stats,
        "profile_removed": profile_removed,
        "deleted_skill_names": deleted_names,
    }
