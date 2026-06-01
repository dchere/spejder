"""
Profile manager handling profile operations like saving, toggling and blocking skills.
"""
from spejder.config import AppConfig

# pylint: disable=too-many-branches,too-many-statements,too-many-locals

def _normalize_skill_name(skill_name: str) -> str:
    if not skill_name:
        return ""
    return str(skill_name).strip()

def _save_profile(profile_path: str, profile: AppConfig) -> None:
    profile.save(profile_path)

def _toggle_profile_skill(profile: AppConfig, field: str, skill_name: str, enabled: bool) -> bool:
    skill = _normalize_skill_name(skill_name)
    key = skill.lower()
    if not key:
        return False

    values = getattr(profile, field, [])
    if not isinstance(values, list):
        values = []

    seen = set()
    cleaned = []
    for item in values:
        normalized = _normalize_skill_name(str(item))
        normalized_key = normalized.lower()
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized)

    had = key in seen
    changed = False
    if enabled and not had:
        cleaned.append(skill)
        changed = True
    if not enabled and had:
        cleaned = [item for item in cleaned if item.lower() != key]
        changed = True

    setattr(profile, field, cleaned)
    return changed

def _remove_skill_from_profile(profile: AppConfig, skill_name: str) -> dict[str, int]:
    key = _normalize_skill_name(skill_name).lower()
    if not key:
        return {"removed": 0}

    removed = 0
    list_fields = [
        "user_skills",
        "missing_skills_suggestions",
        "include_keywords",
        "exclude_keywords",
        "learned_include_keywords",
        "learned_exclude_keywords",
    ]
    for field in list_fields:
        values = getattr(profile, field, [])
        if not isinstance(values, list):
            continue
        kept = []
        for item in values:
            normalized = _normalize_skill_name(str(item))
            if normalized.lower() == key:
                removed += 1
                continue
            kept.append(item)
        setattr(profile, field, kept)

    patterns = profile.known_skill_patterns
    if isinstance(patterns, list):
        kept_patterns = []
        for item in patterns:
            if not isinstance(item, dict):
                kept_patterns.append(item)
                continue
            name = _normalize_skill_name(str(item.get("name", "")))
            if name.lower() == key:
                removed += 1
                continue
            kept_patterns.append(item)
        setattr(profile, "known_skill_patterns", kept_patterns)

    return {"removed": int(removed)}

def _block_skill_in_profile(profile: AppConfig, skill_name: str) -> dict[str, int]:
    skill = _normalize_skill_name(skill_name)
    key = skill.lower()
    if not key:
        return {"blocked_added": 0, "removed": 0}

    removed_info = _remove_skill_from_profile(profile, skill)
    blocked_values = profile.blocked_skills
    if not isinstance(blocked_values, list):
        blocked_values = []

    cleaned = []
    seen = set()
    for item in blocked_values:
        normalized = _normalize_skill_name(str(item))
        normalized_key = normalized.lower()
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized)

    if key not in seen:
        cleaned.append(skill)

    setattr(profile, "blocked_skills", cleaned)

    return {"blocked_added": 1 if key not in seen else 0, "removed": removed_info.get("removed", 0)}
