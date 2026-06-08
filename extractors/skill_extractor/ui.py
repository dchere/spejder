"""Skills tab data builder for the dashboard UI."""

from spejder.config import AppConfig
from spejder.db import get_skill_patterns as get_db_skill_patterns

from .filtering import _blocked_skill_keys
from .normalization import _normalize_skill_name


def _build_skills_tab_items(db_path: str, profile: AppConfig) -> list[dict]:
    """Build the skills tab items for the UI."""
    blocked_keys = _blocked_skill_keys(profile)
    user_keys = {
        _normalize_skill_name(str(s)).lower()
        for s in (profile.user_skills or [])
        if _normalize_skill_name(str(s)) and _normalize_skill_name(str(s)).lower() not in blocked_keys
    }
    learn_keys = {
        _normalize_skill_name(str(s)).lower()
        for s in (profile.missing_skills_suggestions or [])
        if _normalize_skill_name(str(s)) and _normalize_skill_name(str(s)).lower() not in blocked_keys
    }

    by_key: dict[str, dict] = {}

    def upsert(name: str, source: str, occurrences: int = 0, weight: float = 0.0):
        clean = _normalize_skill_name(name)
        key = clean.lower()
        if not key or key in blocked_keys:
            return
        row = by_key.get(key)
        if row is None:
            row = {
                "name": clean,
                "key": key,
                "source": source,
                "occurrences": int(occurrences),
                "weight": float(weight),
                "has_skill": key in user_keys,
                "want_to_learn": key in learn_keys,
            }
            by_key[key] = row
            return
        if source == "db":
            row["source"] = "db"
            row["occurrences"] = max(int(row.get("occurrences", 0)), int(occurrences))
            row["weight"] = max(float(row.get("weight", 0.0)), float(weight))
        row["has_skill"] = row["has_skill"] or (key in user_keys)
        row["want_to_learn"] = row["want_to_learn"] or (key in learn_keys)

    for item in get_db_skill_patterns(db_path, enabled_only=False):
        upsert(
            str(item.get("name", "")),
            "db",
            occurrences=int(item.get("occurrences", 0) or 0),
            weight=float(item.get("weight", 0.0) or 0.0),
        )

    for item in profile.known_skill_patterns or []:
        if isinstance(item, dict):
            upsert(str(item.get("name", "")), "profile")

    for item in profile.user_skills or []:
        upsert(str(item), "profile")

    for item in profile.missing_skills_suggestions or []:
        upsert(str(item), "profile")

    rows = list(by_key.values())
    rows.sort(key=lambda x: (x["name"].lower(), -x.get("occurrences", 0)))
    return rows
