"""Skills tab data builder for the dashboard UI."""

from spejder.config import AppConfig
from spejder.db import (
    count_job_links_for_skills,
    count_jobs_with_skill_links,
    get_skill_patterns as get_db_skill_patterns,
)

from .filtering import _blocked_skill_keys
from .normalization import _normalize_skill_name


def _position_pct(position_count: int, jobs_with_skills: int) -> float:
    if jobs_with_skills <= 0 or position_count <= 0:
        return 0.0
    return round(100.0 * position_count / jobs_with_skills, 1)


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
    jobs_with_skills = count_jobs_with_skill_links(db_path)
    link_counts = count_job_links_for_skills(db_path, [row["name"] for row in rows])
    for row in rows:
        position_count = int(link_counts.get(row["name"], 0))
        row["jobs_with_skills"] = jobs_with_skills
        row["position_count"] = position_count
        row["position_pct"] = _position_pct(position_count, jobs_with_skills)

    rows.sort(key=lambda x: x["name"].lower())
    return rows
