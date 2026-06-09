from typing import Optional

from spejder.config import AppConfig
from spejder.jobs.suggestions import (
    _suggest_keywords_from_labeled_jobs,
    _suggest_missing_skills_from_applied_jobs,
    _unique_keywords,
)


def load_profile(profile_path: Optional[str]) -> AppConfig:
    from spejder.config import AppConfig
    profile = AppConfig.load(profile_path)

    base_include = profile.include_keywords or []
    base_exclude = profile.exclude_keywords or []
    learned_include = profile.learned_include_keywords or []
    learned_exclude = profile.learned_exclude_keywords or []

    profile.include_keywords = _unique_keywords(
        list(base_include) + list(learned_include)
    )
    profile.exclude_keywords = _unique_keywords(
        list(base_exclude) + list(learned_exclude)
    )
    profile.learned_include_keywords = _unique_keywords(
        list(learned_include))
    profile.learned_exclude_keywords = _unique_keywords(
        list(learned_exclude))
    profile.user_skills = _unique_keywords(
        list(profile.user_skills or [])
    )
    profile.blocked_skills = _unique_keywords(
        list(profile.blocked_skills or [])
    )
    profile.missing_skills_suggestions = _unique_keywords(
        list(profile.missing_skills_suggestions)
    )
    return profile



def update_profile_from_db_signals(
    db_path: str, profile_path: str, max_keywords: int = 20
) -> dict[str, int]:
    from spejder.config import AppConfig
    profile = AppConfig.load(profile_path)

    learned_include, learned_exclude, labeled_count = (
        _suggest_keywords_from_labeled_jobs(db_path, max_keywords=max_keywords)
    )
    profile.learned_include_keywords = learned_include
    profile.learned_exclude_keywords = learned_exclude

    max_missing_items = profile.missing_skills_max_items
    missing_skills = _suggest_missing_skills_from_applied_jobs(
        db_path, profile, max_items=max_missing_items
    )
    profile.missing_skills_suggestions = missing_skills

    profile.save(profile_path)

    return {
        "labeled_count": int(labeled_count),
        "learned_include_count": len(learned_include),
        "learned_exclude_count": len(learned_exclude),
        "missing_skills_count": len(missing_skills),
    }



