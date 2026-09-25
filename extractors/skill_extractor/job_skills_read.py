"""Filtered job_skills read path (whitelist → blocked → bad cloud)."""

from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_job_skills_for_jobs, replace_job_skills
from spejder.db.utils import _normalize_skill_name_key

from .filtering import _filter_extracted_skills, _whitelist_skill_keys


def get_job_skills_filtered(
    db_path: str,
    job_id: int,
    profile: Optional[AppConfig] = None,
    *,
    rewrite_cache: bool = True,
) -> list[str]:
    """Return cached job skills after the extraction filter.

    Applies the same whitelist → blocked → bad-cloud gate used on extract.
    When ``rewrite_cache`` is true and the filter drops names, rewrite
    ``job_skills`` so the DB stays aligned with what display/scoring see.
    """
    if not job_id:
        return []
    return get_job_skills_filtered_for_jobs(
        db_path,
        [int(job_id)],
        profile,
        rewrite_cache=rewrite_cache,
    ).get(int(job_id), [])


def get_job_skills_filtered_for_jobs(
    db_path: str,
    job_ids: list[int],
    profile: Optional[AppConfig] = None,
    *,
    rewrite_cache: bool = True,
) -> dict[int, list[str]]:
    """Batch variant of ``get_job_skills_filtered`` (one whitelist + one DB read)."""
    ids = sorted({int(job_id) for job_id in job_ids if int(job_id or 0) > 0})
    if not ids:
        return {}

    raw_by_job = get_job_skills_for_jobs(db_path, ids)
    known_keys = _whitelist_skill_keys(profile, db_path)
    result: dict[int, list[str]] = {}
    for job_id in ids:
        cached = raw_by_job.get(job_id, [])
        if not cached:
            result[job_id] = []
            continue
        filtered = _filter_extracted_skills(cached, profile, db_path, known_keys)
        result[job_id] = filtered
        if rewrite_cache and _skill_key_set(cached) != _skill_key_set(filtered):
            replace_job_skills(db_path, job_id, filtered)
    return result


def _skill_key_set(skills: list[str]) -> set[str]:
    return {
        key
        for key in (_normalize_skill_name_key(skill) for skill in skills)
        if key
    }


__all__ = [
    "get_job_skills_filtered",
    "get_job_skills_filtered_for_jobs",
]
