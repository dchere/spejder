"""Stale low job-share skill pattern cleanup."""

from __future__ import annotations

from typing import Iterable

from .connection import _connect
from .maintenance import JOB_RETENTION_DAYS
from .skills_delete import delete_skill_from_db
from .skills_rank import (
    count_job_links_for_skills,
    count_jobs_with_skill_links,
    position_pct,
)
from .utils import _normalize_skill_name_key

SKILL_RETENTION_DAYS = JOB_RETENTION_DAYS
SKILL_STALE_JOB_SHARE_PCT_THRESHOLD = 0.1


def cleanup_stale_low_share_skills_from_db(
    db_path: str,
    protected_keys: set[str] | Iterable[str],
) -> dict:
    """Delete old skill_patterns with Job share below the Skills-tab threshold.

    Eligible rows must have a parseable ``created_at`` older than
    ``SKILL_RETENTION_DAYS``. Protected keys (normalized) are skipped.
    Deletes only via ``delete_skill_from_db``; does not touch profile or cloud.
    """
    protected: set[str] = set()
    for raw in protected_keys or []:
        key = _normalize_skill_name_key(str(raw))
        if key:
            protected.add(key)

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name, name_key
            FROM skill_patterns
            WHERE created_at IS NOT NULL
              AND TRIM(created_at) <> ''
              AND datetime(replace(created_at, 'T', ' ')) < datetime('now', ?)
            """,
            (f"-{int(SKILL_RETENTION_DAYS)} days",),
        )
        candidates = [
            (str(row[0]), str(row[1]))
            for row in cur.fetchall()
            if row and row[0] and row[1]
        ]
    finally:
        conn.close()

    skills_considered = len(candidates)
    skills_skipped = 0
    skills_deleted = 0
    skill_rows_deleted = 0
    job_skill_links_deleted = 0
    affected_job_ids: set[int] = set()
    deleted_skill_names: list[str] = []

    if not candidates:
        return {
            "skills_deleted": 0,
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
            "skills_considered": 0,
            "skills_skipped": 0,
            "deleted_skill_names": [],
        }

    jobs_with_skills = count_jobs_with_skill_links(db_path)
    names = [name for name, _key in candidates]
    link_counts = count_job_links_for_skills(db_path, names)

    for name, name_key in candidates:
        if name_key in protected:
            skills_skipped += 1
            continue
        share = position_pct(int(link_counts.get(name, 0)), jobs_with_skills)
        if share >= SKILL_STALE_JOB_SHARE_PCT_THRESHOLD:
            skills_skipped += 1
            continue

        deleted = delete_skill_from_db(db_path, name)
        rows = int(deleted.get("skill_rows_deleted", 0) or 0)
        links = int(deleted.get("job_skill_links_deleted", 0) or 0)
        if rows <= 0 and links <= 0:
            skills_skipped += 1
            continue

        skills_deleted += 1
        skill_rows_deleted += rows
        job_skill_links_deleted += links
        deleted_skill_names.append(name)
        for job_id in deleted.get("affected_job_ids", []):
            affected_job_ids.add(int(job_id))

    return {
        "skills_deleted": skills_deleted,
        "skill_rows_deleted": skill_rows_deleted,
        "job_skill_links_deleted": job_skill_links_deleted,
        "affected_job_ids": sorted(affected_job_ids),
        "skills_considered": skills_considered,
        "skills_skipped": skills_skipped,
        "deleted_skill_names": deleted_skill_names,
    }
