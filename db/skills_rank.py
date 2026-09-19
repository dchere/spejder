"""job_skills rank and count."""
from typing import Optional

from .connection import _connect
from .utils import _normalize_skill_name_key


def position_pct(position_count: int, jobs_with_skills: int) -> float:
    """Job-share percent for Skills tab and stale-skill cleanup (one decimal)."""
    if jobs_with_skills <= 0 or position_count <= 0:
        return 0.0
    return round(100.0 * position_count / jobs_with_skills, 1)


def count_jobs_with_skill_links(db_path: str) -> int:
    """Return how many distinct jobs have at least one cached skill link."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT job_id) FROM job_skills")
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def get_top_skills_by_job_links(
    db_path: str,
    limit: int,
    exclude_keys: Optional[set[str]] = None,
) -> list[str]:
    """Return skill names with at least one job_skills link, ranked by link count."""
    if limit <= 0:
        return []

    exclude: set[str] = set()
    for key in exclude_keys or set():
        normalized = _normalize_skill_name_key(key)
        if normalized:
            exclude.add(normalized)

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        params: list = []
        exclude_clause = ""
        if exclude:
            placeholders = ",".join("?" for _ in exclude)
            exclude_clause = f"AND sp.name_key NOT IN ({placeholders})"
            params.extend(sorted(exclude))

        params.append(int(limit))
        cur.execute(
            f"""
            SELECT sp.name, COUNT(js.job_id) AS link_count
            FROM skill_patterns sp
            INNER JOIN job_skills js ON js.skill_id = sp.id
            WHERE COALESCE(sp.occurrences, 0) >= 1
              {exclude_clause}
            GROUP BY sp.id
            ORDER BY link_count DESC, sp.name ASC
            LIMIT ?
            """,
            params,
        )
        return [str(row[0]) for row in cur.fetchall() if row and row[0]]
    finally:
        conn.close()


def count_job_links_for_skills(db_path: str, skill_names: list[str]) -> dict[str, int]:
    """Return job link counts keyed by input skill name."""
    if not skill_names:
        return {}

    key_to_names: dict[str, list[str]] = {}
    for name in skill_names:
        key = _normalize_skill_name_key(name)
        if not key:
            continue
        key_to_names.setdefault(key, []).append(name)

    if not key_to_names:
        return {name: 0 for name in skill_names}

    placeholders = ",".join("?" for _ in key_to_names)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT sp.name_key, COUNT(js.job_id)
            FROM skill_patterns sp
            JOIN job_skills js ON js.skill_id = sp.id
            WHERE sp.name_key IN ({placeholders})
            GROUP BY sp.name_key
            """,
            tuple(key_to_names.keys()),
        )
        counts_by_key = {str(r[0]): int(r[1] or 0) for r in cur.fetchall() if r and r[0]}
    finally:
        conn.close()

    result = {name: 0 for name in skill_names}
    for key, names in key_to_names.items():
        count = counts_by_key.get(key, 0)
        for name in names:
            result[name] = count
    return result
