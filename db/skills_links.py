"""job_skills CRUD, rank, and count."""
from datetime import datetime, timezone
from typing import Optional

from .connection import _connect
from .utils import _normalize_skill_name_key
from .skills_patterns import _skill_to_regex_simple

def replace_job_skills(db_path: str, job_id: int, skill_names: list[str]) -> bool:
    """Replace job skill links with skill_names. Returns True when links changed."""
    if not job_id:
        return False

    incoming_keys: set[str] = set()
    cleaned_names: list[str] = []
    for name in skill_names or []:
        name = (name or "").strip()
        if not name:
            continue
        key = _normalize_skill_name_key(name)
        if not key or key in incoming_keys:
            continue
        incoming_keys.add(key)
        cleaned_names.append(name)

    current_keys = {
        _normalize_skill_name_key(s)
        for s in get_job_skills(db_path, job_id)
        if _normalize_skill_name_key(s)
    }
    if incoming_keys == current_keys:
        return False

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM job_skills WHERE job_id=?", (int(job_id),))
        for name in cleaned_names:
            key = _normalize_skill_name_key(name)
            cur.execute(
                """
                INSERT INTO skill_patterns (name, name_key, pattern, source, occurrences, weight, enabled, created_at, updated_at)
                VALUES (?, ?, ?, 'detected', 0, 0, 1, ?, ?)
                ON CONFLICT(name_key) DO NOTHING
                """,
                (name, key, _skill_to_regex_simple(name), now, now),
            )
            cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", (key,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "INSERT OR IGNORE INTO job_skills (job_id, skill_id) VALUES (?, ?)",
                    (job_id, row[0]),
                )
        conn.commit()
        return True
    finally:
        conn.close()


def set_job_skills(db_path: str, job_id: int, skill_names: list[str]) -> None:
    """Persist the extracted skill list for a job as links to skill_patterns rows."""
    replace_job_skills(db_path, job_id, skill_names)


def get_job_skills(db_path: str, job_id: int) -> list[str]:
    """Return the cached skill names for a job, or empty list if not yet stored."""
    if not job_id:
        return []
    return get_job_skills_for_jobs(db_path, [int(job_id)]).get(int(job_id), [])


def get_job_skills_for_jobs(db_path: str, job_ids: list[int]) -> dict[int, list[str]]:
    """Return cached skill names for many jobs in one query (job_id -> ordered names)."""
    ids = sorted({int(job_id) for job_id in job_ids if int(job_id or 0) > 0})
    if not ids:
        return {}
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        placeholders = ",".join("?" for _ in ids)
        cur.execute(
            f"""
            SELECT js.job_id, sp.name FROM job_skills js
            JOIN skill_patterns sp ON sp.id = js.skill_id
            WHERE js.job_id IN ({placeholders})
            ORDER BY js.job_id, sp.name
            """,
            ids,
        )
        result: dict[int, list[str]] = {job_id: [] for job_id in ids}
        for job_id, name in cur.fetchall():
            result[int(job_id)].append(str(name))
        return result
    finally:
        conn.close()


def clear_job_skills_for_unviewed_jobs(db_path: str) -> int:
    """Clear cached job->skill links for unviewed jobs so skills can be re-extracted."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM job_skills
            WHERE job_id IN (
                SELECT id FROM jobs WHERE COALESCE(viewed, 0)=0
            )
            """
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def clear_job_skills_for_job(db_path: str, job_id: int) -> int:
    """Clear cached job->skill links for one job so skills can be re-extracted."""
    if not job_id:
        return 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM job_skills WHERE job_id=?", (int(job_id),))
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def get_job_ids_for_skill(db_path: str, skill_name: str, limit: int = 2) -> list[int]:
    """Return job ids linked to a skill pattern, most recently updated first."""
    key = _normalize_skill_name_key(skill_name)
    if not key or limit <= 0:
        return []
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT js.job_id
            FROM job_skills js
            JOIN skill_patterns sp ON sp.id = js.skill_id
            JOIN jobs j ON j.id = js.job_id
            WHERE sp.name_key = ?
            ORDER BY j.updated_at DESC, j.id DESC
            LIMIT ?
            """,
            (key, int(limit)),
        )
        return [int(r[0]) for r in cur.fetchall() if r and r[0]]
    finally:
        conn.close()


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


