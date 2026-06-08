import sqlite3
import re
import time
import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse
from .connection import _connect
from .utils import sanitize_job_title, _normalize_skill_name_key, _normalize_position_link, get_job_link, _provider_from_link

def _skill_to_regex_simple(name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(
        r"[A-Za-z0-9+#.]+", name or "") if t]
    if not tokens:
        return name
    return r"\b" + r"\s+".join(tokens) + r"\b"

def get_skill_patterns(db_path: str, enabled_only: bool = True) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT name, pattern, source, occurrences, weight, enabled, last_seen_at "
            "FROM skill_patterns"
        )
        params: list = []
        if enabled_only:
            q += " WHERE enabled=1"
        q += " ORDER BY weight DESC, occurrences DESC, name ASC"
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "name": r[0] or "",
                "pattern": r[1] or "",
                "source": r[2] or "",
                "occurrences": int(r[3] or 0),
                "weight": float(r[4] or 0),
                "enabled": int(r[5] or 0),
                "last_seen_at": r[6] or "",
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_skill_pattern(
    db_path: str,
    name: str,
    pattern: str,
    source: str = "learned",
    occurrences_inc: int = 0,
    weight_inc: float = 0.0,
    enabled: bool = True,
) -> bool:
    name_clean = (name or "").strip()
    pattern_clean = (pattern or "").strip()
    name_key = _normalize_skill_name_key(name_clean)
    if not name_clean or not name_key or not pattern_clean:
        return False

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO skill_patterns
                (name, name_key, pattern, source, occurrences, weight, enabled, created_at, updated_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name_key) DO UPDATE SET
                name=excluded.name,
                pattern=CASE
                    WHEN excluded.pattern IS NOT NULL AND TRIM(excluded.pattern)<>'' THEN excluded.pattern
                    ELSE skill_patterns.pattern
                END,
                source=CASE
                    WHEN skill_patterns.source IS NULL OR TRIM(skill_patterns.source)='' THEN excluded.source
                    ELSE skill_patterns.source
                END,
                occurrences=skill_patterns.occurrences + excluded.occurrences,
                weight=skill_patterns.weight + excluded.weight,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at,
                last_seen_at=excluded.last_seen_at
            """,
            (
                name_clean,
                name_key,
                pattern_clean,
                source,
                max(0, int(occurrences_inc)),
                max(0.0, float(weight_inc)),
                1 if enabled else 0,
                now,
                now,
                now,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def migrate_profile_skill_patterns_to_db(
    db_path: str, profile_path: str
) -> dict[str, int]:
    ensure_db(db_path)

    profile: AppConfig = {}
    if profile_path and os.path.exists(profile_path):
        try:
            with open(profile_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                profile = loaded
        except Exception:
            profile = {}

    raw = profile.known_skill_patterns
    if not isinstance(raw, list) or not raw:
        raw = getattr(DEFAULT_PROFILE, "known_skill_patterns", [])

    inserted = 0
    seed_count = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not name or not pattern:
            continue
        seed_count += 1
        if upsert_skill_pattern(
            db_path,
            name=name,
            pattern=pattern,
            source="profile_seed",
            occurrences_inc=0,
            weight_inc=0.0,
            enabled=True,
        ):
            inserted += 1

    return {"seed_count": int(seed_count), "inserted": int(inserted)}


def set_job_skills(db_path: str, job_id: int, skill_names: list[str]) -> None:
    """Persist the extracted skill list for a job as links to skill_patterns rows."""
    if not job_id or not skill_names:
        return
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for name in skill_names:
            name = (name or "").strip()
            if not name:
                continue
            key = _normalize_skill_name_key(name)
            # Ensure the skill_pattern row exists (source='detected', no weight bump here)
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
    finally:
        conn.close()


def get_job_skills(db_path: str, job_id: int) -> list[str]:
    """Return the cached skill names for a job, or empty list if not yet stored."""
    if not job_id:
        return []
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sp.name FROM job_skills js
            JOIN skill_patterns sp ON sp.id = js.skill_id
            WHERE js.job_id = ?
            ORDER BY sp.name
            """,
            (job_id,),
        )
        return [r[0] for r in cur.fetchall()]
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


def delete_skill_from_db(db_path: str, skill_name: str) -> dict[str, int]:
    """Delete a skill from skill_patterns and all job links by normalized name key."""
    key = _normalize_skill_name_key(skill_name)
    if not key:
        return {"skill_rows_deleted": 0, "job_skill_links_deleted": 0}

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", (key,))
        skill_ids = [int(r[0]) for r in cur.fetchall()]
        if not skill_ids:
            return {"skill_rows_deleted": 0, "job_skill_links_deleted": 0}

        links_deleted = 0
        for skill_id in skill_ids:
            cur.execute("DELETE FROM job_skills WHERE skill_id=?", (skill_id,))
            links_deleted += int(cur.rowcount or 0)

        skill_rows_deleted = 0
        for skill_id in skill_ids:
            cur.execute("DELETE FROM skill_patterns WHERE id=?", (skill_id,))
            skill_rows_deleted += int(cur.rowcount or 0)

        conn.commit()
        return {
            "skill_rows_deleted": int(skill_rows_deleted),
            "job_skill_links_deleted": int(links_deleted),
        }
    finally:
        conn.close()


