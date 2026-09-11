from .connection import _connect
from .utils import _normalize_skill_name_key


def delete_skill_from_db(db_path: str, skill_name: str) -> dict:
    """Delete a skill from skill_patterns and all job links by normalized name key."""
    key = _normalize_skill_name_key(skill_name)
    if not key:
        return {
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
        }

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", (key,))
        skill_ids = [int(r[0]) for r in cur.fetchall()]
        if not skill_ids:
            return {
                "skill_rows_deleted": 0,
                "job_skill_links_deleted": 0,
                "affected_job_ids": [],
            }

        affected_job_ids: set[int] = set()
        links_deleted = 0
        for skill_id in skill_ids:
            cur.execute("SELECT DISTINCT job_id FROM job_skills WHERE skill_id=?", (skill_id,))
            for row in cur.fetchall():
                if row and row[0]:
                    affected_job_ids.add(int(row[0]))
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
            "affected_job_ids": sorted(affected_job_ids),
        }
    finally:
        conn.close()


def cleanup_blocked_skills_from_db(db_path: str, blocked_skills: list[str]) -> dict:
    """Delete blocked skills from skill_patterns and job_skills; dedupe by normalized name key."""
    seen_keys: set[str] = set()
    skills_processed = 0
    skill_rows_deleted = 0
    job_skill_links_deleted = 0
    affected_job_ids: set[int] = set()

    for skill in blocked_skills or []:
        key = _normalize_skill_name_key(str(skill))
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        skills_processed += 1
        deleted = delete_skill_from_db(db_path, skill)
        skill_rows_deleted += int(deleted.get("skill_rows_deleted", 0))
        job_skill_links_deleted += int(deleted.get("job_skill_links_deleted", 0))
        for job_id in deleted.get("affected_job_ids", []):
            affected_job_ids.add(int(job_id))

    return {
        "skills_processed": skills_processed,
        "skill_rows_deleted": skill_rows_deleted,
        "job_skill_links_deleted": job_skill_links_deleted,
        "affected_job_ids": sorted(affected_job_ids),
    }
