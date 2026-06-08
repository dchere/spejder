from typing import Optional

from .connection import _connect
from .utils import _normalize_position_link, _provider_from_link


def get_relevant_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = "SELECT id, company, title, title_english, place, work_type, position_link, raw_text, relevance_score FROM jobs WHERE relevant=1 ORDER BY relevance_score DESC, updated_at DESC"
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        cur.execute(q)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": _provider_from_link(r[6] or ""),
                "company": r[1] or "",
                "title": r[2] or "",
                "title_english": r[3] or "",
                "place": r[4] or "",
                "work_type": r[5] or "Unknown",
                "position_link": r[6] or "",
                "raw_text": r[7] or "",
                "relevance_score": float(r[8] or 0),
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_by_category(
    db_path: str, category: str, limit: int = 0, unviewed_only: bool = False
) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, title_english, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description "
            "FROM jobs WHERE category=?"
        )
        params = [category]
        if unviewed_only:
            q += " AND viewed=0"
        q += " ORDER BY relevance_score DESC, updated_at DESC"
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[7] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "title_english": r[4] or "",
                "place": r[5] or "",
                "work_type": r[6] or "Unknown",
                "position_link": r[7] or "",
                "raw_text": r[8] or "",
                "relevance_score": float(r[9] or 0),
                "relevance_reason": r[10] or "",
                "summary": r[11] or "",
                "viewed": int(r[12] or 0),
                "applied": int(r[13] or 0),
                "description": r[14] or "",
                "category": category,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_count_by_category(
    db_path: str, category: str, unviewed_only: bool = False
) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = "SELECT COUNT(*) FROM jobs WHERE category=?"
        params: list = [category]
        if unviewed_only:
            q += " AND viewed=0"
        cur.execute(q, params)
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    finally:
        conn.close()


def get_jobs_by_category_paged(
    db_path: str,
    category: str,
    limit: int,
    offset: int = 0,
    unviewed_only: bool = False,
) -> list[dict]:
    if int(limit or 0) <= 0:
        return []

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, title_english, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description "
            "FROM jobs WHERE category=?"
        )
        params: list = [category]
        if unviewed_only:
            q += " AND viewed=0"
        q += " ORDER BY relevance_score DESC, updated_at DESC LIMIT ? OFFSET ?"
        params.extend([int(limit), max(0, int(offset or 0))])
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[7] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "title_english": r[4] or "",
                "place": r[5] or "",
                "work_type": r[6] or "Unknown",
                "position_link": r[7] or "",
                "raw_text": r[8] or "",
                "relevance_score": float(r[9] or 0),
                "relevance_reason": r[10] or "",
                "summary": r[11] or "",
                "viewed": int(r[12] or 0),
                "applied": int(r[13] or 0),
                "description": r[14] or "",
                "category": category,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_by_company(db_path: str, company: str, limit: int = 0) -> list[dict]:
    normalized_company = str(company or "").strip()
    if not normalized_company:
        return []

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, title_english, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description, category "
            "FROM jobs WHERE LOWER(TRIM(COALESCE(company, '')))=LOWER(TRIM(?)) "
            "ORDER BY applied DESC, relevance_score DESC, updated_at DESC"
        )
        params: list = [normalized_company]
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[7] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "title_english": r[4] or "",
                "place": r[5] or "",
                "work_type": r[6] or "Unknown",
                "position_link": r[7] or "",
                "raw_text": r[8] or "",
                "relevance_score": float(r[9] or 0),
                "relevance_reason": r[10] or "",
                "summary": r[11] or "",
                "viewed": int(r[12] or 0),
                "applied": int(r[13] or 0),
                "description": r[14] or "",
                "category": r[15] or "not relevant",
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_applied_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, title_english, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description, category "
            "FROM jobs WHERE applied=1 ORDER BY updated_at DESC"
        )
        params: list = []
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[7] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "title_english": r[4] or "",
                "place": r[5] or "",
                "work_type": r[6] or "Unknown",
                "position_link": r[7] or "",
                "raw_text": r[8] or "",
                "relevance_score": float(r[9] or 0),
                "relevance_reason": r[10] or "",
                "summary": r[11] or "",
                "viewed": int(r[12] or 0),
                "applied": int(r[13] or 0),
                "description": r[14] or "",
                "category": r[15] or "relevant",
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_viewed_jobs_count(db_path: str) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM jobs WHERE viewed=1")
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    finally:
        conn.close()


def get_jobs_for_description_refresh(
    db_path: str,
    category: str = "",
    source: str = "",
    links: list[str] = None,
    job_ids: list[int] = None,
    limit: int = 0,
    missing_only: bool = True,
    unviewed_only: bool = False,
) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, title_english, place, work_type, position_link, raw_text, category, description, summary "
            "FROM jobs WHERE 1=1"
        )
        params: list = []

        q += " AND (applied IS NULL OR applied=0)"

        if category:
            q += " AND category=?"
            params.append(category)

        if source:
            q += " AND LOWER(source)=LOWER(?)"
            params.append(source)

        if links:
            normalized_links = [
                _normalize_position_link(link) for link in links if (link or "").strip()
            ]
            if normalized_links:
                placeholders = ",".join(["?"] * len(normalized_links))
                q += f" AND position_link IN ({placeholders})"
                params.extend(normalized_links)

        if job_ids:
            normalized_ids = [int(job_id) for job_id in job_ids]
            if normalized_ids:
                placeholders = ",".join(["?"] * len(normalized_ids))
                q += f" AND id IN ({placeholders})"
                params.extend(normalized_ids)

        if missing_only:
            q += " AND (description IS NULL OR TRIM(description)='')"

        if unviewed_only:
            q += " AND (viewed IS NULL OR viewed=0)"

        q += " ORDER BY updated_at DESC"
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))

        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[7] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "title_english": r[4] or "",
                "place": r[5] or "",
                "work_type": r[6] or "Unknown",
                "position_link": r[7] or "",
                "raw_text": r[8] or "",
                "category": r[9] or "",
                "description": r[10] or "",
                "summary": r[11] or "",
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_for_scoring(db_path: str, provider: str = None) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = "SELECT id, source, title, company, position_link, raw_text, relevance_reason FROM jobs "
        params = []
        if provider:
            q += " WHERE source=?"
            params.append(provider)
        cur.execute(q, params)
        return [{"id": r[0], "source": r[1], "title": r[2], "company": r[3], "position_link": r[4], "raw_text": r[5], "relevance_reason": r[6]} for r in cur.fetchall()]
    finally:
        conn.close()


def get_job_for_rescoring(db_path: str, job_id: int) -> Optional[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, source, title, company, position_link, raw_text, applied FROM jobs WHERE id=?",
            (int(job_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "source": row[1], "title": row[2], "company": row[3], "position_link": row[4], "raw_text": row[5], "applied": row[6]}
    finally:
        conn.close()


def get_jobs_merge_candidates(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, position_link, source, title, company FROM jobs")
        return [{"id": r[0], "position_link": r[1], "source": r[2], "title": r[3], "company": r[4]} for r in cur.fetchall()]
    finally:
        conn.close()



def get_titles_for_labeled_jobs(db_path: str) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT title, description FROM jobs WHERE category IN ('relevant', 'not relevant') AND applied=0"
        )
        return [(r[0] or "") + " " + (r[1] or "") for r in cur.fetchall()]
    finally:
        conn.close()


def get_titles_for_missing_skills(db_path: str) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT title, description FROM jobs WHERE applied=1"
        )
        return [(r[0] or "") + " " + (r[1] or "") for r in cur.fetchall()]
    finally:
        conn.close()



def get_all_jobs_for_dedupe(db_path: str) -> list[tuple]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, company, title, place, work_type, position_link, raw_text, viewed, applied
            FROM jobs
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_jobs_for_keyword_suggestions(db_path: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT category, title, company, place, work_type, raw_text FROM jobs WHERE category IN ('relevant', 'not relevant')"
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_jobs_for_skill_suggestions(db_path: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM jobs WHERE applied=1"
        )
        return cur.fetchall()
    finally:
        conn.close()


