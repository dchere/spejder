from typing import Optional

from .connection import _connect
from .queries_listings import _JOB_SELECT_COLS
from .queries_rows import _map_full_job_row, _map_refresh_job_row
from .utils import _normalize_position_link, _provider_from_link


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
        return [_map_refresh_job_row(r) for r in rows]
    finally:
        conn.close()


def get_jobs_for_description_triage(
    db_path: str,
    *,
    limit: int = 0,
) -> list[dict]:
    """Missing-description rows in sync/ingest triage scope.

    Shared filter for ingest description generation and empty-inbox early-exit
    probes: ``missing_only`` and ``unviewed_only`` (Hidden stays in scope).
    CLI ``refresh-descriptions`` keeps calling ``get_jobs_for_description_refresh``
    with defaults instead.
    """
    return get_jobs_for_description_refresh(
        db_path,
        missing_only=True,
        unviewed_only=True,
        limit=limit,
    )


def get_jobs_for_active_rescore(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {_JOB_SELECT_COLS}, category FROM jobs WHERE "
            "applied=1 OR on_interview=1 OR interview_stopped=1 OR "
            "COALESCE(viewed, 0)=0"
        )
        rows = cur.fetchall()
        return [_map_full_job_row(r[:-1], r[-1] or "") for r in rows]
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


def get_job_scope_flags(db_path: str, job_id: int) -> Optional[dict]:
    """Lightweight pipeline flags for live active-rescore scope checks.

    Returns ``None`` when the job is missing. Keys: ``viewed``, ``applied``,
    ``on_interview``, ``interview_stopped`` (raw SQLite values; callers treat
    null viewed as 0 via ``job_in_active_rescore_scope``).
    """
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT viewed, applied, on_interview, interview_stopped "
            "FROM jobs WHERE id=?",
            (int(job_id),),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "viewed": row[0],
            "applied": row[1],
            "on_interview": row[2],
            "interview_stopped": row[3],
        }
    finally:
        conn.close()

