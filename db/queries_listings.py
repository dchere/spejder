from .connection import _connect
from .queries_rows import _map_applied_job_row, _map_company_job_row, _map_full_job_row
from .utils import _provider_from_link


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
        return [_map_full_job_row(r, category) for r in rows]
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
        return [_map_full_job_row(r, category) for r in rows]
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
        return [_map_company_job_row(r) for r in rows]
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
        return [_map_applied_job_row(r) for r in rows]
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

