from __future__ import annotations

from .connection import _connect
from .queries_listings import _JOB_SELECT_COLS
from .queries_rows import _map_applied_job_row


def get_applied_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            f"SELECT {_JOB_SELECT_COLS}, category "
            "FROM jobs WHERE applied=1 AND on_interview=0 AND interview_stopped=0 "
            "ORDER BY (applied_at IS NULL), applied_at DESC, updated_at DESC"
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


def get_all_applied_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            f"SELECT {_JOB_SELECT_COLS}, category "
            "FROM jobs WHERE applied=1 ORDER BY (applied_at IS NULL), applied_at DESC, updated_at DESC"
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


def get_interview_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            f"SELECT {_JOB_SELECT_COLS}, category "
            "FROM jobs WHERE applied=1 AND on_interview=1 "
            "ORDER BY (applied_at IS NULL), applied_at DESC, updated_at DESC"
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


def get_stopped_interview_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            f"SELECT {_JOB_SELECT_COLS}, category "
            "FROM jobs WHERE applied=1 AND interview_stopped=1 "
            "ORDER BY (applied_at IS NULL), applied_at DESC, updated_at DESC"
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


def get_applied_pipeline_company_keys(db_path: str) -> set[str]:
    """Normalized company keys eligible for applied-company relevance bonus.

    Eligible = union of all alias keys for every resolved company that has ≥1
    applied∧¬stopped job, minus the union of alias keys for companies that have
    any applied+stopped job. Blank companies are ignored.
    """
    from .companies import resolve_company_cur

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT company, interview_stopped FROM jobs WHERE applied=1"
        )
        rows = cur.fetchall()
        cache: dict[str, frozenset[str]] = {}
        non_stopped: set[str] = set()
        stopped: set[str] = set()
        for company, interview_stopped in rows:
            raw = str(company or "")
            if raw not in cache:
                cache[raw] = resolve_company_cur(cur, raw).keys
            keys = cache[raw]
            if not keys:
                continue
            if int(interview_stopped or 0) == 1:
                stopped |= keys
            else:
                non_stopped |= keys
        return non_stopped - stopped
    finally:
        conn.close()
