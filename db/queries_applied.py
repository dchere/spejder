from .connection import _connect
from .deduplication_utils import (
    _canonicalize_company_for_dedupe,
    _normalize_company_key,
)
from .queries_listings import _EXCLUDE_HIDDEN_SQL, _JOB_SELECT_COLS
from .queries_rows import _map_applied_job_row


def get_applied_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            f"SELECT {_JOB_SELECT_COLS}, category "
            "FROM jobs WHERE applied=1 AND on_interview=0 AND interview_stopped=0"
            f"{_EXCLUDE_HIDDEN_SQL} "
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
            "FROM jobs WHERE applied=1 AND on_interview=1"
            f"{_EXCLUDE_HIDDEN_SQL} "
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
            "FROM jobs WHERE applied=1 AND interview_stopped=1"
            f"{_EXCLUDE_HIDDEN_SQL} "
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

    Eligible = keys with ≥1 applied row where interview_stopped=0, minus keys
    that also have any applied+stopped row. Blank companies are ignored.
    Uses the same key as position dedupe.
    """
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT company, interview_stopped FROM jobs WHERE applied=1"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    non_stopped: set[str] = set()
    stopped: set[str] = set()
    for company, interview_stopped in rows:
        key = _normalize_company_key(_canonicalize_company_for_dedupe(company or ""))
        if not key:
            continue
        if int(interview_stopped or 0) == 1:
            stopped.add(key)
        else:
            non_stopped.add(key)
    return non_stopped - stopped
