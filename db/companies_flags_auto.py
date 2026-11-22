"""Auto-promote / idle-clear / backfill for companies.flags Promote bit."""

from __future__ import annotations

from .companies_flag_bits import (
    COMPANY_FLAG_PROMOTE,
    COMPANY_FLAG_PROMOTE_TOUCHED,
    company_has_flag,
    with_flag,
)
from .connection import _connect

__all__ = [
    "auto_promote_if_untouched",
    "auto_clear_promote_if_untouched_and_idle",
    "backfill_promote_flags_from_pipeline",
]


def _company_has_active_pipeline_jobs(cur, company_id: int) -> bool:
    """True when any membership job is applied=1 AND interview_stopped=0."""
    from .companies import _alias_keys_for_company, company_alias_key

    keys = _alias_keys_for_company(cur, int(company_id))
    if not keys:
        return False
    cur.execute(
        """
        SELECT company FROM jobs
        WHERE applied=1 AND COALESCE(interview_stopped, 0)=0
          AND TRIM(COALESCE(company, '')) != ''
        """
    )
    for (company,) in cur.fetchall():
        if company_alias_key(str(company or "")) in keys:
            return True
    return False


def auto_promote_if_untouched(db_path: str, company_id: int) -> bool:
    """Set Promote when untouched. Returns True when flags changed."""
    from .companies import _now

    cid = int(company_id)
    if cid <= 0:
        return False
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT flags FROM companies WHERE id=?", (cid,))
        row = cur.fetchone()
        if row is None:
            return False
        flags = int(row[0] or 0)
        if company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED):
            return False
        if company_has_flag(flags, COMPANY_FLAG_PROMOTE):
            return False
        new_flags = with_flag(flags, COMPANY_FLAG_PROMOTE, True)
        cur.execute(
            "UPDATE companies SET flags=?, updated_at=? WHERE id=?",
            (new_flags, _now(), cid),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def auto_clear_promote_if_untouched_and_idle(db_path: str, company_id: int) -> bool:
    """Clear Promote when untouched and no remaining applied∧¬stopped jobs."""
    from .companies import _now

    cid = int(company_id)
    if cid <= 0:
        return False
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT flags FROM companies WHERE id=?", (cid,))
        row = cur.fetchone()
        if row is None:
            return False
        flags = int(row[0] or 0)
        if company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED):
            return False
        if not company_has_flag(flags, COMPANY_FLAG_PROMOTE):
            return False
        if _company_has_active_pipeline_jobs(cur, cid):
            return False
        new_flags = with_flag(flags, COMPANY_FLAG_PROMOTE, False)
        cur.execute(
            "UPDATE companies SET flags=?, updated_at=? WHERE id=?",
            (new_flags, _now(), cid),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def backfill_promote_flags_from_pipeline(db_path: str) -> int:
    """One-shot: set Promote (untouched) from applied-pipeline key set.

    Uses ``get_applied_pipeline_company_keys`` (non_stopped − stopped): a company
    with any applied+stopped job is excluded entirely, matching old scoring.
    """
    from .companies import _now
    from .queries_applied import get_applied_pipeline_company_keys

    eligible = get_applied_pipeline_company_keys(db_path)
    if not eligible:
        return 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT company_id, alias_key FROM company_aliases")
        ids: set[int] = set()
        for company_id, alias_key in cur.fetchall():
            if str(alias_key or "") in eligible:
                ids.add(int(company_id))
        if not ids:
            return 0
        now = _now()
        updated = 0
        for cid in ids:
            cur.execute("SELECT flags FROM companies WHERE id=?", (cid,))
            row = cur.fetchone()
            if row is None:
                continue
            flags = int(row[0] or 0)
            new_flags = with_flag(flags, COMPANY_FLAG_PROMOTE, True)
            # Keep touched clear on backfill (only OR in promote bit).
            new_flags = with_flag(new_flags, COMPANY_FLAG_PROMOTE_TOUCHED, False)
            if new_flags == flags:
                continue
            cur.execute(
                "UPDATE companies SET flags=?, updated_at=? WHERE id=?",
                (new_flags, now, cid),
            )
            updated += 1
        conn.commit()
        return updated
    finally:
        conn.close()
