"""Company promote flag helpers (bitfield on companies.flags)."""

from __future__ import annotations

from typing import Optional

from .companies_flag_bits import (
    COMPANY_FLAG_PROMOTE,
    COMPANY_FLAG_PROMOTE_TOUCHED,
    companies_flags_column_missing,
    company_has_flag,
    merge_promote_flags,
    with_flag,
)
from .companies_flags_auto import (
    auto_clear_promote_if_untouched_and_idle,
    auto_promote_if_untouched,
    backfill_promote_flags_from_pipeline,
)
from .connection import _connect

# Re-export leaf bits + auto/backfill for stable `from spejder.db.companies_flags import …`.
__all__ = [
    "COMPANY_FLAG_PROMOTE",
    "COMPANY_FLAG_PROMOTE_TOUCHED",
    "company_has_flag",
    "with_flag",
    "companies_flags_column_missing",
    "ensure_company_for_raw",
    "get_company_flags",
    "get_promoted_company_keys",
    "auto_promote_if_untouched",
    "auto_clear_promote_if_untouched_and_idle",
    "set_company_promote",
    "promote_status_for_job",
    "backfill_promote_flags_from_pipeline",
    "merge_promote_flags",
]


def ensure_company_for_raw(db_path: str, raw: str) -> Optional[int]:
    """Create catalog row for non-blank raw if missing. Blank → None."""
    from .companies import _insert_alias, _now, company_alias_key

    text = (raw or "").strip()
    if not text:
        return None
    key = company_alias_key(text)
    if not key:
        return None
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT company_id FROM company_aliases WHERE alias_key=? LIMIT 1",
            (key,),
        )
        row = cur.fetchone()
        if row:
            return int(row[0])
        now = _now()
        cur.execute(
            """
            INSERT INTO companies (display_name, flags, created_at, updated_at)
            VALUES (?, 0, ?, ?)
            """,
            (text, now, now),
        )
        company_id = int(cur.lastrowid)
        _insert_alias(cur, company_id, text, key, now)
        conn.commit()
        return company_id
    finally:
        conn.close()


def get_company_flags(db_path: str, company_id: int) -> Optional[int]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT flags FROM companies WHERE id=?", (int(company_id),))
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0] or 0)
    finally:
        conn.close()


def get_promoted_company_keys(db_path: str) -> set[str]:
    """Alias keys for every catalog company with the Promote bit set."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ca.alias_key
            FROM company_aliases ca
            JOIN companies c ON c.id = ca.company_id
            WHERE (COALESCE(c.flags, 0) & ?) != 0
            """,
            (COMPANY_FLAG_PROMOTE,),
        )
        return {
            str(alias_key)
            for (alias_key,) in cur.fetchall()
            if str(alias_key or "").strip()
        }
    finally:
        conn.close()


def set_company_promote(
    db_path: str,
    company_id: int,
    promote: bool,
    *,
    touch: bool = True,
) -> dict:
    """Set/clear Promote; optionally set touched. Raises ValueError if missing."""
    from .companies import _now

    cid = int(company_id)
    if cid <= 0:
        raise ValueError("company_id is required")
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT flags FROM companies WHERE id=?", (cid,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"company_id not found: {cid}")
        flags = int(row[0] or 0)
        new_flags = with_flag(flags, COMPANY_FLAG_PROMOTE, bool(promote))
        if touch:
            new_flags = with_flag(new_flags, COMPANY_FLAG_PROMOTE_TOUCHED, True)
        if new_flags == flags:
            return {
                "company_id": cid,
                "promote": company_has_flag(new_flags, COMPANY_FLAG_PROMOTE),
                "touched": company_has_flag(new_flags, COMPANY_FLAG_PROMOTE_TOUCHED),
                "changed": False,
            }
        cur.execute(
            "UPDATE companies SET flags=?, updated_at=? WHERE id=?",
            (new_flags, _now(), cid),
        )
        conn.commit()
        return {
            "company_id": cid,
            "promote": company_has_flag(new_flags, COMPANY_FLAG_PROMOTE),
            "touched": company_has_flag(new_flags, COMPANY_FLAG_PROMOTE_TOUCHED),
            "changed": True,
        }
    finally:
        conn.close()


def promote_status_for_job(db_path: str, job_id: int) -> dict:
    """Promote status for the job's company (catalogued or ensure-ready)."""
    from .companies import resolve_company_cur

    jid = int(job_id)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT company FROM jobs WHERE id=?", (jid,))
        row = cur.fetchone()
        if row is None:
            return {
                "job_id": jid,
                "company_id": None,
                "promote": False,
                "touched": False,
            }
        raw = str(row[0] or "")
        ref = resolve_company_cur(cur, raw)
        if ref.id is None:
            return {
                "job_id": jid,
                "company_id": None,
                "promote": False,
                "touched": False,
            }
        cur.execute("SELECT flags FROM companies WHERE id=?", (int(ref.id),))
        flags_row = cur.fetchone()
        flags = int(flags_row[0] or 0) if flags_row else 0
        return {
            "job_id": jid,
            "company_id": int(ref.id),
            "promote": company_has_flag(flags, COMPANY_FLAG_PROMOTE),
            "touched": company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED),
        }
    finally:
        conn.close()
