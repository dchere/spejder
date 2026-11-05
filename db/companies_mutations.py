"""Company catalog write helpers (merge / rename / delete / prune)."""

from __future__ import annotations

from typing import Optional

from .companies import (
    _alias_keys_for_company,
    _alias_owner,
    _insert_alias,
    _member_job_ids_for_keys,
    _now,
    company_alias_key,
    rewrite_jobs_company_for_keys,
)
from .connection import _connect


def _reject_foreign_display_alias(
    cur,
    display_name: str,
    allowed_ids: set[int],
    *,
    merge_hint: bool,
) -> None:
    """Raise ValueError when display_name's alias_key is owned outside allowed_ids."""
    owner = _alias_owner(cur, company_alias_key(display_name))
    if owner is not None and int(owner[0]) not in allowed_ids:
        owner_id, owner_name = owner
        label = (owner_name or "").strip() or f"id {owner_id}"
        if merge_hint:
            raise ValueError(
                f"display name {display_name!r} is already used by {label} "
                f"— merge that company too"
            )
        raise ValueError(
            f"display name {display_name!r} is already used by {label} "
            f"— merge that company instead"
        )


def merge_company_rows(
    db_path: str,
    company_ids: list[int],
    display_name: str,
) -> dict:
    """One transaction: survivor identity, alias re-point, job rewrite, delete losers."""
    ids = []
    seen: set[int] = set()
    for raw_id in company_ids:
        cid = int(raw_id)
        if cid <= 0 or cid in seen:
            continue
        seen.add(cid)
        ids.append(cid)
    name = (display_name or "").strip()
    if len(ids) < 2:
        raise ValueError("merge requires at least two distinct company_ids")
    if not name:
        raise ValueError("display_name is required")

    survivor_id = ids[0]
    loser_ids = ids[1:]
    now = _now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM companies WHERE id=?", (survivor_id,))
        if cur.fetchone() is None:
            raise ValueError(f"company_id not found: {survivor_id}")
        for loser_id in loser_ids:
            cur.execute("SELECT id FROM companies WHERE id=?", (loser_id,))
            if cur.fetchone() is None:
                raise ValueError(f"company_id not found: {loser_id}")

        _reject_foreign_display_alias(cur, name, set(ids), merge_hint=True)

        cur.execute(
            "UPDATE companies SET display_name=?, updated_at=? WHERE id=?",
            (name, now, survivor_id),
        )

        # Re-point every loser alias onto the survivor (UNIQUE alias_key already
        # prevents dual ownership). Preserves rename history that jobs no longer carry.
        for loser_id in loser_ids:
            cur.execute(
                "UPDATE company_aliases SET company_id=? WHERE company_id=?",
                (survivor_id, loser_id),
            )
            cur.execute("DELETE FROM companies WHERE id=?", (loser_id,))

        _insert_alias(cur, survivor_id, name, company_alias_key(name), now)

        keys_after = _alias_keys_for_company(cur, survivor_id)
        jobs_rewritten = rewrite_jobs_company_for_keys(cur, keys_after, name, now)
        conn.commit()
        return {
            "survivor_id": survivor_id,
            "deleted_ids": loser_ids,
            "display_name": name,
            "jobs_rewritten": jobs_rewritten,
        }
    finally:
        conn.close()


def rename_company_row(db_path: str, company_id: int, display_name: str) -> dict:
    name = (display_name or "").strip()
    if not name:
        raise ValueError("display_name is required")
    cid = int(company_id)
    now = _now()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM companies WHERE id=?", (cid,))
        if cur.fetchone() is None:
            raise ValueError(f"company_id not found: {cid}")
        _reject_foreign_display_alias(cur, name, {cid}, merge_hint=False)
        cur.execute(
            "UPDATE companies SET display_name=?, updated_at=? WHERE id=?",
            (name, now, cid),
        )
        _insert_alias(cur, cid, name, company_alias_key(name), now)
        # Only rewrite membership of this company's keys (after successful alias insert).
        keys_after = _alias_keys_for_company(cur, cid)
        jobs_rewritten = rewrite_jobs_company_for_keys(cur, keys_after, name, now)
        conn.commit()
        return {
            "company_id": cid,
            "display_name": name,
            "jobs_rewritten": jobs_rewritten,
        }
    finally:
        conn.close()


def delete_company_row(db_path: str, company_id: int) -> dict:
    """Delete company+aliases and membership jobs. Idempotent if already gone."""
    cid = int(company_id)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM companies WHERE id=?", (cid,))
        if cur.fetchone() is None:
            return {
                "company_id": cid,
                "already_gone": True,
                "jobs_deleted": 0,
            }
        keys = _alias_keys_for_company(cur, cid)
        job_ids = _member_job_ids_for_keys(cur, keys)
        if job_ids:
            placeholders = ",".join("?" * len(job_ids))
            cur.execute(
                f"DELETE FROM job_skills WHERE job_id IN ({placeholders})",
                job_ids,
            )
            cur.execute(f"DELETE FROM jobs WHERE id IN ({placeholders})", job_ids)
        cur.execute("DELETE FROM company_aliases WHERE company_id=?", (cid,))
        cur.execute("DELETE FROM companies WHERE id=?", (cid,))
        conn.commit()
        return {
            "company_id": cid,
            "already_gone": False,
            "jobs_deleted": len(job_ids),
        }
    finally:
        conn.close()


def prune_empty_companies(
    db_path: str, company_ids: Optional[list[int]] = None
) -> list[int]:
    """Delete catalog companies with zero membership jobs (optional id filter)."""
    conn = _connect(db_path)
    deleted: list[int] = []
    try:
        cur = conn.cursor()
        if company_ids is None:
            cur.execute("SELECT id FROM companies")
            targets = [int(row[0]) for row in cur.fetchall()]
        else:
            targets = [int(cid) for cid in company_ids]
        for cid in targets:
            keys = _alias_keys_for_company(cur, cid)
            if _member_job_ids_for_keys(cur, keys):
                continue
            cur.execute("DELETE FROM company_aliases WHERE company_id=?", (cid,))
            cur.execute("DELETE FROM companies WHERE id=?", (cid,))
            deleted.append(cid)
        conn.commit()
        return deleted
    finally:
        conn.close()
