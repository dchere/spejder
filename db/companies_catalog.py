"""Company catalog list reads: Companies tab rows and stage counts."""

from __future__ import annotations

from .companies import company_alias_key, ensure_companies_from_jobs
from .connection import _connect


def _stage_bucket(applied: int, on_interview: int, interview_stopped: int, category: str) -> str | None:
    """Exclusive stage for Companies tab counts: Stopped → Interview → Applied → category."""
    if applied == 1:
        if interview_stopped == 1:
            return "stopped"
        if on_interview == 1:
            return "interview"
        return "applied"
    cat = (category or "").strip().lower()
    if cat == "relevant":
        return "relevant"
    if cat == "not relevant":
        return "not_relevant"
    return None


def list_company_catalog_rows(db_path: str) -> list[dict]:
    """Companies tab rows: bootstrap, then created_at / stage counts / not_viewed / alias_count.

    Default order matches Skills Added DESC: SQL name A→Z, then stable sort by
    created_at DESC (empty → "0000" so empties sort last).
    """
    ensure_companies_from_jobs(db_path)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, display_name, created_at FROM companies "
            "ORDER BY display_name COLLATE NOCASE ASC"
        )
        companies = [
            (int(row[0]), str(row[1] or ""), str(row[2] or ""))
            for row in cur.fetchall()
        ]
        if not companies:
            return []

        cur.execute("SELECT company_id, alias_key, alias_raw FROM company_aliases")
        aliases_by_id: dict[int, list[tuple[str, str]]] = {}
        key_to_ids: dict[str, set[int]] = {}
        for company_id, alias_key, alias_raw in cur.fetchall():
            cid = int(company_id)
            key = str(alias_key or "")
            aliases_by_id.setdefault(cid, []).append((key, str(alias_raw or "")))
            if key:
                key_to_ids.setdefault(key, set()).add(cid)

        zero = {
            "job_count": 0,
            "not_viewed_count": 0,
            "relevant_count": 0,
            "not_relevant_count": 0,
            "applied_count": 0,
            "interview_count": 0,
            "stopped_count": 0,
        }
        counts: dict[int, dict[str, int]] = {
            cid: dict(zero) for cid, _, _ in companies
        }
        # Soft-hidden included (no hidden=0 filter); membership via alias key like resolve_company.
        cur.execute(
            "SELECT company, COALESCE(applied, 0), COALESCE(on_interview, 0), "
            "COALESCE(interview_stopped, 0), COALESCE(category, ''), "
            "COALESCE(viewed, 0) "
            "FROM jobs WHERE TRIM(COALESCE(company, '')) != ''"
        )
        for (
            company,
            applied,
            on_interview,
            interview_stopped,
            category,
            viewed,
        ) in cur.fetchall():
            key = company_alias_key(str(company or ""))
            if not key:
                continue
            member_ids = key_to_ids.get(key)
            if not member_ids:
                continue
            bucket = _stage_bucket(
                int(applied or 0),
                int(on_interview or 0),
                int(interview_stopped or 0),
                str(category or ""),
            )
            unviewed = int(viewed or 0) == 0
            for cid in member_ids:
                row_counts = counts[cid]
                row_counts["job_count"] += 1
                if unviewed:
                    row_counts["not_viewed_count"] += 1
                if bucket == "relevant":
                    row_counts["relevant_count"] += 1
                elif bucket == "not_relevant":
                    row_counts["not_relevant_count"] += 1
                elif bucket == "applied":
                    row_counts["applied_count"] += 1
                elif bucket == "interview":
                    row_counts["interview_count"] += 1
                elif bucket == "stopped":
                    row_counts["stopped_count"] += 1

        rows: list[dict] = []
        for cid, display_name, created_at in companies:
            alias_list = aliases_by_id.get(cid, [])
            row_counts = counts.get(cid, zero)
            rows.append(
                {
                    "id": cid,
                    "display_name": display_name,
                    "created_at": created_at,
                    "job_count": int(row_counts["job_count"]),
                    "not_viewed_count": int(row_counts["not_viewed_count"]),
                    "relevant_count": int(row_counts["relevant_count"]),
                    "not_relevant_count": int(row_counts["not_relevant_count"]),
                    "applied_count": int(row_counts["applied_count"]),
                    "interview_count": int(row_counts["interview_count"]),
                    "stopped_count": int(row_counts["stopped_count"]),
                    "alias_count": len(alias_list),
                }
            )
        # Skills parity: stable name A→Z (SQL), then created_at DESC for SSR default.
        rows.sort(key=lambda r: r.get("created_at") or "0000", reverse=True)
        return rows
    finally:
        conn.close()
