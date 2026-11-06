"""Company catalog: soft identity via aliases (no jobs.company_id FK)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional  # CompanyRef.id

from .connection import _connect
from .deduplication_utils import (
    _canonicalize_company_for_dedupe,
    _normalize_company_key,
)


def company_alias_key(raw: str) -> str:
    """alias_key / dedupe company half: normalize(canonicalize(raw))."""
    return _normalize_company_key(_canonicalize_company_for_dedupe(raw or ""))


@dataclass(frozen=True)
class CompanyRef:
    """Resolved company identity. id is None when not in the catalog (ephemeral)."""

    id: Optional[int]
    display_name: str
    keys: frozenset[str]

    @property
    def catalogued(self) -> bool:
        return self.id is not None

    @property
    def dedupe_key(self) -> str:
        display_key = company_alias_key(self.display_name)
        if display_key:
            return display_key
        if self.keys:
            return next(iter(sorted(self.keys)))
        return ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ephemeral_ref(raw: str) -> CompanyRef:
    text = (raw or "").strip()
    key = company_alias_key(text)
    keys = frozenset({key}) if key else frozenset()
    return CompanyRef(id=None, display_name=text, keys=keys)


def resolve_company_cur(cur, raw: str) -> CompanyRef:
    text = (raw or "").strip()
    if not text:
        return CompanyRef(id=None, display_name="", keys=frozenset())
    key = company_alias_key(text)
    if not key:
        return _ephemeral_ref(text)
    cur.execute(
        """
        SELECT c.id, c.display_name
        FROM company_aliases ca
        JOIN companies c ON c.id = ca.company_id
        WHERE ca.alias_key = ?
        LIMIT 1
        """,
        (key,),
    )
    row = cur.fetchone()
    if not row:
        return _ephemeral_ref(text)
    company_id = int(row[0])
    display_name = str(row[1] or "")
    cur.execute(
        "SELECT alias_key FROM company_aliases WHERE company_id=?",
        (company_id,),
    )
    keys = frozenset(
        str(alias_key)
        for (alias_key,) in cur.fetchall()
        if str(alias_key or "").strip()
    )
    return CompanyRef(id=company_id, display_name=display_name, keys=keys)


def resolve_company(db_path: str, raw: str) -> CompanyRef:
    conn = _connect(db_path)
    try:
        return resolve_company_cur(conn.cursor(), raw)
    finally:
        conn.close()


def _insert_alias(cur, company_id: int, alias_raw: str, alias_key: str, now: str) -> bool:
    """Insert alias if alias_key is free. Returns True when inserted."""
    if not alias_key:
        return False
    cur.execute(
        "SELECT 1 FROM company_aliases WHERE alias_key=? LIMIT 1",
        (alias_key,),
    )
    if cur.fetchone():
        return False
    cur.execute(
        """
        INSERT INTO company_aliases (company_id, alias_raw, alias_key, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (company_id, alias_raw, alias_key, now),
    )
    return True


def _alias_owner(cur, alias_key: str) -> Optional[tuple[int, str]]:
    """Return (company_id, display_name) when alias_key is owned, else None."""
    if not alias_key:
        return None
    cur.execute(
        """
        SELECT c.id, c.display_name
        FROM company_aliases ca
        JOIN companies c ON c.id = ca.company_id
        WHERE ca.alias_key = ?
        LIMIT 1
        """,
        (alias_key,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), str(row[1] or "")


def ensure_companies_from_jobs(db_path: str) -> int:
    """Idempotent bootstrap: catalog DISTINCT non-blank jobs.company. Returns rows created."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT company FROM jobs
            WHERE TRIM(COALESCE(company, '')) != ''
            """
        )
        raws = [str(row[0] or "").strip() for row in cur.fetchall()]
        created = 0
        now = _now()
        for raw in raws:
            if not raw:
                continue
            key = company_alias_key(raw)
            if not key:
                continue
            cur.execute(
                "SELECT company_id FROM company_aliases WHERE alias_key=? LIMIT 1",
                (key,),
            )
            if cur.fetchone():
                continue
            cur.execute(
                """
                INSERT INTO companies (display_name, created_at, updated_at)
                VALUES (?, ?, ?)
                """,
                (raw, now, now),
            )
            company_id = int(cur.lastrowid)
            _insert_alias(cur, company_id, raw, key, now)
            created += 1
        conn.commit()
        return created
    finally:
        conn.close()


def get_company_alias_raws(db_path: str, company_id: int) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT alias_raw FROM company_aliases WHERE company_id=? ORDER BY alias_raw COLLATE NOCASE",
            (company_id,),
        )
        return [str(row[0] or "") for row in cur.fetchall()]
    finally:
        conn.close()


def get_company_job_ids(db_path: str, company_id: int) -> list[int]:
    """Job ids whose company key matches any alias of company_id."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT alias_key FROM company_aliases WHERE company_id=?",
            (company_id,),
        )
        keys = {str(row[0] or "") for row in cur.fetchall() if str(row[0] or "").strip()}
        if not keys:
            return []
        cur.execute(
            "SELECT id, company FROM jobs WHERE TRIM(COALESCE(company, '')) != ''"
        )
        return [
            int(job_id)
            for job_id, company in cur.fetchall()
            if company_alias_key(str(company or "")) in keys
        ]
    finally:
        conn.close()


def count_company_jobs(db_path: str, company_id: int) -> int:
    return len(get_company_job_ids(db_path, company_id))


def company_exists(db_path: str, company_id: int) -> bool:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM companies WHERE id=? LIMIT 1", (company_id,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def _member_job_ids_for_keys(cur, keys: set[str]) -> list[int]:
    if not keys:
        return []
    cur.execute(
        "SELECT id, company FROM jobs WHERE TRIM(COALESCE(company, '')) != ''"
    )
    return [
        int(job_id)
        for job_id, company in cur.fetchall()
        if company_alias_key(str(company or "")) in keys
    ]


def _alias_keys_for_company(cur, company_id: int) -> set[str]:
    cur.execute(
        "SELECT alias_key FROM company_aliases WHERE company_id=?",
        (company_id,),
    )
    return {str(row[0] or "") for row in cur.fetchall() if str(row[0] or "").strip()}


def rewrite_jobs_company_for_keys(cur, keys: set[str], display_name: str, now: str) -> int:
    job_ids = _member_job_ids_for_keys(cur, keys)
    if not job_ids:
        return 0
    placeholders = ",".join("?" * len(job_ids))
    cur.execute(
        f"UPDATE jobs SET company=?, updated_at=? WHERE id IN ({placeholders})",
        [display_name, now, *job_ids],
    )
    return len(job_ids)


# List/stage helpers live in companies_catalog.py; writes in companies_mutations.py.
# Re-exported for stable imports.
from .companies_catalog import list_company_catalog_rows  # noqa: E402
from .companies_mutations import (  # noqa: E402
    delete_company_row,
    delete_company_rows,
    merge_company_rows,
    prune_empty_companies,
    rename_company_row,
)

__all__ = [
    "CompanyRef",
    "company_alias_key",
    "resolve_company",
    "resolve_company_cur",
    "ensure_companies_from_jobs",
    "list_company_catalog_rows",
    "get_company_alias_raws",
    "get_company_job_ids",
    "count_company_jobs",
    "company_exists",
    "merge_company_rows",
    "rename_company_row",
    "delete_company_row",
    "delete_company_rows",
    "prune_empty_companies",
]
