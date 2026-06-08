import sqlite3
import re
import time
import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse
from .connection import _connect
from .utils import sanitize_job_title, _normalize_position_link, get_job_link, _provider_from_link
from .deduplication_utils import _cross_source_dedupe_key

def upsert_job(db_path: str, job: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        position_link = job.get("position_link", "")
        source = job.get("source") or _provider_from_link(position_link)
        company = job.get("company", "")
        title = sanitize_job_title(job.get("title", ""))
        place = job.get("place", "")
        work_type = job.get("work_type", "Unknown")
        raw_text = job.get("raw_text", "")
        cur.execute(
            "SELECT 1 FROM jobs WHERE position_link=? LIMIT 1", (position_link,)
        )
        is_new_record = cur.fetchone() is None

        if is_new_record:
            dedupe_key = _cross_source_dedupe_key(source, company, title)
            if dedupe_key:
                cur.execute(
                    """
                    SELECT id, source, company, title, title_english, place, work_type, raw_text, viewed, applied, position_link
                    FROM jobs
                    """
                )
                for row in cur.fetchall():
                    existing_id = int(row[0] or 0)
                    existing_source = str(row[1] or "").strip() or _provider_from_link(
                        str(row[10] or "")
                    )
                    existing_key = _cross_source_dedupe_key(
                        existing_source,
                        str(row[2] or ""),
                        str(row[3] or ""),
                    )
                    if existing_key != dedupe_key:
                        continue

                    merged_company = str(row[2] or "") or company
                    merged_title = sanitize_job_title(str(row[3] or "") or title)
                    merged_place = str(row[5] or "")
                    merged_work_type = str(row[6] or "")
                    merged_raw = str(row[7] or "")
                    merged_viewed = int(row[8] or 0)
                    merged_applied = int(row[9] or 0)

                    if not merged_company and company:
                        merged_company = company
                    if not merged_title and title:
                        merged_title = title
                    if (not merged_place or merged_place.lower() == "unknown") and place:
                        merged_place = place
                    if (
                        not merged_work_type
                        or merged_work_type.lower() == "unknown"
                    ) and work_type:
                        merged_work_type = work_type
                    if len(raw_text) > len(merged_raw):
                        merged_raw = raw_text

                    cur.execute(
                        """
                        UPDATE jobs
                        SET source=?, company=?, title=?, title_english=?, place=?, work_type=?, raw_text=?,
                            viewed=?, applied=?, updated_at=?
                        WHERE id=?
                        """,
                        (
                            source or existing_source,
                            merged_company,
                            sanitize_job_title(merged_title),
                            "",
                            merged_place,
                            merged_work_type or "Unknown",
                            merged_raw,
                            merged_viewed,
                            merged_applied,
                            now,
                            existing_id,
                        ),
                    )
                    conn.commit()
                    return False

        if is_new_record:
            cur.execute(
                """
                            INSERT INTO jobs (source, company, title, place, work_type, position_link, raw_text, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source,
                    company,
                    title,
                    place,
                    work_type,
                    position_link,
                    raw_text,
                    now,
                    now,
                ),
            )
            conn.commit()
        return is_new_record
    finally:
        conn.close()


def set_job_summary(db_path: str, job_id: int, summary: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET summary=?, updated_at=? WHERE id=?",
            (summary, datetime.now(timezone.utc).isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_description(db_path: str, job_id: int, description: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET description=?, updated_at=? WHERE id=?",
            (description, datetime.now(timezone.utc).isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_title_english(db_path: str, job_id: int, title_english: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET title_english=?, updated_at=? WHERE id=?",
            (title_english, datetime.now(timezone.utc).isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def append_applied_job_raw_text(
    db_path: str,
    job_id: int,
    manual_text: str,
    marker: str = "[MANUAL_APPLIED_DESCRIPTION]",
    max_total_chars: int = 120000,
) -> bool:
    """Append manual description text to raw_text for an applied job only."""
    cleaned = (manual_text or "").strip()
    if not cleaned:
        return False

    block = f"{marker}\n{cleaned}".strip()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT raw_text, applied FROM jobs WHERE id=?", (int(job_id),))
        row = cur.fetchone()
        if not row:
            return False

        raw_text = (row[0] or "").strip()
        applied = int(row[1] or 0)
        if applied != 1:
            return False

        if block in raw_text:
            return True

        merged = f"{raw_text}\n\n{block}".strip() if raw_text else block
        merged = merged[-max_total_chars:]
        cur.execute(
            "UPDATE jobs SET raw_text=?, updated_at=? WHERE id=?",
            (merged, datetime.now(timezone.utc).isoformat(), int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_feedback(db_path: str, job_id: int, signal: str) -> bool:
    normalized = (signal or "").strip().lower()
    if normalized not in {"relevant", "not relevant"}:
        raise ValueError(f"Unsupported signal: {signal}")

    relevant = 1 if normalized == "relevant" else 0
    now = datetime.now(timezone.utc).isoformat()

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        applied = 0 if normalized == "not relevant" else None
        cur.execute(
            """
            UPDATE jobs
            SET relevant=?, category=?, relevance_reason=?, applied=COALESCE(?, applied), updated_at=?
            WHERE id=?
            """,
            (
                relevant,
                normalized,
                f"manual_feedback={normalized}",
                applied,
                now,
                int(job_id),
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_viewed(db_path: str, job_id: int, viewed: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    viewed_int = 1 if viewed else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET viewed=?, applied=CASE WHEN ?=0 THEN 0 ELSE applied END, updated_at=? WHERE id=?",
            (viewed_int, viewed_int, now, int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_applied(db_path: str, job_id: int, applied: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    applied_int = 1 if applied else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        if applied_int == 1:
            cur.execute(
                """
                UPDATE jobs
                SET applied=1, viewed=1, relevant=1, category='relevant', relevance_reason='manual_feedback=relevant', updated_at=?
                WHERE id=?
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET applied=0, updated_at=? WHERE id=?",
                (now, int(job_id)),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()



def update_jobs_relevance(db_path: str, updates: list[tuple[int, float, str, int, str]], prune_irrelevant: bool = False):
    from datetime import datetime, timezone
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        for rid, score, reason, relevant, category in updates:
            cur.execute(
                "UPDATE jobs SET relevance_score=?, relevance_reason=?, relevant=?, category=?, updated_at=? WHERE id=?",
                (score, reason, relevant, category, now, rid),
            )

        if prune_irrelevant:
            cur.execute("DELETE FROM jobs WHERE category='not relevant'")

        conn.commit()
    finally:
        conn.close()


def delete_jobs(db_path: str, rids: list[int]):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for rid in rids:
            cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
            cur.execute("DELETE FROM job_skills WHERE job_id=?", (rid,))
        conn.commit()
    finally:
        conn.close()


def update_job_source(db_path: str, job_id: int, source: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET source=? WHERE id=?", (source, job_id))
        conn.commit()
    finally:
        conn.close()



def batch_update_and_delete_jobs(db_path: str, updates: list[tuple], deletes: list[int]):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for u in updates:
            # (company, title, place, work_type, raw_text, viewed, applied, updated_at, id)
            cur.execute(
                "UPDATE jobs SET company=?, title=?, place=?, work_type=?, raw_text=?, viewed=?, applied=?, updated_at=? WHERE id=?",
                u
            )
        for rid in deletes:
            cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
        conn.commit()
    finally:
        conn.close()


