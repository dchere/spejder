import sqlite3
import re
import time
import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse
from .connection import _connect
from .connection import get_job_link
from .utils import sanitize_job_title, _normalize_position_link, _provider_from_link
from .deduplication_utils import _keeper_sort_key, _merge_duplicate_into_keeper, _position_dedupe_key

_INTERVIEW_FIELDS_CLEAR = (
    "on_interview=0, interview_stopped=0, company_feedback=NULL, "
    "cover_letter=NULL, cover_letter_requested=0, applied_at=NULL"
)


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
            dedupe_key = _position_dedupe_key(company, title, place)
            if dedupe_key:
                cur.execute(
                    """
                    SELECT id, source, company, title, title_english, place, work_type, raw_text, viewed, applied, position_link, created_at
                    FROM jobs
                    """
                )
                matches = []
                for row in cur.fetchall():
                    existing_key = _position_dedupe_key(
                        str(row[2] or ""), str(row[3] or ""), str(row[5] or "")
                    )
                    if existing_key != dedupe_key:
                        continue
                    matches.append({
                        "id": int(row[0] or 0),
                        "source": str(row[1] or "").strip() or _provider_from_link(str(row[10] or "")),
                        "company": str(row[2] or ""),
                        "title": str(row[3] or ""),
                        "place": str(row[5] or ""),
                        "work_type": str(row[6] or ""),
                        "raw_text": str(row[7] or ""),
                        "viewed": int(row[8] or 0),
                        "applied": int(row[9] or 0),
                        "position_link": str(row[10] or ""),
                        "created_at": str(row[11] or ""),
                    })

                if matches:
                    matches.sort(key=_keeper_sort_key)
                    keeper = dict(matches[0])
                    incoming = {
                        "company": company,
                        "title": title,
                        "place": place,
                        "work_type": work_type,
                        "raw_text": raw_text,
                        "viewed": 0,
                        "applied": 0,
                    }
                    _merge_duplicate_into_keeper(keeper, incoming)

                    cur.execute(
                        """
                        UPDATE jobs
                        SET company=?, title=?, title_english=?, place=?, work_type=?, raw_text=?,
                            viewed=?, applied=?, updated_at=?
                        WHERE id=?
                        """,
                        (
                            keeper["company"],
                            sanitize_job_title(keeper["title"]),
                            "",
                            keeper["place"],
                            keeper["work_type"] or "Unknown",
                            keeper["raw_text"],
                            keeper["viewed"],
                            keeper["applied"],
                            now,
                            keeper["id"],
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

        cur.execute(
            """
            SELECT id, company, title, place, work_type, raw_text
            FROM jobs
            WHERE position_link=?
            LIMIT 1
            """,
            (position_link,),
        )
        existing = cur.fetchone()
        if existing:
            job_id = int(existing[0] or 0)
            merged_company = str(existing[1] or "") or company
            merged_title = sanitize_job_title(str(existing[2] or "") or title)
            merged_place = str(existing[3] or "")
            merged_work_type = str(existing[4] or "") or work_type
            merged_raw = str(existing[5] or "")

            provider_company = _provider_from_link(position_link)
            if company and (
                not merged_company
                or (provider_company and provider_company == company and merged_company != company)
            ):
                merged_company = company
            if title and (not merged_title or len(title) > len(merged_title)):
                merged_title = sanitize_job_title(title)
            if place and (not merged_place or merged_place.lower() == "unknown"):
                merged_place = place
            if work_type and (
                not merged_work_type or merged_work_type.lower() == "unknown"
            ):
                merged_work_type = work_type
            if len(raw_text) > len(merged_raw):
                merged_raw = raw_text

            if (
                merged_company != str(existing[1] or "")
                or merged_title != sanitize_job_title(str(existing[2] or ""))
                or merged_place != str(existing[3] or "")
                or merged_work_type != str(existing[4] or "")
                or merged_raw != str(existing[5] or "")
            ):
                cur.execute(
                    """
                    UPDATE jobs
                    SET source=?, company=?, title=?, place=?, work_type=?, raw_text=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        source,
                        merged_company,
                        merged_title,
                        merged_place,
                        merged_work_type or "Unknown",
                        merged_raw,
                        now,
                        job_id,
                    ),
                )
                conn.commit()
        return False
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


def set_job_place(db_path: str, job_id: int, place: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET place=?, updated_at=? WHERE id=?",
            (str(place or "").strip(), datetime.now(timezone.utc).isoformat(), job_id),
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


def set_job_cover_letter_requested(db_path: str, job_id: int, requested: bool) -> bool:
    requested_int = 1 if requested else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT applied, cover_letter FROM jobs WHERE id=?",
            (int(job_id),),
        )
        row = cur.fetchone()
        if not row:
            return False
        if int(row[0] or 0) != 1:
            return False
        if str(row[1] or "").strip():
            return False

        cur.execute(
            "UPDATE jobs SET cover_letter_requested=?, updated_at=? WHERE id=?",
            (requested_int, datetime.now(timezone.utc).isoformat(), int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_cover_letter(db_path: str, job_id: int, text: str) -> bool:
    cleaned = (text or "").strip()
    if not cleaned:
        return False

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT applied, cover_letter_requested, cover_letter FROM jobs WHERE id=?",
            (int(job_id),),
        )
        row = cur.fetchone()
        if not row:
            return False
        if int(row[0] or 0) != 1:
            return False
        if int(row[1] or 0) != 1:
            return False
        if str(row[2] or "").strip():
            return True

        cur.execute(
            "UPDATE jobs SET cover_letter=?, updated_at=? WHERE id=?",
            (cleaned, datetime.now(timezone.utc).isoformat(), int(job_id)),
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
        if normalized == "not relevant":
            cur.execute(
                f"""
                UPDATE jobs
                SET relevant=?, category=?, relevance_reason=?, applied=0, {_INTERVIEW_FIELDS_CLEAR}, updated_at=?
                WHERE id=?
                """,
                (
                    relevant,
                    normalized,
                    f"manual_feedback={normalized}",
                    now,
                    int(job_id),
                ),
            )
        else:
            cur.execute(
                """
                UPDATE jobs
                SET relevant=?, category=?, relevance_reason=?, updated_at=?
                WHERE id=?
                """,
                (
                    relevant,
                    normalized,
                    f"manual_feedback={normalized}",
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
        if viewed_int == 0:
            cur.execute(
                f"""
                UPDATE jobs
                SET viewed=0, applied=0, {_INTERVIEW_FIELDS_CLEAR}, updated_at=?
                WHERE id=?
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET viewed=1, updated_at=? WHERE id=?",
                (now, int(job_id)),
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
                SET applied=1, viewed=1, relevant=1, category='relevant', relevance_reason='manual_feedback=relevant',
                    applied_at=COALESCE(applied_at, ?), updated_at=?
                WHERE id=?
                """,
                (now, now, int(job_id)),
            )
        else:
            cur.execute(
                f"""
                UPDATE jobs
                SET applied=0, {_INTERVIEW_FIELDS_CLEAR}, updated_at=?
                WHERE id=?
                """,
                (now, int(job_id)),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_on_interview(db_path: str, job_id: int, on_interview: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    on_interview_int = 1 if on_interview else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        if on_interview_int == 1:
            cur.execute(
                """
                UPDATE jobs
                SET on_interview=1, interview_stopped=0, updated_at=?
                WHERE id=? AND applied=1
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET on_interview=0, updated_at=? WHERE id=?",
                (now, int(job_id)),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_interview_stopped(db_path: str, job_id: int, stopped: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    stopped_int = 1 if stopped else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        if stopped_int == 1:
            cur.execute(
                """
                UPDATE jobs
                SET interview_stopped=1, on_interview=0, updated_at=?
                WHERE id=? AND applied=1
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET interview_stopped=0, updated_at=? WHERE id=?",
                (now, int(job_id)),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_company_feedback(db_path: str, job_id: int, feedback: str) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE jobs SET company_feedback=?, updated_at=?
            WHERE id=? AND applied=1 AND interview_stopped=1
            """,
            (feedback, now, int(job_id)),
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
            applied = int(u[6] or 0)
            if applied == 0:
                cur.execute(
                    f"UPDATE jobs SET company=?, title=?, place=?, work_type=?, raw_text=?, viewed=?, applied=?, {_INTERVIEW_FIELDS_CLEAR}, updated_at=? WHERE id=?",
                    u,
                )
            else:
                cur.execute(
                    "UPDATE jobs SET company=?, title=?, place=?, work_type=?, raw_text=?, viewed=?, applied=?, updated_at=? WHERE id=?",
                    u,
                )
        for rid in deletes:
            cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
        conn.commit()
    finally:
        conn.close()


