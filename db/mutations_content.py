"""Content mutations: summary, description, place, title, raw-text, cover letter."""
from datetime import datetime, timezone

from .connection import _connect

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

