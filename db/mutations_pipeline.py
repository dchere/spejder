"""Pipeline mutations: viewed/applied/hidden/interview/feedback."""
from datetime import datetime, timezone

from .connection import _connect

_INTERVIEW_FIELDS_CLEAR = (
    "on_interview=0, interview_stopped=0, company_feedback=NULL, "
    "cover_letter=NULL, cover_letter_requested=0, applied_at=NULL"
)
# Clears hidden when merge/batch writes viewed=1 or applied=1 (placeholders: viewed, applied).
_HIDDEN_CLEAR_IF_VIEWED_OR_APPLIED = (
    "hidden=CASE WHEN ? = 1 OR ? = 1 THEN 0 ELSE hidden END"
)

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
                "UPDATE jobs SET viewed=1, hidden=0, updated_at=? WHERE id=?",
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
                SET applied=1, viewed=1, hidden=0, relevant=1, category='relevant',
                    relevance_reason='manual_feedback=relevant',
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


def set_job_hidden(db_path: str, job_id: int, hidden: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    hidden_int = 1 if hidden else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        if hidden_int == 1:
            cur.execute(
                f"""
                UPDATE jobs
                SET hidden=1, viewed=0, applied=0, {_INTERVIEW_FIELDS_CLEAR}, updated_at=?
                WHERE id=?
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET hidden=0, updated_at=? WHERE id=?",
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

