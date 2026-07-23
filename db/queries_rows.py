"""Shared row-to-dict mappers for job query results."""

from .utils import _provider_from_link


def _map_full_job_row(r, category: str) -> dict:
    return {
        "id": r[0],
        "source": (r[1] or _provider_from_link(r[7] or "")) if len(r) > 1 else "Unknown",
        "company": r[2] or "",
        "title": r[3] or "",
        "title_english": r[4] or "",
        "place": r[5] or "",
        "work_type": r[6] or "Unknown",
        "position_link": r[7] or "",
        "raw_text": r[8] or "",
        "relevance_score": float(r[9] or 0),
        "relevance_reason": r[10] or "",
        "summary": r[11] or "",
        "viewed": int(r[12] or 0),
        "applied": int(r[13] or 0),
        "on_interview": int(r[14] or 0),
        "interview_stopped": int(r[15] or 0),
        "company_feedback": r[16] or "",
        "description": r[17] or "",
        "cover_letter": r[18] or "",
        "cover_letter_requested": int(r[19] or 0),
        "applied_at": r[20] or "",
        "hidden": int(r[21] or 0),
        "category": category,
    }


def _map_company_job_row(r) -> dict:
    row = _map_full_job_row(r, r[22] or "not relevant")
    return row


def _map_applied_job_row(r) -> dict:
    return _map_full_job_row(r, r[22] or "relevant")


def _map_refresh_job_row(r) -> dict:
    return {
        "id": r[0],
        "source": (r[1] or _provider_from_link(r[7] or "")) if len(r) > 1 else "Unknown",
        "company": r[2] or "",
        "title": r[3] or "",
        "title_english": r[4] or "",
        "place": r[5] or "",
        "work_type": r[6] or "Unknown",
        "position_link": r[7] or "",
        "raw_text": r[8] or "",
        "category": r[9] or "",
        "description": r[10] or "",
        "summary": r[11] or "",
    }
