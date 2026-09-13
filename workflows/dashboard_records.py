from spejder.config import AppConfig
from spejder.db import (
    get_hidden_jobs,
    get_job_skills,
    get_viewed_today_jobs,
    local_day_start_utc_iso,
)
from spejder.extractors.skill_extractor import _format_skills
from spejder.workflows.job_enrichment import (
    _build_title_fields,
    _fallback_description_text,
    _summary_for_display,
)


def build_dashboard_record(
    db_path: str,
    runtime_profile: AppConfig,
    title_translation_cache: dict[str, str],
    row: dict,
    default_category: str,
    default_viewed: int = 0,
    default_applied: int = 0,
    translate_title: bool = True,
) -> dict:
    raw_text = row.get("raw_text") or ""
    summary = _summary_for_display(
        row.get("summary", ""),
        raw_text,
    )
    cached_skills = get_job_skills(db_path, int(row.get("id", 0) or 0))
    title_fields = (
        _build_title_fields(
            db_path,
            row,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        )
        if translate_title
        else {
            "title": str(row.get("title", "") or ""),
            "title_english": str(row.get("title_english", "") or ""),
        }
    )

    return {
        "id": row.get("id", 0),
        "source": row.get("source", "Unknown"),
        "company": row.get("company", ""),
        **title_fields,
        "place": title_fields.get("place", row.get("place", "")),
        "work_type": row.get("work_type", "Unknown"),
        "description": _fallback_description_text(row.get("description") or "", raw_text),
        "skills": _format_skills(cached_skills),
        "position_link": row.get("position_link", ""),
        "raw_text": raw_text,
        "relevance_score": row.get("relevance_score", 0),
        "relevance_reason": row.get("relevance_reason", ""),
        "summary": summary,
        "category": row.get("category", default_category),
        "viewed": row.get("viewed", default_viewed),
        "applied": row.get("applied", default_applied),
        "on_interview": int(row.get("on_interview", 0) or 0),
        "interview_stopped": int(row.get("interview_stopped", 0) or 0),
        "company_feedback": row.get("company_feedback", "") or "",
        "cover_letter": row.get("cover_letter", "") or "",
        "cover_letter_requested": int(row.get("cover_letter_requested", 0) or 0),
        "applied_at": row.get("applied_at", "") or "",
        "hidden": int(row.get("hidden", 0) or 0),
    }


def build_hidden_dashboard_records(
    db_path: str,
    runtime_profile: AppConfig,
    title_translation_cache: dict[str, str],
) -> list[dict]:
    """Load Hidden-tab rows and shape them with rebuild defaults."""
    return [
        build_dashboard_record(
            db_path,
            runtime_profile,
            title_translation_cache,
            row,
            default_category=str(row.get("category") or "not relevant"),
            default_viewed=0,
            default_applied=0,
            translate_title=False,
        )
        for row in get_hidden_jobs(db_path, limit=0)
    ]


def build_viewed_today_dashboard_records(
    db_path: str,
    runtime_profile: AppConfig,
    title_translation_cache: dict[str, str],
) -> list[dict]:
    """Load Viewed-today tab rows (updated_at DESC) with rebuild defaults."""
    since_iso = local_day_start_utc_iso()
    return [
        build_dashboard_record(
            db_path,
            runtime_profile,
            title_translation_cache,
            row,
            default_category=str(row.get("category") or "not relevant"),
            default_viewed=1,
            default_applied=0,
            translate_title=False,
        )
        for row in get_viewed_today_jobs(db_path, since_iso, limit=0)
    ]
