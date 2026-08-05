import os
import re
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    get_all_applied_jobs,
    get_jobs_by_category,
    get_viewed_jobs_count,
    set_job_description,
    set_job_summary,
)
from spejder.extractors.skill_extractor import _build_skills_tab_items
from spejder.llm import LocalLLM
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.managers.language_manager import get_title_english_for_row as _get_title_english_for_row
from spejder.workflows.dashboard import (
    build_hidden_dashboard_records,
    build_viewed_today_dashboard_records,
)
from spejder.workflows.job_enrichment import (
    _build_description_summary,
    _build_title_fields,
    _fallback_description_text,
    _has_invalid_description_marker,
    _is_invalid_summary_text,
    _is_low_quality_description,
    _summary_for_display,
    materialize_job_skills,
)
from spejder.workflows.report_workflow import (
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
)


def summarize_relevant_jobs_for_inbox(
    db_path: str,
    relevant_jobs: list[dict],
    llm: LocalLLM,
    *,
    max_tokens: int,
    max_input_chars: Optional[int],
) -> None:
    for job in relevant_jobs:
        company = job.get("company", "")
        title = job.get("title_english", "") or job.get("title", "")
        link = job.get("position_link", "")
        text = job.get("raw_text", "")

        compact = re.sub(r"https?://\S+", "[link]", text)
        compact = re.sub(r"\s+", " ", compact).strip()
        text_for_llm = (
            compact[:max_input_chars] if max_input_chars and max_input_chars > 0 else compact
        )

        prompt = (
            "Summarize this job posting in 4 bullets: role focus, key requirements, "
            "location/remote info, and why it may fit the candidate.\n\n"
            f"Company: {company}\nTitle: {title}\nLink: {link}\n\n"
            f"Content:\n{text_for_llm}\n\nSummary:"
        )
        try:
            summary = llm.generate(prompt, max_tokens=max_tokens)
        except Exception as exc:
            print(f"Summary generation failed for job_id={job.get('id', 0)}: {exc}")
            summary = ""

        if _is_invalid_summary_text(summary):
            summary = ""

        set_job_summary(db_path, job["id"], summary)

        print(f"[{job.get('relevance_score', 0):.1f}] {title} @ {company}")
        print(f"    {link}")
        print(f"    {summary[:300]}")
        print()


def write_inbox_dashboard_report(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    report_dir: str,
) -> str:
    os.makedirs(report_dir, exist_ok=True)

    for legacy_name in [
        "other.html",
        "unrelevant.html",
        "relevant.html",
        "not_relevant.html",
    ]:
        legacy_path = os.path.join(report_dir, legacy_name)
        if os.path.exists(legacy_path):
            os.remove(legacy_path)

    report_data: dict[str, list[dict]] = {}
    page_context_cache: dict[str, str] = {}
    report_title_translation_cache: dict[str, str] = {}
    for cat in ["relevant", "not relevant"]:
        rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
        records = []
        for row in rows:
            summary = _summary_for_display(
                row.get("summary", ""),
                row.get("raw_text", ""),
            )
            description = row.get("description") or ""
            source_raw = row.get("raw_text", "") or ""
            skills, raw_text, _ = materialize_job_skills(
                db_path,
                row,
                llm=llm,
                runtime_profile=profile,
                page_context_cache=page_context_cache,
                title_translation_cache=report_title_translation_cache,
                rescore=False,
            )
            if not description:
                generated = _build_description_summary(
                    raw_text,
                    llm=llm,
                    position_link=row.get("position_link", ""),
                    runtime_profile=profile,
                    page_context_cache=page_context_cache,
                )
                if (
                    generated
                    and not _has_invalid_description_marker(generated)
                    and not _is_low_quality_description(
                        generated,
                        raw_text=raw_text,
                        title=_get_title_english_for_row(
                            db_path,
                            row,
                            runtime_profile=profile,
                            title_translation_cache=report_title_translation_cache,
                        ),
                    )
                ):
                    description = generated
                    set_job_description(db_path, row.get("id", 0), description)
            if not description:
                description = _fallback_description_text("", source_raw or raw_text)
                if description:
                    set_job_description(db_path, row.get("id", 0), description)
            title_fields = _build_title_fields(
                db_path,
                row,
                runtime_profile=profile,
                title_translation_cache=report_title_translation_cache,
            )
            records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    **title_fields,
                    "place": title_fields.get("place", row.get("place", "")),
                    "work_type": row.get("work_type", "Unknown"),
                    "description": description,
                    "skills": skills,
                    "position_link": row.get("position_link", ""),
                    "raw_text": raw_text,
                    "relevance_score": row.get("relevance_score", 0),
                    "relevance_reason": row.get("relevance_reason", ""),
                    "summary": summary,
                    "category": cat,
                    "viewed": row.get("viewed", 0),
                    "applied": row.get("applied", 0),
                }
            )
        report_data[cat] = records

    applied_records = []
    interview_records = []
    stopped_records = []
    for row in get_all_applied_jobs(db_path, limit=0):
        summary = _summary_for_display(
            row.get("summary", ""),
            row.get("raw_text", ""),
        )
        description = row.get("description") or ""
        skills, raw_text, _ = materialize_job_skills(
            db_path,
            row,
            llm=llm,
            runtime_profile=profile,
            page_context_cache=page_context_cache,
            title_translation_cache=report_title_translation_cache,
            rescore=False,
        )
        title_fields = _build_title_fields(
            db_path,
            row,
            runtime_profile=profile,
            title_translation_cache=report_title_translation_cache,
        )
        record = {
            "id": row.get("id", 0),
            "source": row.get("source", "Unknown"),
            "company": row.get("company", ""),
            **title_fields,
            "place": title_fields.get("place", row.get("place", "")),
            "work_type": row.get("work_type", "Unknown"),
            "description": description,
            "skills": skills,
            "position_link": row.get("position_link", ""),
            "raw_text": raw_text,
            "relevance_score": row.get("relevance_score", 0),
            "relevance_reason": row.get("relevance_reason", ""),
            "summary": summary,
            "category": row.get("category", "relevant"),
            "viewed": row.get("viewed", 1),
            "applied": row.get("applied", 1),
            "on_interview": int(row.get("on_interview", 0) or 0),
            "interview_stopped": int(row.get("interview_stopped", 0) or 0),
            "company_feedback": row.get("company_feedback", "") or "",
        }
        if record["interview_stopped"]:
            stopped_records.append(record)
        elif record["on_interview"]:
            interview_records.append(record)
        else:
            applied_records.append(record)

    hidden_records = build_hidden_dashboard_records(
        db_path,
        profile,
        report_title_translation_cache,
    )
    viewed_today_records = build_viewed_today_dashboard_records(
        db_path,
        profile,
        report_title_translation_cache,
    )

    dashboard_path = os.path.join(report_dir, "report.html")
    _render_html_dashboard(
        report_data.get("relevant", []),
        report_data.get("not relevant", []),
        applied_records,
        dashboard_path,
        "Positions Report",
        viewed_total=get_viewed_jobs_count(db_path),
        skills_items=_build_skills_tab_items(db_path, profile),
        report_max_relevant_positions=_report_max_relevant_positions(profile),
        report_max_not_relevant_positions=_report_max_not_relevant_positions(profile),
        interview_items=interview_records,
        stopped_items=stopped_records,
        hidden_items=hidden_records,
        viewed_today_items=viewed_today_records,
        runtime_profile=profile,
    )
    print(f"Report written: {dashboard_path}")
    return dashboard_path
