import os

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db.connection import ensure_db
from spejder.db.mutations import set_job_description
from spejder.db.queries import (
    get_all_applied_jobs,
    get_jobs_by_category,
    get_jobs_for_description_refresh,
    get_viewed_jobs_count,
)
from spejder.extractors.skill_extractor import (
    _build_skills_tab_items,
    _ensure_skill_pattern_seed_migration,
    _learn_skill_patterns_from_positions,
)
from spejder.llm import LocalLLM
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.workflows.job_enrichment import (
    _build_description_summary,
    _build_title_fields,
    _enrich_raw_text_with_position_page,
    _fallback_description_text,
    _get_title_english_for_row,
    _has_invalid_description_marker,
    _is_low_quality_description,
    _summary_for_display,
    materialize_job_skills,
    materialize_relevant_and_applied_skills,
)
from spejder.workflows.report_workflow import (
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
)


def refresh_descriptions(profile: str = None, db: str = None, model: str = "", source: str = "", category: str = "", link: list = None, job_id: list = None, limit: int = 0, overwrite: bool = False, allow_empty: bool = False, quiet_model: bool = False, report_dir: str = ""):
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"
    model_path = model or runtime_profile.default_model or ""

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)

    llm = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=not quiet_model) if model_path else None
    if not llm:
        raise SystemExit("Model init: model is required for refresh-descriptions")

    rows = get_jobs_for_description_refresh(
        db_path,
        category=category,
        source=source,
        links=link,
        job_ids=job_id,
        limit=limit,
        missing_only=not overwrite,
    )

    if not rows:
        print("No matching jobs found for description refresh.")
        return

    updated = 0
    skipped = 0
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    for row in rows:
        source_raw = row.get("raw_text", "") or ""
        raw = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        )
        if not raw:
            skipped += 1
            continue
        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            runtime_profile=runtime_profile,
            page_context_cache=page_context_cache,
        )
        if (
            not description
            or _has_invalid_description_marker(description)
            or _is_low_quality_description(
                description,
                raw_text=raw,
                title=_get_title_english_for_row(
                    db_path,
                    row,
                    runtime_profile=runtime_profile,
                    title_translation_cache=title_translation_cache,
                ),
            )
        ):
            description = _fallback_description_text("", source_raw or raw)
        if not description and not allow_empty:
            skipped += 1
            continue
        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    print(f"Description refresh done. matched={len(rows)}, updated={updated}, skipped={skipped}")

    materialize_relevant_and_applied_skills(
        db_path,
        llm=llm,
        runtime_profile=runtime_profile,
        rescore=True,
        skip_cached=True,
        progress_label="Skill materialization",
    )

    skill_learning = _learn_skill_patterns_from_positions(
        db_path,
        runtime_profile=runtime_profile,
        llm=llm,
        progress=True,
        progress_label="Skill pattern learning",
    )
    print(
        "Skill pattern learning: "
        f"considered={skill_learning.get('considered_positions', 0)}, "
        f"new_patterns={skill_learning.get('new_skill_patterns', 0)}, "
        f"total_patterns={skill_learning.get('total_known_skill_patterns', 0)}"
    )

    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        report_data = {}
        page_context_cache: dict[str, str] = {}
        title_translation_cache: dict[str, str] = {}
        for cat in ["relevant", "not relevant"]:
            cat_rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
            records = []
            for row in cat_rows:
                skills, raw_text, _ = materialize_job_skills(
                    db_path,
                    row,
                    llm=llm,
                    runtime_profile=runtime_profile,
                    page_context_cache=page_context_cache,
                    title_translation_cache=title_translation_cache,
                    rescore=False,
                )
                records.append(
                    {
                        "id": row.get("id", 0),
                        "source": row.get("source", "Unknown"),
                        "company": row.get("company", ""),
                        **_build_title_fields(
                            db_path,
                            row,
                            runtime_profile=runtime_profile,
                            title_translation_cache=title_translation_cache,
                        ),
                        "place": row.get("place", ""),
                        "work_type": row.get("work_type", "Unknown"),
                        "description": _fallback_description_text(
                            row.get("description") or "",
                            raw_text or row.get("raw_text") or "",
                        ),
                        "skills": skills,
                        "position_link": row.get("position_link", ""),
                        "raw_text": raw_text,
                        "relevance_score": row.get("relevance_score", 0),
                        "relevance_reason": row.get("relevance_reason", ""),
                        "summary": _summary_for_display(
                            row.get("summary", ""),
                            raw_text or row.get("raw_text") or "",
                        ),
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
            skills, raw_text, _ = materialize_job_skills(
                db_path,
                row,
                llm=llm,
                runtime_profile=runtime_profile,
                page_context_cache=page_context_cache,
                title_translation_cache=title_translation_cache,
                rescore=False,
            )
            record = {
                "id": row.get("id", 0),
                "source": row.get("source", "Unknown"),
                "company": row.get("company", ""),
                **_build_title_fields(
                    db_path,
                    row,
                    runtime_profile=runtime_profile,
                    title_translation_cache=title_translation_cache,
                ),
                "place": row.get("place", ""),
                "work_type": row.get("work_type", "Unknown"),
                "description": _fallback_description_text(
                    row.get("description") or "", raw_text or row.get("raw_text") or ""
                ),
                "skills": skills,
                "position_link": row.get("position_link", ""),
                "raw_text": raw_text,
                "relevance_score": row.get("relevance_score", 0),
                "relevance_reason": row.get("relevance_reason", ""),
                "summary": _summary_for_display(
                    row.get("summary", ""),
                    raw_text or row.get("raw_text") or "",
                ),
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

        dashboard_path = os.path.join(report_dir, "report.html")
        _render_html_dashboard(
            report_data.get("relevant", []),
            report_data.get("not relevant", []),
            applied_records,
            dashboard_path,
            "Positions Report",
            viewed_total=get_viewed_jobs_count(db_path),
            skills_items=_build_skills_tab_items(db_path, runtime_profile),
            report_max_relevant_positions=_report_max_relevant_positions(runtime_profile),
            report_max_not_relevant_positions=_report_max_not_relevant_positions(runtime_profile),
            interview_items=interview_records,
            stopped_items=stopped_records,
            runtime_profile=runtime_profile,
        )
        print(f"Report written: {dashboard_path}")

