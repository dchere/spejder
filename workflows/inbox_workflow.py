import os
import re

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import (
    ensure_db,
    get_applied_jobs,
    get_jobs_by_category,
    get_relevant_jobs,
    get_viewed_jobs_count,
    set_job_description,
    set_job_summary,
)
from spejder.extractors.skill_extractor import (
    _build_skills_tab_items,
    _ensure_skill_pattern_seed_migration,
    _get_or_extract_job_skills,
    _learn_skill_patterns_from_positions,
)
from spejder.jobs import apply_relevance, ingest_docs_to_db, update_profile_from_db_signals
from spejder.llm import LocalLLM
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.managers.language_manager import finalize_title_english as _finalize_title_english
from spejder.managers.language_manager import (
    get_title_english_for_row as _get_title_english_for_row,
)
from spejder.managers.language_manager import (
    normalize_title_compare_key as _normalize_title_compare_key,
)
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)
from spejder.managers.language_manager import (
    translate_title_to_english as _translate_title_to_english,
)
from spejder.parsers import email_parser
from spejder.workflows.job_enrichment import (
    _build_description_summary,
    _build_title_fields,
    _enrich_raw_text_with_position_page,
    _fallback_description_text,
    _generate_missing_descriptions_for_ingest,
    _has_invalid_description_marker,
    _is_invalid_summary_text,
    _is_low_quality_description,
    _summary_for_display,
)
from spejder.workflows.reporting import (
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
)

MAX_INGEST_FILE_STATS_LINES = 10

def _delete_processed_inbox_files(ingest_stats: dict, inbox_root: str = "") -> dict[str, int]:
    rows = ingest_stats.get("positions_by_file") or []
    if not isinstance(rows, list) or not rows:
        return {"eligible": 0, "deleted": 0, "missing": 0, "failed": 0}

    root = os.path.abspath(inbox_root) if inbox_root else ""
    eligible = 0
    deleted = 0
    missing = 0
    failed = 0

    for row in rows:
        found = int(row.get("found", 0) or 0)
        file_path = str(row.get("file", "") or "").strip()
        if found <= 0 or not file_path:
            continue

        abs_path = os.path.abspath(file_path)
        if root:
            try:
                if os.path.commonpath([abs_path, root]) != root:
                    continue
            except Exception:
                continue

        eligible += 1

        if not os.path.exists(abs_path):
            missing += 1
            continue
        if not os.path.isfile(abs_path):
            failed += 1
            continue

        try:
            os.remove(abs_path)
            deleted += 1
        except Exception:
            failed += 1

    return {
        "eligible": int(eligible),
        "deleted": int(deleted),
        "missing": int(missing),
        "failed": int(failed),
    }


def process_inbox(inbox: str = None, db: str = None, profile: str = None, model: str = "", report_dir: str = None, limit: int = 0, max_tokens: int = 220, max_input_chars: int = None, prune_irrelevant: bool = False, verbose: bool = False):
    profile_path = profile or DEFAULT_PROFILE_PATH
    profile = load_runtime_profile(profile_path)
    inbox = inbox or profile.default_inbox or "./inbox"
    db_path = db or profile.default_db or "./jobs.db"
    report_dir = report_dir or profile.default_report_dir or "./outbox"
    model_path = model or profile.default_model or ""
    max_input_chars = (
        max_input_chars
        if max_input_chars is not None
        else int(profile.max_input_chars or 4500)
    )

    docs = email_parser.load_files(inbox)
    if not docs:
        print("No documents found in inbox:", inbox)
        return

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)
    text_translation_cache: dict[str, str] = {}

    def _translate_job_entry_for_storage(entry: dict) -> dict:
        entry = dict(entry)
        entry["raw_text"] = _translate_text_to_english_if_needed(
            str(entry.get("raw_text", "") or ""),
            runtime_profile=profile,
            translation_cache=text_translation_cache,
        )
        title_value = str(entry.get("title", "") or "")
        try:
            title_english = _translate_title_to_english(
                title_value,
                runtime_profile=profile,
                title_translation_cache=title_translation_cache,
            )
        except Exception:
            try:
                title_english = _translate_text_to_english_if_needed(
                    title_value,
                    runtime_profile=profile,
                    translation_cache=text_translation_cache,
                )
            except Exception:
                title_english = title_value
        final_title_english = _finalize_title_english(title_english, title_value)
        if _normalize_title_compare_key(final_title_english) == _normalize_title_compare_key(
            title_value
        ):
            final_title_english = ""
        entry["title_english"] = final_title_english
        return entry

    title_translation_cache: dict[str, str] = {}
    ingest_stats = ingest_docs_to_db(db_path, docs, entry_transform=_translate_job_entry_for_storage)
    print(
        "Ingestion done: "
        f"processed={ingest_stats.get('processed', 0)}, "
        f"inserted_new={ingest_stats.get('inserted_new', 0)}, "
        f"skipped_existing={ingest_stats.get('skipped_existing', 0)} "
        f"into DB: {db_path}"
    )
    _print_ingest_file_stats(ingest_stats)
    delete_stats = _delete_processed_inbox_files(ingest_stats, inbox_root=inbox)
    print(
        "Inbox cleanup: "
        f"eligible={delete_stats.get('eligible', 0)}, "
        f"deleted={delete_stats.get('deleted', 0)}, "
        f"missing={delete_stats.get('missing', 0)}, "
        f"failed={delete_stats.get('failed', 0)}"
    )

    total, relevant_count = apply_relevance(
        db_path, profile, prune_irrelevant=prune_irrelevant
    )
    print(f"Scored {total} positions; relevant={relevant_count}")

    relevant_jobs = get_relevant_jobs(db_path, limit=limit)
    llm = LocalLLM(model_path=model_path, n_ctx=int(profile.n_ctx), verbose=bool(verbose)) if model_path else None
    if not llm:
        raise SystemExit("Model init: model is required for process-inbox")

    desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
        db_path, llm=llm, runtime_profile=profile, allow_empty=False
    )
    print(f"Descriptions generated during ingest: updated={desc_updated}, skipped={desc_skipped}")

    skill_learning = _learn_skill_patterns_from_positions(
        db_path,
        runtime_profile=profile,
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

    learning_info = update_profile_from_db_signals(db_path, profile_path)
    print(
        "Profile learning: "
        f"labeled={learning_info.get('labeled_count', 0)}, "
        f"include={learning_info.get('learned_include_count', 0)}, "
        f"exclude={learning_info.get('learned_exclude_count', 0)}, "
        f"missing_skills={learning_info.get('missing_skills_count', 0)}"
    )

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

    report_data = {}
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
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
            raw_text = _enrich_raw_text_with_position_page(
                db_path,
                row,
                page_context_cache=page_context_cache,
                llm=llm,
                runtime_profile=profile,
                title_translation_cache=title_translation_cache,
            )
            skills = _get_or_extract_job_skills(
                db_path,
                row.get("id", 0),
                raw_text,
                llm=llm,
                profile=profile,
                position_link=row.get("position_link", ""),
                page_context_cache=page_context_cache,
                limit=10,
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
                            title_translation_cache=title_translation_cache,
                        ),
                    )
                ):
                    description = generated
                    set_job_description(db_path, row.get("id", 0), description)
            if not description:
                description = _fallback_description_text("", source_raw or raw_text)
                if description:
                    set_job_description(db_path, row.get("id", 0), description)
            records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    **_build_title_fields(
                        db_path,
                        row,
                        runtime_profile=profile,
                        title_translation_cache=title_translation_cache,
                    ),
                    "place": row.get("place", ""),
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

    applied_rows = get_applied_jobs(db_path, limit=0)
    applied_records = []
    for row in applied_rows:
        summary = _summary_for_display(
            row.get("summary", ""),
            row.get("raw_text", ""),
        )
        description = row.get("description") or ""
        raw_text = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            runtime_profile=profile,
            title_translation_cache=title_translation_cache,
        )
        skills = _get_or_extract_job_skills(
            db_path,
            row.get("id", 0),
            raw_text,
            llm=llm,
            profile=profile,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
            limit=10,
        )
        applied_records.append(
            {
                "id": row.get("id", 0),
                "source": row.get("source", "Unknown"),
                "company": row.get("company", ""),
                **_build_title_fields(
                    db_path,
                    row,
                    runtime_profile=profile,
                    title_translation_cache=title_translation_cache,
                ),
                "place": row.get("place", ""),
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
            }
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
    )
    print(f"Report written: {dashboard_path}")

    if not relevant_jobs:
        print("No relevant positions after filtering.")

    print(f"Done. Relevant summarized={len(relevant_jobs)}")



def _print_ingest_file_stats(ingest_stats: dict) -> None:
    rows = ingest_stats.get("positions_by_file") or []
    if not isinstance(rows, list) or not rows:
        return

    print("Positions found by file:")
    shown = 0
    for row in rows:
        if shown >= MAX_INGEST_FILE_STATS_LINES:
            remaining = len(rows) - shown
            print(f"  ... and {remaining} more files")
            break
        file_path = str(row.get("file", "") or "")
        file_label = file_path if file_path else "(unknown file)"
        found = int(row.get("found", 0) or 0)
        inserted = int(row.get("inserted_new", 0) or 0)
        skipped = int(row.get("skipped_existing", 0) or 0)
        print(
            f"  - {file_label}: found={found}, inserted_new={inserted}, skipped_existing={skipped}"
        )
        shown += 1


