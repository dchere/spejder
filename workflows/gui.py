# pylint: disable=all
import os
import sys
import threading
import webbrowser
import time
import json
import codecs
import html as html_lib
import re
import traceback
import subprocess
from typing import Optional
from contextlib import contextmanager
from jinja2 import Environment, FileSystemLoader
import spejder
from spejder.llm import LocalLLM
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile, save_profile
from spejder.db import *
from spejder.jobs import *
from spejder.parsers import email_parser
from spejder.managers.dashboard_manager import _render_html_from_items, _render_html_dashboard
from spejder.workflows.reporting import _report_max_not_relevant_positions, _report_max_relevant_positions
from spejder.workflows.inbox_workflow import _print_ingest_file_stats, _delete_processed_inbox_files
from spejder.jobs.parsing.core import extract_job_entries
from spejder.workflows.job_enrichment import _summary_for_display, _build_title_fields, _fallback_description_text, _enrich_raw_text_with_position_page
from spejder.managers.language_manager import translate_text_to_english_if_needed as _translate_text_to_english_if_needed, translate_title_to_english as _translate_title_to_english, finalize_title_english as _finalize_title_english, normalize_title_compare_key as _normalize_title_compare_key
from spejder.extractors.skill_extractor import _format_skills
from spejder.config import AppConfig
from spejder.extractors.skill_extractor import _ensure_skill_pattern_seed_migration, _build_skills_tab_items, _get_or_extract_job_skills, _learn_skill_patterns_from_positions

# from spejder.server import run_server
def serve_gui(
profile: str = None, report_dir: str = None, db: str = None, host: str = None, port: int = None, no_open: bool = False, verbose: bool = False):
    cli_verbose = bool(verbose)
    profile_path = os.path.abspath(profile or DEFAULT_PROFILE_PATH)
    runtime_profile = load_runtime_profile(profile_path)
    report_dir = os.path.abspath(
        report_dir or runtime_profile.default_report_dir or "./outbox"
    )
    db_path = os.path.abspath(db or runtime_profile.default_db or "./jobs.db")
    inbox_path = os.path.abspath(runtime_profile.default_inbox or "./inbox")
    model_path = runtime_profile.default_model or ""
    host = host or runtime_profile.server_host or "127.0.0.1"
    port = port if port is not None else int(runtime_profile.server_port or 8765)

    print(f"Serve GUI: initializing (profile={profile_path})")
    print(f"Serve GUI: db={db_path}, report_dir={report_dir}, inbox={inbox_path}")
    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)
    print("Serve GUI: database ready")
    os.makedirs(report_dir, exist_ok=True)
    dashboard_path = os.path.join(report_dir, "report.html")
    dashboard_lock = threading.Lock()
    rebuild_signal = threading.Event()
    rebuild_pending_lock = threading.Lock()
    rebuild_pending_reasons: list[str] = []
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    title_translation_llm: Optional[LocalLLM] = None

    def _get_title_translation_llm() -> Optional[LocalLLM]:
        nonlocal title_translation_llm
        if title_translation_llm is not None:
            return title_translation_llm
        if not model_path:
            return None
        title_translation_llm = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=False)
        return title_translation_llm

    def _persist_runtime_profile() -> None:
        save_profile(runtime_profile, profile_path)

    def _reload_runtime_profile() -> None:
        fresh = load_runtime_profile(profile_path)
        for key, value in fresh.model_dump().items():
            setattr(runtime_profile, key, value)

    def _regain_skills_for_unviewed_positions(llm_for_skills: LocalLLM = None) -> int:
        affected = clear_job_skills_for_unviewed_jobs(db_path)

        rows = []
        seen = set()
        for cat in ["relevant", "not relevant"]:
            for row in get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True):
                row_id = int(row.get("id", 0) or 0)
                if not row_id or row_id in seen:
                    continue
                seen.add(row_id)
                rows.append(row)

        updated = _populate_missing_dashboard_skills(
            rows,
            llm=llm_for_skills,
            progress_label="Skill rebuild: unviewed",
        )
        return max(int(affected), int(updated))

    def _build_dashboard_record(
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
            "place": row.get("place", ""),
            "work_type": row.get("work_type", "Unknown"),
            "description": _fallback_description_text(
                row.get("description") or "", raw_text or row.get("raw_text") or ""
            ),
            "skills": _format_skills(cached_skills, limit=10),
            "position_link": row.get("position_link", ""),
            "raw_text": raw_text,
            "relevance_score": row.get("relevance_score", 0),
            "relevance_reason": row.get("relevance_reason", ""),
            "summary": summary,
            "category": row.get("category", default_category),
            "viewed": row.get("viewed", default_viewed),
            "applied": row.get("applied", default_applied),
        }

    def _populate_missing_dashboard_skills(
        rows: list[dict], llm: LocalLLM = None, progress_label: str = ""
    ) -> int:
        if not rows:
            return 0

        page_context_cache: dict[str, str] = {}
        title_translation_cache: dict[str, str] = {}
        updated = 0
        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            job_id = int(row.get("id", 0) or 0)
            if not job_id or get_job_skills(db_path, job_id):
                continue

            raw_text = _enrich_raw_text_with_position_page(
                db_path,
                row,
                page_context_cache=page_context_cache,
                llm=llm,
                runtime_profile=runtime_profile,
                title_translation_cache=title_translation_cache,
            )
            skills = _get_or_extract_job_skills(
                db_path,
                job_id,
                raw_text,
                llm=llm,
                profile=runtime_profile,
                position_link=row.get("position_link", ""),
                page_context_cache=page_context_cache,
                limit=10,
            )
            if skills:
                updated += 1
            if progress_label and (idx % 25 == 0 or idx == total):
                print(f"{progress_label}: checked={idx}/{total}, updated={updated}")
        return updated

    def _rebuild_dashboard(reason: str = ""):
        should_log_rebuild = bool(reason)

        if should_log_rebuild:
            print(f"Dashboard rebuild: started ({reason})")
        for attempt in range(3):
            try:
                with dashboard_lock:
                    refreshed_report_data = {}
                    relevant_limit = _report_max_relevant_positions(runtime_profile)
                    not_relevant_limit = _report_max_not_relevant_positions(runtime_profile)
                    category_totals: dict[str, int] = {}
                    for cat in ["relevant", "not relevant"]:
                        total_rows = get_jobs_count_by_category(
                            db_path,
                            cat,
                            unviewed_only=True,
                        )
                        category_totals[cat] = int(total_rows)
                        report_limit = relevant_limit if cat == "relevant" else not_relevant_limit
                        rows = get_jobs_by_category(
                            db_path,
                            cat,
                            limit=report_limit,
                            unviewed_only=True,
                        )
                        if should_log_rebuild:
                            print(
                                "Dashboard rebuild: collecting "
                                f"{cat} (showing={len(rows)}, total_unviewed={total_rows})"
                            )
                        refreshed_report_data[cat] = [
                            _build_dashboard_record(
                                row,
                                default_category=cat,
                                default_viewed=0,
                                default_applied=0,
                            )
                            for row in rows
                        ]

                    refreshed_applied_rows = get_applied_jobs(db_path, limit=0)
                    if should_log_rebuild:
                        print(
                            f"Dashboard rebuild: collecting applied ({len(refreshed_applied_rows)} rows)"
                        )
                    refreshed_applied_records = [
                        _build_dashboard_record(
                            row,
                            default_category="relevant",
                            default_viewed=1,
                            default_applied=1,
                            translate_title=False,
                        )
                        for row in refreshed_applied_rows
                    ]

                    _render_html_dashboard(
                        refreshed_report_data.get("relevant", []),
                        refreshed_report_data.get("not relevant", []),
                        refreshed_applied_records,
                        dashboard_path,
                        "Positions Report",
                        viewed_total=get_viewed_jobs_count(db_path),
                        skills_items=_build_skills_tab_items(db_path, runtime_profile),
                        report_max_relevant_positions=_report_max_relevant_positions(runtime_profile),
                        report_max_not_relevant_positions=_report_max_not_relevant_positions(runtime_profile),
                        relevant_total_count=category_totals.get("relevant", 0),
                        not_relevant_total_count=category_totals.get("not relevant", 0),
                    )
                if should_log_rebuild and not reason.startswith("new record"):
                    print(f"Dashboard rebuild: done ({reason})")
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"Report regeneration failed ({reason or 'unknown'}): {exc}")
                else:
                    time.sleep(0.2)

    def _queue_dashboard_rebuild(reason: str = ""):
        reason_text = str(reason or "").strip() or "queued update"
        with rebuild_pending_lock:
            rebuild_pending_reasons.append(reason_text)
        rebuild_signal.set()

    def _dashboard_rebuild_worker():
        while True:
            rebuild_signal.wait()
            with rebuild_pending_lock:
                reasons = list(rebuild_pending_reasons)
                rebuild_pending_reasons.clear()
                rebuild_signal.clear()

            if not reasons:
                continue

            reason = reasons[-1]
            if len(reasons) > 1:
                reason = f"{reason} (+{len(reasons) - 1} queued)"
            _rebuild_dashboard(reason=reason)

    def _sync_inbox_in_background():
        try:
            docs = []
            if os.path.isdir(inbox_path):
                docs = email_parser.load_files(inbox_path)

            missing_descriptions = get_jobs_for_description_refresh(
                db_path, missing_only=True, limit=1
            )
            has_missing_descriptions = bool(missing_descriptions)

            if not docs and not has_missing_descriptions:
                print("Background sync: no documents in inbox and no missing descriptions, skipping")
                return

            llm_for_sync = (
                LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=cli_verbose) if model_path else None
            )
            if docs:
                print(f"Background sync started: files={len(docs)}")
            else:
                print("Background sync started: backfilling missing descriptions/skills")

            new_records = 0
            last_inserted_logged = -1

            def _on_new_record():
                nonlocal new_records
                new_records += 1

            def _on_progress(processed: int, inserted_new: int, skipped_existing: int):
                nonlocal last_inserted_logged
                if inserted_new != last_inserted_logged:
                    print(
                        f"Background sync progress: processed={processed}, inserted={inserted_new}, "
                        f"skipped_existing={skipped_existing}"
                    )
                    last_inserted_logged = inserted_new

            text_translation_cache: dict[str, str] = {}
            title_translation_cache: dict[str, str] = {}

            def _translate_job_entry_for_storage(entry: dict) -> dict:
                entry = dict(entry)
                entry["raw_text"] = _translate_text_to_english_if_needed(
                    str(entry.get("raw_text", "") or ""),
                    runtime_profile=runtime_profile,
                    translation_cache=text_translation_cache,
                )
                title_value = str(entry.get("title", "") or "")
                try:
                    title_english = _translate_title_to_english(
                        title_value,
                        runtime_profile=runtime_profile,
                        title_translation_cache=title_translation_cache,
                    )
                except Exception:
                    try:
                        title_english = _translate_text_to_english_if_needed(
                            title_value,
                            runtime_profile=runtime_profile,
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

            if docs:
                ingest_stats = ingest_docs_to_db(
                    db_path,
                    docs,
                    entry_transform=_translate_job_entry_for_storage,
                    on_new_record=_on_new_record,
                    on_progress=_on_progress,
                )
            else:
                ingest_stats = {
                    "processed": 0,
                    "inserted_new": 0,
                    "skipped_existing": 0,
                    "positions_by_file": [],
                }

            _print_ingest_file_stats(ingest_stats)
            _queue_dashboard_rebuild(reason=f"ingest processed={ingest_stats['processed']}")
            delete_stats = _delete_processed_inbox_files(ingest_stats, inbox_root=inbox_path)
            print(
                "Background sync inbox cleanup: "
                f"eligible={delete_stats.get('eligible', 0)}, "
                f"deleted={delete_stats.get('deleted', 0)}, "
                f"missing={delete_stats.get('missing', 0)}, "
                f"failed={delete_stats.get('failed', 0)}"
            )

            print("Background sync: scoring relevance...")
            total, relevant_count = apply_relevance(
                db_path, runtime_profile, prune_irrelevant=False
            )
            print(f"Background sync: relevance scored (total={total}, relevant={relevant_count})")

            relevant_rows = get_jobs_by_category(db_path, "relevant", limit=0, unviewed_only=True)
            not_relevant_rows = get_jobs_by_category(
                db_path, "not relevant", limit=0, unviewed_only=True
            )
            applied_rows = get_applied_jobs(db_path, limit=0)
            skill_rows = []
            seen_job_ids = set()
            for row in relevant_rows + not_relevant_rows + applied_rows:
                row_id = int(row.get("id", 0) or 0)
                if not row_id or row_id in seen_job_ids:
                    continue
                seen_job_ids.add(row_id)
                skill_rows.append(row)

            skills_updated = _populate_missing_dashboard_skills(
                skill_rows,
                llm=llm_for_sync,
                progress_label="Background sync: skills",
            )
            print(f"Background sync: missing skills populated ({skills_updated} jobs updated)")

            _queue_dashboard_rebuild(reason="relevance re-scored")

            desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
                db_path,
                llm=llm_for_sync,
                runtime_profile=runtime_profile,
                allow_empty=False,
                progress=True,
                progress_label="Background sync: descriptions",
            )
            if desc_updated > 0:
                _queue_dashboard_rebuild(reason=f"descriptions updated {desc_updated}")

            skill_learning = _learn_skill_patterns_from_positions(
                db_path,
                runtime_profile=runtime_profile,
                llm=llm_for_sync,
                progress=True,
                progress_label="Background sync: skill patterns",
            )
            if skill_learning.get("new_skill_patterns", 0) > 0:
                _queue_dashboard_rebuild(
                    reason=f"skill patterns learned {skill_learning.get('new_skill_patterns', 0)}"
                )
            print(
                "Background sync: skill pattern learning "
                f"(considered={skill_learning.get('considered_positions', 0)}, "
                f"new={skill_learning.get('new_skill_patterns', 0)}, "
                f"total={skill_learning.get('total_known_skill_patterns', 0)})"
            )

            print(
                f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
                f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
                f"total_jobs={total}, relevant={relevant_count}"
            )
        except Exception as exc:
            print(f"Background sync failed: {exc}")

    sync_thread = threading.Thread(
        target=_sync_inbox_in_background, name="spejder-inbox-sync", daemon=True
    )


    app_factory_kwargs = {
        "db_path": db_path,
        "profile_path": profile_path,
        "runtime_profile": runtime_profile,
        "model_path": model_path,
        "report_dir": report_dir,
        "get_title_translation_llm": _get_title_translation_llm,
        "persist_runtime_profile": _persist_runtime_profile,
        "reload_runtime_profile": _reload_runtime_profile,
        "queue_dashboard_rebuild": _queue_dashboard_rebuild,
        "cli_verbose": cli_verbose
    }

    if not no_open:
        def open_browser():
            time.sleep(1)
            report_url = f"http://{host}:{port}/report.html"
            opened = webbrowser.open(report_url, new=2)
            if opened:
                print(f"Opened in default browser: {report_url}")
        threading.Thread(target=open_browser, daemon=True).start()

    print("Serve GUI: starting startup tasks in background")
    threading.Thread(
        target=_dashboard_rebuild_worker,
        name="spejder-dashboard-rebuild-worker",
        daemon=True,
    ).start()
    threading.Thread(
        target=lambda: _queue_dashboard_rebuild(reason="startup snapshot"),
        name="spejder-startup-dashboard",
        daemon=True,
    ).start()
    sync_thread.start()

    try:
        from spejder.server import start_server
        start_server(host, port, app_factory_kwargs)
    except KeyboardInterrupt:
        print("Stopping server...")



