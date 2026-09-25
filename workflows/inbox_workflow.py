from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile, save_profile
from spejder.db import ensure_db, get_jobs_for_description_refresh, get_relevant_jobs
from spejder.extractors.skill_extractor import (
    _ensure_skill_pattern_seed_migration,
    _learn_skill_patterns_from_positions,
)
from spejder.jobs import ingest_docs_to_db, update_profile_from_db_signals
from spejder.llm import LocalLLM
from spejder.parsers import email_parser
from spejder.workflows.ingest_utils import (
    delete_processed_inbox_files,
    print_ingest_file_stats,
)
from spejder.workflows.inbox_report import (
    summarize_relevant_jobs_for_inbox,
    write_inbox_dashboard_report,
)
from spejder.workflows.job_enrichment import (
    _generate_missing_descriptions_for_ingest,
    make_translate_job_entry_for_storage,
    materialize_relevant_and_applied_skills,
)
from spejder.workflows.portal_sync import sync_itday_portal
from spejder.workflows.deduplication import run_cross_source_dedupe
from spejder.workflows.skill_hygiene import run_skill_hygiene_stages
from spejder.workflows.sync_log import (
    IngestProgressTracker,
    SyncRunLog,
    default_sync_log_path,
)


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

    sync_log = SyncRunLog.open(default_sync_log_path(report_dir))
    sync_log.run_start(source="process_inbox")
    run_status = "failed"
    run_message = ""

    try:
        docs = email_parser.load_files(inbox)

        ensure_db(db_path)
        _ensure_skill_pattern_seed_migration(db_path, profile_path)
        text_translation_cache: dict[str, str] = {}

        title_translation_cache: dict[str, str] = {}
        entry_transform = make_translate_job_entry_for_storage(
            profile, text_translation_cache, title_translation_cache
        )
        portal_enabled = profile.itday_portal_sync_enabled
        if portal_enabled:
            sync_log.stage_start("portal", "Checking IT-DAY job portal")
        portal_stats = sync_itday_portal(
            db_path,
            entry_transform=entry_transform,
            enabled=portal_enabled,
        )
        if int(portal_stats.get("inserted_new", 0) or 0) > 0:
            sync_log.stage_start("portal_dedupe", "Deduplicating portal positions")
            run_cross_source_dedupe(
                db_path,
                log_prefix="process-inbox: post-portal dedupe",
            )
        missing_descriptions = get_jobs_for_description_refresh(
            db_path, missing_only=True, limit=1
        )
        has_missing_descriptions = bool(missing_descriptions)
        if (
            not docs
            and int(portal_stats.get("inserted_new", 0) or 0) == 0
            and not has_missing_descriptions
        ):
            print("No documents found in inbox:", inbox)
            sync_log.pipeline_end(status="skipped", message="Nothing to process")
            run_status = "skipped"
            return

        llm = LocalLLM(model_path=model_path, n_ctx=int(profile.n_ctx), verbose=bool(verbose)) if model_path else None
        if not llm:
            raise SystemExit("Model init: model is required for process-inbox")

        sync_log.stage_start("ingest", f"Ingesting {len(docs)} inbox file(s)")
        ingest_file_count = len(docs)
        ingest_progress = IngestProgressTracker()

        def _emit_ingest_progress(
            processed: int, inserted_new: int, skipped_existing: int
        ) -> None:
            sync_log.progress(
                "ingest",
                checked=processed,
                total=0,
                inserted=inserted_new,
                skipped_existing=skipped_existing,
                files=ingest_file_count,
            )

        def _on_ingest_progress(processed: int, inserted_new: int, skipped_existing: int):
            # Job counts (not files); total=0 skips pct. Tick on insert change or milestone.
            if not ingest_progress.note(processed, inserted_new):
                return
            _emit_ingest_progress(processed, inserted_new, skipped_existing)

        ingest_stats = ingest_docs_to_db(
            db_path,
            docs,
            entry_transform=entry_transform,
            runtime_profile=profile,
            llm=llm,
            on_progress=_on_ingest_progress if docs else None,
        )
        final_processed = int(ingest_stats.get("processed", 0) or 0)
        if docs and ingest_progress.needs_final(final_processed):
            _emit_ingest_progress(
                final_processed,
                int(ingest_stats.get("inserted_new", 0) or 0),
                int(ingest_stats.get("skipped_existing", 0) or 0),
            )
            ingest_progress.last_processed = final_processed
        print(
            "Ingestion done: "
            f"processed={ingest_stats.get('processed', 0)}, "
            f"inserted_new={ingest_stats.get('inserted_new', 0)}, "
            f"skipped_existing={ingest_stats.get('skipped_existing', 0)} "
            f"into DB: {db_path}"
        )
        print_ingest_file_stats(ingest_stats)
        sync_log.stage_start("cleanup", "Cleaning up processed inbox files")
        delete_stats = delete_processed_inbox_files(ingest_stats, inbox_root=inbox)
        print(
            "Inbox cleanup: "
            f"eligible={delete_stats.get('eligible', 0)}, "
            f"deleted={delete_stats.get('deleted', 0)}, "
            f"missing={delete_stats.get('missing', 0)}, "
            f"failed={delete_stats.get('failed', 0)}"
        )

        sync_log.stage_start("descriptions", "Generating missing descriptions")

        def _on_desc_progress(checked: int, total: int, updated: int) -> None:
            sync_log.progress(
                "descriptions", checked=checked, total=total, updated=updated
            )

        desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
            db_path,
            llm=llm,
            runtime_profile=profile,
            allow_empty=False,
            on_progress=_on_desc_progress,
        )
        print(f"Descriptions generated during ingest: updated={desc_updated}, skipped={desc_skipped}")

        sync_log.stage_start("skills", "Materializing skills and rescoring jobs")

        def _on_skills_progress(checked: int, total: int, updated: int) -> None:
            sync_log.progress(
                "skills", checked=checked, total=total, updated=updated
            )

        materialize_relevant_and_applied_skills(
            db_path,
            llm=llm,
            runtime_profile=profile,
            rescore=True,
            skip_cached=True,
            progress_label="",
            on_progress=_on_skills_progress,
        )

        relevant_jobs = get_relevant_jobs(db_path, limit=limit)
        print(f"Relevant after skill scoring: {len(relevant_jobs)}")

        sync_log.stage_start("patterns", "Learning skill patterns")

        def _on_patterns_progress(checked: int, total: int) -> None:
            sync_log.progress("patterns", checked=checked, total=total)

        skill_learning = _learn_skill_patterns_from_positions(
            db_path,
            runtime_profile=profile,
            llm=llm,
            progress=False,
            on_progress=_on_patterns_progress,
        )
        print(
            "Skill pattern learning: "
            f"considered={skill_learning.get('considered_positions', 0)}, "
            f"new_patterns={skill_learning.get('new_skill_patterns', 0)}, "
            f"total_patterns={skill_learning.get('total_known_skill_patterns', 0)}"
        )

        hygiene = run_skill_hygiene_stages(
            db_path,
            profile,
            on_stage=sync_log.stage_start,
        )
        blocked_cleanup = hygiene.blocked_cleanup
        print(
            "process-inbox: blocked-skills cleanup "
            f"(processed={blocked_cleanup.get('skills_processed', 0)}, "
            f"links_deleted={blocked_cleanup.get('job_skill_links_deleted', 0)}, "
            f"patterns_deleted={blocked_cleanup.get('skill_rows_deleted', 0)}, "
            f"affected_jobs={len(blocked_cleanup.get('affected_job_ids', []))})"
        )
        if hygiene.blocked_rescored:
            print(
                f"process-inbox: rescored blocked-skill jobs ({hygiene.blocked_rescored})"
            )

        stale_cleanup = hygiene.stale_cleanup
        print(
            "process-inbox: stale-skills cleanup "
            f"(deleted={stale_cleanup.get('skills_deleted', 0)}, "
            f"links_deleted={stale_cleanup.get('job_skill_links_deleted', 0)}, "
            f"patterns_deleted={stale_cleanup.get('skill_rows_deleted', 0)}, "
            f"affected_jobs={len(stale_cleanup.get('affected_job_ids', []))})"
        )
        if hygiene.stale_rescored:
            print(f"process-inbox: rescored stale-skill jobs ({hygiene.stale_rescored})")

        print(
            "process-inbox: bad-cloud threshold recalibrated "
            f"(threshold={hygiene.new_threshold}, changed={hygiene.threshold_changed})"
        )
        cloud_stats = hygiene.cloud_stats
        if cloud_stats.get("seeded") or cloud_stats.get("pruned"):
            print(
                "process-inbox: bad cloud initialized "
                f"(seeded={cloud_stats.get('seeded')}, "
                f"ngram_keys={cloud_stats.get('ngram_keys_upserted', 0)}, "
                f"pruned={len(cloud_stats.get('pruned', []))})"
            )
        if hygiene.profile_dirty:
            save_profile(profile, profile_path)

        learning_info = update_profile_from_db_signals(db_path, profile_path)
        print(
            "Profile learning: "
            f"labeled={learning_info.get('labeled_count', 0)}, "
            f"include={learning_info.get('learned_include_count', 0)}, "
            f"exclude={learning_info.get('learned_exclude_count', 0)}, "
            f"missing_skills={learning_info.get('missing_skills_count', 0)}"
        )

        summarize_relevant_jobs_for_inbox(
            db_path,
            relevant_jobs,
            llm,
            max_tokens=max_tokens,
            max_input_chars=max_input_chars,
        )

        write_inbox_dashboard_report(db_path, profile, llm, report_dir)

        if not relevant_jobs:
            print("No relevant positions after filtering.")

        print(f"Done. Relevant summarized={len(relevant_jobs)}")
        sync_log.pipeline_end(status="done", message="process-inbox complete")
        run_status = "complete"
    except SystemExit as exc:
        run_status = "failed"
        run_message = str(exc)
        sync_log.pipeline_end(status="failed", message=run_message)
        raise
    except Exception as exc:
        run_status = "failed"
        run_message = str(exc)
        sync_log.pipeline_end(status="failed", message=run_message)
        raise
    finally:
        sync_log.run_end(status=run_status, message=run_message)
