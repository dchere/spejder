import os

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import ensure_db, get_relevant_jobs
from spejder.extractors.skill_extractor import (
    _ensure_skill_pattern_seed_migration,
    _learn_skill_patterns_from_positions,
)
from spejder.jobs import apply_relevance, ingest_docs_to_db, update_profile_from_db_signals
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

    title_translation_cache: dict[str, str] = {}
    entry_transform = make_translate_job_entry_for_storage(
        profile, text_translation_cache, title_translation_cache
    )
    ingest_stats = ingest_docs_to_db(db_path, docs, entry_transform=entry_transform)
    print(
        "Ingestion done: "
        f"processed={ingest_stats.get('processed', 0)}, "
        f"inserted_new={ingest_stats.get('inserted_new', 0)}, "
        f"skipped_existing={ingest_stats.get('skipped_existing', 0)} "
        f"into DB: {db_path}"
    )
    print_ingest_file_stats(ingest_stats)
    delete_stats = delete_processed_inbox_files(ingest_stats, inbox_root=inbox)
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

    materialize_relevant_and_applied_skills(
        db_path,
        llm=llm,
        runtime_profile=profile,
        rescore=True,
        skip_cached=True,
        progress_label="Skill materialization",
    )

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


