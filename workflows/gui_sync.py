import os
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional, Protocol

from spejder.config import AppConfig
from spejder.core import load_runtime_profile
from spejder.db import get_all_applied_jobs, get_jobs_by_category, get_jobs_for_description_refresh
from spejder.extractors.skill_extractor import (
    _learn_skill_patterns_from_positions,
    should_sync_skill_antipatterns,
    sync_skill_extraction_antipatterns,
)
from spejder.jobs import apply_relevance, ingest_docs_to_db
from spejder.llm import LocalLLM
from spejder.parsers import email_parser
from spejder.workflows.deduplication import run_cross_source_dedupe
from spejder.workflows.ingest_utils import (
    delete_processed_inbox_files,
    print_ingest_file_stats,
)
from spejder.workflows.job_enrichment import (
    _generate_missing_descriptions_for_ingest,
    make_translate_job_entry_for_storage,
)

if TYPE_CHECKING:
    from spejder.llm import LocalLLM


class _PopulateSkillsFn(Protocol):
    def __call__(
        self,
        rows: list[dict],
        *,
        llm: Optional["LocalLLM"] = None,
        progress_label: str = "",
    ) -> int: ...


class _QueueDashboardRebuildFn(Protocol):
    def __call__(self, *, reason: str = "") -> None: ...


@dataclass(frozen=True)
class GuiSyncContext:
    db_path: str
    inbox_path: str
    model_path: str
    profile_path: str
    runtime_profile: AppConfig
    cli_verbose: bool
    queue_dashboard_rebuild: _QueueDashboardRebuildFn
    reload_runtime_profile: Callable[[], None]
    populate_missing_dashboard_skills: _PopulateSkillsFn


def run_inbox_sync(context: GuiSyncContext) -> None:
    try:
        docs = []
        if os.path.isdir(context.inbox_path):
            docs = email_parser.load_files(context.inbox_path)

        missing_descriptions = get_jobs_for_description_refresh(
            context.db_path, missing_only=True, limit=1
        )
        has_missing_descriptions = bool(missing_descriptions)

        if not docs and not has_missing_descriptions:
            print("Background sync: no documents in inbox and no missing descriptions, skipping")
            return

        llm_for_sync = (
            __import__("spejder.llm", fromlist=["LocalLLM"]).LocalLLM(
                model_path=context.model_path,
                n_ctx=int(context.runtime_profile.n_ctx),
                verbose=context.cli_verbose,
            )
            if context.model_path
            else None
        )
        if docs:
            print(f"Background sync started: files={len(docs)}")
        else:
            print("Background sync started: backfilling missing descriptions/skills")

        last_inserted_logged = -1

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
        entry_transform = make_translate_job_entry_for_storage(
            context.runtime_profile,
            text_translation_cache,
            title_translation_cache,
        )

        if docs:
            ingest_stats = ingest_docs_to_db(
                context.db_path,
                docs,
                entry_transform=entry_transform,
                on_new_record=None,
                on_progress=_on_progress,
            )
        else:
            ingest_stats = {
                "processed": 0,
                "inserted_new": 0,
                "skipped_existing": 0,
                "positions_by_file": [],
            }

        print_ingest_file_stats(ingest_stats)
        delete_stats = delete_processed_inbox_files(ingest_stats, inbox_root=context.inbox_path)
        print(
            "Background sync inbox cleanup: "
            f"eligible={delete_stats.get('eligible', 0)}, "
            f"deleted={delete_stats.get('deleted', 0)}, "
            f"missing={delete_stats.get('missing', 0)}, "
            f"failed={delete_stats.get('failed', 0)}"
        )

        dedupe_result = {"groups_merged": 0, "rows_updated": 0, "rows_deleted": 0}
        try:
            dedupe_result = run_cross_source_dedupe(
                context.db_path,
                log_prefix="Background sync: cross-source dedupe",
            )
        except Exception as exc:
            print(f"Background sync: cross-source dedupe failed: {exc}")

        context.queue_dashboard_rebuild(
            reason=(
                f"ingest processed={ingest_stats['processed']}, "
                f"dedupe rows_deleted={dedupe_result.get('rows_deleted', 0)}"
            )
        )

        print("Background sync: scoring relevance...")
        total, relevant_count = apply_relevance(
            context.db_path, context.runtime_profile, prune_irrelevant=False
        )
        print(f"Background sync: relevance scored (total={total}, relevant={relevant_count})")

        relevant_rows = get_jobs_by_category(
            context.db_path, "relevant", limit=0, unviewed_only=True
        )
        not_relevant_rows = get_jobs_by_category(
            context.db_path, "not relevant", limit=0, unviewed_only=True
        )
        applied_rows = get_all_applied_jobs(context.db_path, limit=0)
        skill_rows = []
        seen_job_ids = set()
        for row in relevant_rows + not_relevant_rows + applied_rows:
            row_id = int(row.get("id", 0) or 0)
            if not row_id or row_id in seen_job_ids:
                continue
            seen_job_ids.add(row_id)
            skill_rows.append(row)

        skills_updated = context.populate_missing_dashboard_skills(
            skill_rows,
            llm=llm_for_sync,
            progress_label="Background sync: skills",
        )
        print(f"Background sync: missing skills populated ({skills_updated} jobs updated)")

        context.queue_dashboard_rebuild(reason="relevance re-scored")

        desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
            context.db_path,
            llm=llm_for_sync,
            runtime_profile=context.runtime_profile,
            allow_empty=False,
            progress=True,
            progress_label="Background sync: descriptions",
        )
        if desc_updated > 0:
            context.queue_dashboard_rebuild(reason=f"descriptions updated {desc_updated}")

        skill_learning = _learn_skill_patterns_from_positions(
            context.db_path,
            runtime_profile=context.runtime_profile,
            llm=llm_for_sync,
            progress=True,
            progress_label="Background sync: skill patterns",
        )
        if skill_learning.get("new_skill_patterns", 0) > 0:
            context.queue_dashboard_rebuild(
                reason=f"skill patterns learned {skill_learning.get('new_skill_patterns', 0)}"
            )
        print(
            "Background sync: skill pattern learning "
            f"(considered={skill_learning.get('considered_positions', 0)}, "
            f"new={skill_learning.get('new_skill_patterns', 0)}, "
            f"total={skill_learning.get('total_known_skill_patterns', 0)})"
        )

        if llm_for_sync and should_sync_skill_antipatterns(context.runtime_profile, llm_for_sync):

            def _run_antipattern_sync_worker():
                try:
                    fresh_profile = load_runtime_profile(context.profile_path)
                    worker_llm = LocalLLM(
                        model_path=context.model_path,
                        n_ctx=int(fresh_profile.n_ctx),
                        verbose=False,
                    )
                    stats = sync_skill_extraction_antipatterns(
                        context.db_path,
                        fresh_profile,
                        worker_llm,
                        profile_path=context.profile_path,
                    )
                    if stats.get("skipped"):
                        reason = stats.get("skip_reason") or "unknown"
                        print(
                            "Background sync: antipattern sync skipped "
                            f"(reason={reason}, blocked_input={stats.get('blocked_input', 0)}, "
                            f"synthesized={stats.get('synthesized', [])})."
                        )
                    else:
                        prune_parts = []
                        if stats.get("would_prune_blocked", 0):
                            prune_parts.append(
                                f"would_prune={stats.get('would_prune_blocked', 0)}"
                            )
                        if stats.get("pruned_blocked", 0):
                            prune_parts.append(
                                f"pruned={stats.get('pruned_blocked', 0)}"
                            )
                        prune_summary = ", ".join(prune_parts) if prune_parts else "pruned=0"
                        save_skipped = (
                            ", profile_save_skipped=True"
                            if stats.get("profile_save_skipped")
                            else ""
                        )
                        print(
                            "Background sync: antipattern sync "
                            f"(synthesized={stats.get('synthesized', [])}, "
                            f"candidates_accepted={stats.get('candidates_accepted', 0)}, "
                            f"candidates_skipped={stats.get('candidates_skipped', 0)}, "
                            f"merged={stats.get('merged', 0)}, "
                            f"{prune_summary}, "
                            f"db_deleted={stats.get('db_skill_rows_deleted', 0)}, "
                            f"committed={stats.get('committed', False)}"
                            f"{save_skipped})"
                        )
                    if stats.get("committed"):
                        context.reload_runtime_profile()
                        context.queue_dashboard_rebuild(reason="antipattern sync")
                except Exception as exc:
                    print(f"Background sync: antipattern sync failed: {exc}")

            threading.Thread(
                target=_run_antipattern_sync_worker,
                name="spejder-antipattern-sync",
                daemon=True,
            ).start()

        print(
            f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
            f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
            f"total_jobs={total}, relevant={relevant_count}"
        )
    except Exception as exc:
        print(f"Background sync failed: {exc}")
