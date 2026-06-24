import os
import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Callable, Optional, Protocol

from spejder.config import AppConfig
from spejder.core import save_profile
from spejder.db import (
    cleanup_blocked_skills_from_db,
    ensure_db,
    get_jobs_for_active_rescore,
    get_jobs_for_description_refresh,
)
from spejder.extractors.skill_extractor import _learn_skill_patterns_from_positions
from spejder.extractors.skill_extractor.bad_cloud import ensure_bad_cloud_initialized
from spejder.jobs import ingest_docs_to_db, rescore_jobs_if_active
from spejder.llm import LocalLLM
from spejder.parsers import email_parser
from spejder.workflows.dashboard import DashboardRebuildQueue
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
class InboxSyncResult:
    status: str
    message: str = ""
    error: str = ""


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
    on_stage: Optional[Callable[[str, str], None]] = None


def _emit_stage(context: GuiSyncContext, stage_id: str, message: str) -> None:
    if context.on_stage is not None:
        context.on_stage(stage_id, message)


def run_inbox_sync(context: GuiSyncContext) -> InboxSyncResult:
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
            _emit_stage(context, "skipped", "Nothing to sync")
            return InboxSyncResult(status="skipped")

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
            _emit_stage(context, "ingest", f"Ingesting {len(docs)} inbox file(s)")
        else:
            print("Background sync started: backfilling missing descriptions/skills")
            _emit_stage(context, "ingest", "Backfilling missing descriptions and skills")

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
        _emit_stage(context, "cleanup", "Cleaning up processed inbox files")
        delete_stats = delete_processed_inbox_files(ingest_stats, inbox_root=context.inbox_path)
        print(
            "Background sync inbox cleanup: "
            f"eligible={delete_stats.get('eligible', 0)}, "
            f"deleted={delete_stats.get('deleted', 0)}, "
            f"missing={delete_stats.get('missing', 0)}, "
            f"failed={delete_stats.get('failed', 0)}"
        )

        _emit_stage(context, "dedupe", "Deduplicating positions")
        dedupe_result = {"groups_merged": 0, "rows_updated": 0, "rows_deleted": 0}
        try:
            dedupe_result = run_cross_source_dedupe(
                context.db_path,
                log_prefix="Background sync: cross-source dedupe",
            )
        except Exception as exc:
            print(f"Background sync: cross-source dedupe failed: {exc}")

        blocked_rescored = 0

        _emit_stage(context, "skills", "Materializing skills and rescoring jobs")
        skill_rows = get_jobs_for_active_rescore(context.db_path)
        skills_updated = context.populate_missing_dashboard_skills(
            skill_rows,
            llm=llm_for_sync,
            progress_label="Background sync: skills",
        )
        print(f"Background sync: missing skills populated ({skills_updated} jobs updated)")

        if skills_updated > 0:
            context.queue_dashboard_rebuild(reason=f"skills materialized={skills_updated}")

        _emit_stage(context, "descriptions", "Generating missing descriptions")
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

        _emit_stage(context, "patterns", "Learning skill patterns")
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

        _emit_stage(context, "blocked_skills", "Cleaning blocked skills from database")
        blocked_cleanup = cleanup_blocked_skills_from_db(
            context.db_path,
            list(context.runtime_profile.blocked_skills or []),
        )
        print(
            "Background sync: blocked-skills cleanup "
            f"(processed={blocked_cleanup.get('skills_processed', 0)}, "
            f"links_deleted={blocked_cleanup.get('job_skill_links_deleted', 0)}, "
            f"patterns_deleted={blocked_cleanup.get('skill_rows_deleted', 0)}, "
            f"affected_jobs={len(blocked_cleanup.get('affected_job_ids', []))})"
        )

        blocked_rescored = rescore_jobs_if_active(
            context.db_path,
            context.runtime_profile,
            list(blocked_cleanup.get("affected_job_ids", [])),
        )
        if blocked_rescored:
            print(f"Background sync: rescored blocked-skill jobs ({blocked_rescored})")

        if (
            blocked_rescored
            or int(blocked_cleanup.get("job_skill_links_deleted", 0) or 0) > 0
            or int(blocked_cleanup.get("skill_rows_deleted", 0) or 0) > 0
        ):
            context.queue_dashboard_rebuild(
                reason=(
                    f"blocked-skills cleanup "
                    f"(rescored={blocked_rescored}, "
                    f"links_deleted={blocked_cleanup.get('job_skill_links_deleted', 0)}, "
                    f"patterns_deleted={blocked_cleanup.get('skill_rows_deleted', 0)})"
                )
            )

        ensure_db(context.db_path)
        cloud_stats = ensure_bad_cloud_initialized(
            context.runtime_profile,
            context.db_path,
        )
        if cloud_stats.get("seeded") or cloud_stats.get("pruned"):
            save_profile(context.runtime_profile, context.profile_path)
            context.reload_runtime_profile()
            print(
                "Background sync: bad cloud initialized "
                f"(seeded={cloud_stats.get('seeded')}, "
                f"ngram_keys={cloud_stats.get('ngram_keys_upserted', 0)}, "
                f"threshold={cloud_stats.get('threshold')}, "
                f"pruned={len(cloud_stats.get('pruned', []))})"
            )
            if cloud_stats.get("pruned"):
                context.queue_dashboard_rebuild(reason="bad cloud prune")

        print(
            f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
            f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
            f"skills_updated={skills_updated}, blocked_rescored={blocked_rescored}"
        )
        _emit_stage(context, "done", "Inbox sync pipeline complete")
        return InboxSyncResult(status="done")
    except Exception as exc:
        print(f"Background sync failed: {exc}")
        _emit_stage(context, "failed", str(exc))
        return InboxSyncResult(status="failed", error=str(exc))


_SYNC_COMPLETE_MESSAGE = "Sync complete — reload the page to see new positions"
_SYNC_SKIPPED_MESSAGE = "Nothing to sync — inbox is empty and descriptions are up to date"
_SYNC_REBUILD_TIMEOUT_MESSAGE = (
    "Sync finished — dashboard rebuild may still be in progress; reload when ready"
)


class InboxSyncRunner:
    def __init__(
        self,
        context: GuiSyncContext,
        rebuild_queue: DashboardRebuildQueue,
    ) -> None:
        self._base_context = context
        self._rebuild_queue = rebuild_queue
        self._lock = threading.Lock()
        self._running = False
        self._stage_id = ""
        self._stage_message = ""
        self._terminal_status = "idle"
        self._terminal_message = ""

    def trigger(self) -> dict:
        with self._lock:
            if not self._claim_sync_locked():
                return {"ok": False, "error": "sync already running"}
        self._start_sync_thread()
        return {"ok": True, "started": True}

    def get_status(self) -> dict:
        with self._lock:
            if self._running:
                return {
                    "running": True,
                    "stage_id": self._stage_id,
                    "stage_message": self._stage_message,
                    "status": "running",
                    "message": self._stage_message,
                }
            return {
                "running": False,
                "stage_id": "",
                "stage_message": "",
                "status": self._terminal_status,
                "message": self._terminal_message,
            }

    def _claim_sync_locked(self) -> bool:
        if self._running:
            return False
        self._running = True
        self._terminal_status = "idle"
        self._terminal_message = ""
        self._stage_id = "start"
        self._stage_message = "Sync started"
        return True

    def _start_sync_thread(self) -> None:
        threading.Thread(
            target=self._run_sync,
            name="spejder-inbox-sync",
            daemon=True,
        ).start()

    def _run_sync(self) -> None:
        terminal_status = "failed"
        terminal_message = "Sync failed"

        def on_stage(stage_id: str, message: str) -> None:
            with self._lock:
                self._stage_id = stage_id
                self._stage_message = message

        context = replace(self._base_context, on_stage=on_stage)

        try:
            result = run_inbox_sync(context)
            if result.status == "done":
                on_stage("rebuild", "Waiting for dashboard rebuild")
                rebuild_ready = self._rebuild_queue.wait_until_idle(timeout=600)
                if rebuild_ready:
                    terminal_status = "complete"
                    terminal_message = _SYNC_COMPLETE_MESSAGE
                else:
                    terminal_status = "complete"
                    terminal_message = _SYNC_REBUILD_TIMEOUT_MESSAGE
            elif result.status == "skipped":
                on_stage("rebuild", "Waiting for dashboard rebuild")
                self._rebuild_queue.wait_until_idle(timeout=600)
                terminal_status = "skipped"
                terminal_message = _SYNC_SKIPPED_MESSAGE
            else:
                terminal_status = "failed"
                terminal_message = result.error or "Sync failed"
        except Exception as exc:
            terminal_status = "failed"
            terminal_message = str(exc)
        finally:
            with self._lock:
                self._running = False
                self._terminal_status = terminal_status
                self._terminal_message = terminal_message
