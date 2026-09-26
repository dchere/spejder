import os
import threading
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Callable, Optional, Protocol

from spejder.config import AppConfig
from spejder.core import save_profile
from spejder.db import (
    ensure_db,
    get_jobs_for_active_rescore,
    get_jobs_for_description_refresh,
)
from spejder.extractors.skill_extractor import _learn_skill_patterns_from_positions
from spejder.jobs import ingest_docs_to_db
from spejder.llm import LocalLLM
from spejder.parsers import email_parser
from spejder.workflows.dashboard import DashboardRebuildQueue
from spejder.workflows.deduplication import run_cross_source_dedupe
from spejder.workflows.ingest_utils import (
    default_parse_quarantine_path,
    delete_processed_inbox_files,
    log_ingest_parse_outcomes,
    print_ingest_file_stats,
    quarantine_unparsed_inbox_files,
)
from spejder.workflows.job_enrichment import (
    _generate_missing_descriptions_for_ingest,
    make_translate_job_entry_for_storage,
)
from spejder.workflows.portal_sync import sync_itday_portal
from spejder.workflows.skill_hygiene import run_skill_hygiene_stages
from spejder.workflows.sync_log import IngestProgressTracker, SyncRunLog, SyncRunLogLike

if TYPE_CHECKING:
    from spejder.llm import LocalLLM


class _PopulateSkillsFn(Protocol):
    def __call__(
        self,
        rows: list[dict],
        *,
        llm: Optional["LocalLLM"] = None,
        progress_label: str = "",
        on_progress: Optional[Callable[[int, int, int], None]] = None,
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
    sync_log_path: str = ""
    sync_log: Optional[SyncRunLogLike] = None


def _emit_stage(context: GuiSyncContext, stage_id: str, message: str) -> None:
    if context.on_stage is not None:
        context.on_stage(stage_id, message)
    sync_log = context.sync_log
    if sync_log is None:
        return
    if stage_id in ("done", "failed", "skipped"):
        sync_log.pipeline_end(status=stage_id, message=message)
    else:
        sync_log.stage_start(stage_id, message)


def run_inbox_sync(context: GuiSyncContext) -> InboxSyncResult:
    try:
        docs = []
        if os.path.isdir(context.inbox_path):
            docs = email_parser.load_files(context.inbox_path)

        missing_descriptions = get_jobs_for_description_refresh(
            context.db_path, missing_only=True, limit=1
        )
        has_missing_descriptions = bool(missing_descriptions)

        text_translation_cache: dict[str, str] = {}
        title_translation_cache: dict[str, str] = {}
        entry_transform = make_translate_job_entry_for_storage(
            context.runtime_profile,
            text_translation_cache,
            title_translation_cache,
        )

        portal_enabled = context.runtime_profile.itday_portal_sync_enabled
        if portal_enabled:
            _emit_stage(context, "portal", "Checking IT-DAY job portal")
        portal_stats = sync_itday_portal(
            context.db_path,
            entry_transform=entry_transform,
            enabled=portal_enabled,
        )
        if int(portal_stats.get("inserted_new", 0) or 0) > 0:
            _emit_stage(context, "portal_dedupe", "Deduplicating portal positions")
            try:
                run_cross_source_dedupe(
                    context.db_path,
                    log_prefix="Background sync: post-portal dedupe",
                )
            except Exception as exc:
                print(f"Background sync: post-portal dedupe failed: {exc}")

        if (
            not docs
            and not has_missing_descriptions
            and int(portal_stats.get("inserted_new", 0) or 0) == 0
        ):
            if portal_stats.get("skipped_disabled"):
                print(
                    "Background sync: no documents in inbox, no missing descriptions, "
                    "and IT-DAY portal sync disabled (portal_sync=disabled), skipping"
                )
            else:
                print(
                    "Background sync: no documents in inbox, no missing descriptions, "
                    "and no new IT-DAY portal positions "
                    f"(portal_found={portal_stats.get('found', 0)}), skipping"
                )
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
            _emit_stage(context, "ingest", f"Ingesting {len(docs)} inbox file(s)")
        else:
            _emit_stage(context, "ingest", "Backfilling missing descriptions and skills")

        ingest_file_count = len(docs)
        ingest_progress = IngestProgressTracker()

        def _emit_ingest_progress(
            processed: int,
            inserted_new: int,
            skipped_existing: int,
        ) -> None:
            if context.sync_log is not None:
                context.sync_log.progress(
                    "ingest",
                    checked=processed,
                    total=0,
                    inserted=inserted_new,
                    skipped_existing=skipped_existing,
                    files=ingest_file_count,
                )

        def _on_progress(processed: int, inserted_new: int, skipped_existing: int):
            # Job counts (not files); total=0 skips pct. Emit on insert/milestone;
            # console mirrors via sync_log.
            if not ingest_progress.note(processed, inserted_new):
                return
            _emit_ingest_progress(processed, inserted_new, skipped_existing)

        if docs:
            ingest_stats = ingest_docs_to_db(
                context.db_path,
                docs,
                entry_transform=entry_transform,
                on_new_record=None,
                on_progress=_on_progress,
                llm=llm_for_sync,
                runtime_profile=context.runtime_profile,
            )
        else:
            ingest_stats = {
                "processed": 0,
                "inserted_new": 0,
                "skipped_existing": 0,
                "positions_by_file": [],
            }

        final_processed = int(ingest_stats.get("processed", 0) or 0)
        if docs and ingest_progress.needs_final(final_processed):
            _emit_ingest_progress(
                final_processed,
                int(ingest_stats.get("inserted_new", 0) or 0),
                int(ingest_stats.get("skipped_existing", 0) or 0),
            )
            ingest_progress.last_processed = final_processed

        print_ingest_file_stats(ingest_stats)
        logged = log_ingest_parse_outcomes(context.sync_log, ingest_stats)
        if logged:
            print(f"Background sync parse outcomes logged: {logged} non-ok file(s)")
        _emit_stage(context, "cleanup", "Cleaning up processed inbox files")
        delete_stats = delete_processed_inbox_files(ingest_stats, inbox_root=context.inbox_path)
        print(
            "Background sync inbox cleanup: "
            f"eligible={delete_stats.get('eligible', 0)}, "
            f"deleted={delete_stats.get('deleted', 0)}, "
            f"missing={delete_stats.get('missing', 0)}, "
            f"failed={delete_stats.get('failed', 0)}"
        )
        if context.sync_log_path:
            quarantine_dir = default_parse_quarantine_path(
                os.path.dirname(os.path.abspath(context.sync_log_path))
            )
        else:
            quarantine_dir = default_parse_quarantine_path(
                context.runtime_profile.default_report_dir or "./outbox"
            )
        quarantine_stats = quarantine_unparsed_inbox_files(
            ingest_stats,
            inbox_root=context.inbox_path,
            quarantine_dir=quarantine_dir,
        )
        print(
            "Background sync parse quarantine: "
            f"eligible={quarantine_stats.get('eligible', 0)}, "
            f"moved={quarantine_stats.get('moved', 0)}, "
            f"missing={quarantine_stats.get('missing', 0)}, "
            f"failed={quarantine_stats.get('failed', 0)} "
            f"dir={quarantine_dir}"
        )
        if quarantine_stats.get("moved", 0) and context.sync_log is not None:
            context.sync_log.note(
                "parse_quarantine",
                stage="cleanup",
                moved=int(quarantine_stats.get("moved", 0) or 0),
                eligible=int(quarantine_stats.get("eligible", 0) or 0),
                dir=quarantine_dir,
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

        def _on_skills_progress(checked: int, total: int, updated: int) -> None:
            if context.sync_log is not None:
                context.sync_log.progress(
                    "skills", checked=checked, total=total, updated=updated
                )

        skills_updated = context.populate_missing_dashboard_skills(
            skill_rows,
            llm=llm_for_sync,
            progress_label="",
            on_progress=_on_skills_progress if context.sync_log is not None else None,
        )
        print(f"Background sync: missing skills populated ({skills_updated} jobs updated)")

        if skills_updated > 0:
            context.queue_dashboard_rebuild(reason=f"skills materialized={skills_updated}")

        _emit_stage(context, "descriptions", "Generating missing descriptions")

        def _on_desc_progress(checked: int, total: int, updated: int) -> None:
            if context.sync_log is not None:
                context.sync_log.progress(
                    "descriptions", checked=checked, total=total, updated=updated
                )

        desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
            context.db_path,
            llm=llm_for_sync,
            runtime_profile=context.runtime_profile,
            allow_empty=False,
            progress=False,
            on_progress=_on_desc_progress if context.sync_log is not None else None,
        )
        if desc_updated > 0:
            context.queue_dashboard_rebuild(reason=f"descriptions updated {desc_updated}")

        _emit_stage(context, "patterns", "Learning skill patterns")

        def _on_patterns_progress(checked: int, total: int) -> None:
            if context.sync_log is not None:
                context.sync_log.progress("patterns", checked=checked, total=total)

        skill_learning = _learn_skill_patterns_from_positions(
            context.db_path,
            runtime_profile=context.runtime_profile,
            llm=llm_for_sync,
            progress=False,
            on_progress=_on_patterns_progress if context.sync_log is not None else None,
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

        hygiene = run_skill_hygiene_stages(
            context.db_path,
            context.runtime_profile,
            on_stage=lambda stage_id, message: _emit_stage(context, stage_id, message),
        )
        blocked_cleanup = hygiene.blocked_cleanup
        blocked_rescored = hygiene.blocked_rescored
        print(
            "Background sync: blocked-skills cleanup "
            f"(processed={blocked_cleanup.get('skills_processed', 0)}, "
            f"links_deleted={blocked_cleanup.get('job_skill_links_deleted', 0)}, "
            f"patterns_deleted={blocked_cleanup.get('skill_rows_deleted', 0)}, "
            f"affected_jobs={len(blocked_cleanup.get('affected_job_ids', []))})"
        )
        if blocked_rescored:
            print(f"Background sync: rescored blocked-skill jobs ({blocked_rescored})")
        if hygiene.blocked_needs_rebuild():
            context.queue_dashboard_rebuild(
                reason=(
                    f"blocked-skills cleanup "
                    f"(rescored={blocked_rescored}, "
                    f"links_deleted={blocked_cleanup.get('job_skill_links_deleted', 0)}, "
                    f"patterns_deleted={blocked_cleanup.get('skill_rows_deleted', 0)})"
                )
            )

        stale_cleanup = hygiene.stale_cleanup
        stale_rescored = hygiene.stale_rescored
        print(
            "Background sync: stale-skills cleanup "
            f"(deleted={stale_cleanup.get('skills_deleted', 0)}, "
            f"links_deleted={stale_cleanup.get('job_skill_links_deleted', 0)}, "
            f"patterns_deleted={stale_cleanup.get('skill_rows_deleted', 0)}, "
            f"affected_jobs={len(stale_cleanup.get('affected_job_ids', []))})"
        )
        if stale_rescored:
            print(f"Background sync: rescored stale-skill jobs ({stale_rescored})")
        if hygiene.stale_needs_rebuild():
            context.queue_dashboard_rebuild(
                reason=(
                    f"stale-skills cleanup "
                    f"(rescored={stale_rescored}, "
                    f"links_deleted={stale_cleanup.get('job_skill_links_deleted', 0)}, "
                    f"patterns_deleted={stale_cleanup.get('skill_rows_deleted', 0)})"
                )
            )

        cloud_stats = hygiene.cloud_stats
        print(
            "Background sync: bad-cloud threshold recalibrated "
            f"(threshold={hygiene.new_threshold}, changed={hygiene.threshold_changed})"
        )
        if hygiene.profile_dirty:
            save_profile(context.runtime_profile, context.profile_path)
            context.reload_runtime_profile()
            if cloud_stats.get("seeded") or cloud_stats.get("pruned"):
                print(
                    "Background sync: bad cloud initialized "
                    f"(seeded={cloud_stats.get('seeded')}, "
                    f"ngram_keys={cloud_stats.get('ngram_keys_upserted', 0)}, "
                    f"pruned={len(cloud_stats.get('pruned', []))})"
                )
            if cloud_stats.get("pruned"):
                context.queue_dashboard_rebuild(reason="bad cloud prune")
            elif hygiene.threshold_changed:
                context.queue_dashboard_rebuild(reason="bad cloud threshold recalibrated")

        print(
            f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
            f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
            f"portal_found={portal_stats.get('found', 0)}, portal_inserted={portal_stats.get('inserted_new', 0)}, "
            f"skills_updated={skills_updated}, blocked_rescored={blocked_rescored}"
        )
        _emit_stage(context, "done", "Inbox sync pipeline complete")
        return InboxSyncResult(status="done")
    except Exception as exc:
        print(f"Background sync failed: {exc}")
        _emit_stage(context, "failed", str(exc))
        return InboxSyncResult(status="failed", error=str(exc))


_SYNC_COMPLETE_MESSAGE = "Sync complete — reload the page to see new positions"
_SYNC_SKIPPED_MESSAGE = (
    "Nothing to sync — inbox is empty, descriptions are up to date, "
    "and IT-DAY portal has no new positions"
)
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
        sync_log = SyncRunLog.open(self._base_context.sync_log_path)
        run_ended = False

        def on_stage(stage_id: str, message: str) -> None:
            with self._lock:
                self._stage_id = stage_id
                self._stage_message = message

        context = replace(self._base_context, on_stage=on_stage, sync_log=sync_log)
        sync_log.run_start(source="gui_sync")

        try:
            result = run_inbox_sync(context)
            if result.status == "done":
                on_stage("rebuild", "Waiting for dashboard rebuild")
                sync_log.stage_start("rebuild", "Waiting for dashboard rebuild")
                rebuild_ready = self._rebuild_queue.wait_until_idle(timeout=600)
                sync_log.stage_end("rebuild")
                if rebuild_ready:
                    terminal_status = "complete"
                    terminal_message = _SYNC_COMPLETE_MESSAGE
                else:
                    terminal_status = "complete"
                    terminal_message = _SYNC_REBUILD_TIMEOUT_MESSAGE
            elif result.status == "skipped":
                on_stage("rebuild", "Waiting for dashboard rebuild")
                sync_log.stage_start("rebuild", "Waiting for dashboard rebuild")
                self._rebuild_queue.wait_until_idle(timeout=600)
                sync_log.stage_end("rebuild")
                terminal_status = "skipped"
                terminal_message = _SYNC_SKIPPED_MESSAGE
            else:
                terminal_status = "failed"
                terminal_message = result.error or "Sync failed"
            sync_log.run_end(status=terminal_status, message=terminal_message)
            run_ended = True
        except Exception as exc:
            terminal_status = "failed"
            terminal_message = str(exc)
        finally:
            if not run_ended:
                sync_log.run_end(status=terminal_status, message=terminal_message)
            with self._lock:
                self._running = False
                self._terminal_status = terminal_status
                self._terminal_message = terminal_message
