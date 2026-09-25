# spejder.workflows.gui_sync

**Purpose:**
Background inbox synchronization pipeline extracted from `gui.py`, preserving existing behavior while isolating sync-side orchestration from dashboard/server concerns.

**API:**
- `GuiSyncContext` (frozen dataclass carrying paths, runtime config, callbacks, optional `on_stage(stage_id, message)`, `sync_log_path`, and optional per-run `sync_log: SyncRunLogLike | None`)
- `InboxSyncResult` — `status` is `done`, `skipped`, or `failed`
- `run_inbox_sync(context: GuiSyncContext) -> InboxSyncResult`
- `InboxSyncRunner` — thread-safe runner; at most one sync at a time (`_running` claimed under lock before the worker thread is spawned); `trigger()` is used for both `serve_gui` startup sync and the dashboard **Sync inbox** button. Opens `{sync_log_path}` via `SyncRunLog`, `run_start(source="gui_sync")`, `replace(..., sync_log=log)`, wires `on_stage` into `run_inbox_sync`, then logs **rebuild** stage start/end around `DashboardRebuildQueue.wait_until_idle` after a successful or skipped run (skipped waits for in-flight rebuilds so startup snapshot does not race the status message). If rebuild wait times out after a successful sync, terminal `status` stays `complete` but `message` notes rebuild may still be in progress. Always `run_end` in `finally` if not already closed.

**Sync log dual-write:**
`_emit_stage` still calls `on_stage` unchanged. When `context.sync_log` is set: terminal ids `done` / `failed` / `skipped` → `pipeline_end`; otherwise → `stage_start` (auto-ends previous stage). Progress callbacks during skills / descriptions / patterns / ingest call `sync_log.progress`; the log mirrors those events to the console (`sync …`, no `ts=`). Ingest progress uses **job** counts (`checked`) with `total=0` (no pct) and a static `files=` metric via `IngestProgressTracker`: ticks on `inserted_new` change, every 25 processed jobs, and a final line when the last tick was not at the final count. Dropped console banners: `Background sync started:…` and per-ingest `Background sync progress:…` (covered by stage/progress events). Kept: skip reasons (`portal_found=` / `portal_sync=disabled`), per-file ingest stats, cleanup/blocked/stale/bad-cloud/pattern-learning **summary** counts, `Background sync: missing skills populated…`, and final `Background sync done:…` / failure lines. Sync pipelines pass empty `progress_label` / `progress=False` into enrichment when `on_progress` is wired. Frozen base context holds `sync_log_path` only; the live `sync_log` handle (`SyncRunLogLike | None`) is attached on the per-run `replace`d context. `SyncRunLog.open` may return `_NullSyncRunLog` (empty path or open failure) — **silent sinks** (no file, no `sync …` mirror); enrichment still gets `on_progress` + `progress=False`, so operators lose tick progress until the path is writable (one stderr warning on open failure; see `sync_log.md`).

**Pipeline (11 steps):**
0. Sync IT-DAY job portal listings (`sync_itday_portal`) when `runtime_profile.itday_portal_sync_enabled` (default true); emit stage `"portal"` / `"Checking IT-DAY job portal"` only when enabled; otherwise skip fetch/ingest with zeros (no portal stage)
0b. When portal `inserted_new > 0`, run company+title dedupe immediately (`run_cross_source_dedupe`, stage `"portal_dedupe"`) so portal rows that already exist from LinkedIn/Jobindex are merged before inbox ingest; keep the later post-ingest dedupe for new inbox overlaps
1. Ingest inbox input (or detect missing-description backfill mode)
2. Delete processed inbox files
3. Run company+title position deduplication (`merge_duplicate_positions`)
4. Materialize skills for active-rescore scope jobs (`get_jobs_for_active_rescore`); conditional per-job rescore when skills changed; dashboard rebuild only when `skills_updated > 0`
5. Generate missing descriptions; dashboard rebuild when descriptions updated
6. Learn skill patterns from applied/relevant positions; dashboard rebuild when new patterns added
7–8. Shared skill hygiene via `run_skill_hygiene_stages` (`skill_hygiene.py`) — **same stage order and DB/profile side effects as CLI `process-inbox`**:
   - 7. Clean blocked skills from SQLite (`cleanup_blocked_skills_from_db` on `runtime_profile.blocked_skills`); rescore affected jobs (`rescore_jobs_if_active`); dashboard rebuild when links/patterns deleted or jobs rescored (deferred hygiene — does not block earlier enrichment)
   - 7b. Stale low-share skill cleanup (`run_stale_skill_cleanup`): after blocked cleanup, delete unflagged DB skills older than retention (same rule as job retention) with Job share < 0.1%; skips Skills-tab flags and remaining `blocked_skills` keys; rescore affected jobs; dashboard rebuild when rows/links deleted or jobs rescored (does **not** append `blocked_skills` or touch bad cloud)
   - 8. Emit stage `"bad_cloud"` then initialize bad cloud (`ensure_bad_cloud_initialized`): one-time seed from `blocked_skills`, prune redundant blocked entries outside the seeded batch; then **always** recalibrate `skill_bigram_toxicity_threshold` via `recalibrate_and_store_threshold` (mature non-blocked DB skills vs blocked list)
   - Single `save_profile` + `reload_runtime_profile` when `SkillHygieneResult.profile_dirty` (stale profile prune and/or seed/prune/threshold change); dashboard rebuild on prune or threshold change

**Removed from pipeline:** full-DB `apply_relevance` on every sync; early dashboard rebuild after ingest/dedupe.

**Constraints:**
- No wildcard imports (`import *`) anywhere under `spejder/`; keep imports explicit.
- Keep invocation-scoped mutable caches (`text_translation_cache`, `title_translation_cache`) inside `run_inbox_sync`; do not promote to module globals.
- Build ingest translation transform via `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage` to keep GUI sync and inbox ingest logic aligned.
- Use `spejder.workflows.ingest_utils` for ingest per-file stats logging and inbox cleanup.
- Pass `llm` + `runtime_profile` into `ingest_docs_to_db` so opt-in career-alert synthesis can run on `found=0` during background sync.
- Call `sync_itday_portal(..., enabled=runtime_profile.itday_portal_sync_enabled)` on every sync before inbox ingest; skip the pipeline only when inbox is empty, descriptions are complete, and the portal inserted no new jobs (skip means **no new portal rows**, not an empty portal listing — existing listings can still yield `found>0` with `inserted_new=0`; disabled portal also yields `inserted_new=0` with `skipped_disabled=True`). The skip log includes `portal_found` when the portal ran, or `portal_sync=disabled` when it was skipped via the profile flag.
- Treat `GuiSyncContext` callbacks as the only bridge back into GUI orchestration.
- Sync event format/I/O lives in `spejder.workflows.sync_log`; do not inline line formatting here.
