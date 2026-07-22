# spejder.workflows.gui_sync

**Purpose:**
Background inbox synchronization pipeline extracted from `gui.py`, preserving existing behavior while isolating sync-side orchestration from dashboard/server concerns.

**API:**
- `GuiSyncContext` (frozen dataclass carrying paths, runtime config, callbacks, and optional `on_stage(stage_id, message)`)
- `InboxSyncResult` — `status` is `done`, `skipped`, or `failed`
- `run_inbox_sync(context: GuiSyncContext) -> InboxSyncResult`
- `InboxSyncRunner` — thread-safe runner; at most one sync at a time (`_running` claimed under lock before the worker thread is spawned); `trigger()` is used for both `serve_gui` startup sync and the dashboard **Sync inbox** button. Wires `on_stage` into `run_inbox_sync`, then `DashboardRebuildQueue.wait_until_idle` after a successful or skipped run (skipped waits for in-flight rebuilds so startup snapshot does not race the status message). If rebuild wait times out after a successful sync, terminal `status` stays `complete` but `message` notes rebuild may still be in progress.

**Pipeline (8 steps):**
1. Ingest inbox input (or detect missing-description backfill mode)
2. Delete processed inbox files
3. Run company+title position deduplication (`merge_duplicate_positions`)
4. Materialize skills for active-rescore scope jobs (`get_jobs_for_active_rescore`); conditional per-job rescore when skills changed; dashboard rebuild only when `skills_updated > 0`
5. Generate missing descriptions; dashboard rebuild when descriptions updated
6. Learn skill patterns from applied/relevant positions; dashboard rebuild when new patterns added
7. Clean blocked skills from SQLite (`cleanup_blocked_skills_from_db` on `runtime_profile.blocked_skills`); rescore affected jobs (`rescore_jobs_if_active`); dashboard rebuild when links/patterns deleted or jobs rescored (deferred hygiene — does not block earlier enrichment)
8. Initialize bad cloud (`ensure_bad_cloud_initialized`): one-time seed from `blocked_skills`, prune redundant blocked entries outside the seeded batch; then **always** recalibrate `skill_bigram_toxicity_threshold` via `recalibrate_and_store_threshold` (mature non-blocked DB skills vs blocked list). Save profile / reload / rebuild when seed, prune, or threshold changed.

**Removed from pipeline:** full-DB `apply_relevance` on every sync; early dashboard rebuild after ingest/dedupe.

**Constraints:**
- No wildcard imports (`import *`) anywhere under `spejder/`; keep imports explicit.
- Keep invocation-scoped mutable caches (`text_translation_cache`, `title_translation_cache`) inside `run_inbox_sync`; do not promote to module globals.
- Build ingest translation transform via `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage` to keep GUI sync and inbox ingest logic aligned.
- Use `spejder.workflows.ingest_utils` for ingest per-file stats logging and inbox cleanup.
- Pass `llm` + `runtime_profile` into `ingest_docs_to_db` so opt-in career-alert synthesis can run on `found=0` during background sync.
- Treat `GuiSyncContext` callbacks as the only bridge back into GUI orchestration.
