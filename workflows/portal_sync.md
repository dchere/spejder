# spejder.workflows.portal_sync

**Purpose:**
Sync external job portal listings into SQLite during inbox/GUI sync.

**API:**
- `sync_itday_portal(db_path, *, entry_transform=None, enabled=True) -> dict` — when `enabled` is true, `ensure_db` then fetch IT-DAY portal pages, upsert entries, return `{processed, inserted_new, skipped_existing, found}` plus optional `error` when fetch fails; when `enabled` is false, skip `ensure_db`/fetch/ingest, log `IT-DAY portal sync: not enabled in profile`, and return zeros for those keys plus `skipped_disabled=True` (no `error`)

**Context:**
Called at the start of `run_inbox_sync` (GUI **Sync inbox** and `serve-gui` startup sync) and from `process_inbox` (including an empty inbox). Callers pass `enabled` from `AppConfig.itday_portal_sync_enabled` (default true). Uses the same ingest translation transform as email ingest. Fetch failures are logged and do not abort the sync pipeline. When `inserted_new > 0`, callers run company+title dedupe immediately after this function returns (GUI stage `portal_dedupe`; process-inbox log prefix `post-portal dedupe`) so portal overlaps with existing sources merge before later pipeline stages.

**Skip interaction:**
GUI sync skips the rest of the pipeline only when inbox is empty, descriptions are up to date, **and** the portal inserted zero new rows (`found>0` with `inserted_new=0` still skips; disabled portal also yields `inserted_new=0`). `process_inbox` uses the same empty-inbox gate (plus missing-description backfill) before requiring an LLM.
