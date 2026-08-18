# spejder.workflows.portal_sync

**Purpose:**
Sync external job portal listings into SQLite during inbox/GUI sync.

**API:**
- `sync_itday_portal(db_path, *, entry_transform=None) -> dict` — fetch IT-DAY portal pages, upsert entries, return `{processed, inserted_new, skipped_existing, found}` plus optional `error` when fetch fails

**Context:**
Called at the start of `run_inbox_sync` (GUI **Sync inbox** and `serve-gui` startup sync) and from `process_inbox` (including an empty inbox). Uses the same ingest translation transform as email ingest. Fetch failures are logged and do not abort the sync pipeline.

**Skip interaction:**
GUI sync skips the rest of the pipeline only when inbox is empty, descriptions are up to date, **and** the portal inserted zero new rows (`found>0` with `inserted_new=0` still skips). `process_inbox` uses the same empty-inbox gate (plus missing-description backfill) before requiring an LLM.
