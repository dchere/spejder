# spejder.workflows.gui_sync

**Purpose:**
Background inbox synchronization pipeline extracted from `gui.py`, preserving existing behavior while isolating sync-side orchestration from dashboard/server concerns.

**API:**
- `GuiSyncContext` (frozen dataclass carrying paths, runtime config, and callbacks)
- `run_inbox_sync(context: GuiSyncContext) -> None`

**Pipeline (7 steps):**
1. Ingest inbox input (or detect missing-description backfill mode)
2. Delete processed inbox files
3. Run cross-source deduplication
4. Queue dashboard rebuild (ingest + dedupe snapshot)
5. Apply relevance scoring
6. Materialize missing skills, generate missing descriptions, learn skill patterns
7. Optionally run async antipattern sync (daemon thread). Skipped runs log `skip_reason` and do **not** reload the profile or queue a dashboard rebuild; successful **commits** (`committed=True`) do both.

**Constraints:**
- No wildcard imports (`import *`) anywhere under `spejder/`; keep imports explicit.
- Keep invocation-scoped mutable caches (`text_translation_cache`, `title_translation_cache`) inside `run_inbox_sync`; do not promote to module globals.
- Build ingest translation transform via `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage` to keep GUI sync and inbox ingest logic aligned.
- Use `spejder.workflows.ingest_utils` for ingest per-file stats logging and inbox cleanup.
- Treat `GuiSyncContext` callbacks as the only bridge back into GUI orchestration.
