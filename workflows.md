# spejder.workflows

**Purpose:**
Contains the core orchestration logic for all CLI orchestrations and heavy workflows.

**API:**
- High-level orchestrators: `process_inbox`, `report_links`, `serve_gui`, `dedupe_jobs`, `sync_user_skills`, etc.
- Dashboard extraction module: `spejder/workflows/dashboard.py` (record building + rebuild queue worker)
- Shared ingest helpers: `spejder/workflows/ingest_utils.py` (per-file ingest stats + inbox file cleanup)

**Context:**
Extracted from `cli.py` to create a "thin CLI" pattern. `cli.py` now merely sets up `argparse` and delegates execution to the workflow functions here. This decouples the CLI interface from the actual execution logic, making workflows usable directly from tests or Web endpoints.

**GUI background sync pipeline (`run_inbox_sync` in `gui_sync.py`):**
1. Ingest inbox (or backfill missing descriptions)
2. Delete processed inbox files
3. Cross-source dedupe (`run_cross_source_dedupe`)
4. Queue dashboard rebuild (ingest + dedupe stats)
5. Relevance scoring
6. Skill materialization, description generation, skill-pattern learning
7. Optional antipattern sync (async)
