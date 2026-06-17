# spejder.workflows

**Purpose:**
Core orchestration for CLI commands, GUI background sync, and heavy multi-step pipelines.

**High-level orchestrators** (via `workflows/__init__.py`):
- `process_inbox` — `inbox_workflow.py`
- `serve_gui` — `gui.py`
- `run_inbox_sync` — `gui_sync.py`
- `report_links`, `render_html` — `report_workflow.py`
- `dedupe_jobs` — `deduplication.py`
- `refresh_descriptions` — `enrichment.py`
- `summarize_file`, `summarize_folder` — `summarization.py`
- `init_profile` — `profile.py`

**Submodules:**

| Module | Role |
|--------|------|
| `dashboard.py` | Dashboard record building + rebuild queue worker |
| `gui.py` | GUI/server thread orchestration |
| `gui_sync.py` | Background inbox sync pipeline (7 steps) |
| `ingest_utils.py` | Per-file ingest stats + inbox file cleanup |
| `inbox_report.py` | Inbox relevant-job summaries + HTML dashboard write |
| `job_enrichment.py` | Facade re-exporting job enrichment helpers |
| `job_translation.py` | Ingest entry translation factory |
| `job_text_enrichment.py` | Raw-text enrichment (title, summary, page context) |
| `job_descriptions.py` | LLM description generation + quality heuristics |
| `job_skills_materialize.py` | Skill extraction materialization batches |
| `job_easy_apply.py` | LinkedIn easy-apply detection |
| `report_workflow.py` | CLI link reports + JSONL HTML export |
| `deduplication.py` | Company+title position dedupe wrapper |
| `enrichment.py` | `refresh-descriptions` command |
| `text_prepend.py` | Summary validity checks + title/summary raw-text prepend |
| `formatting.py` | Dashboard title HTML line rendering |
| `llm_utils.py` | CLI LLM init helpers |
| `summarization.py` | File/folder summarization commands |

**Context:**
`cli.py` is a thin argparse layer delegating here. Workflows are callable from tests and `server.py` without going through the CLI.

**Dashboard rebuild:** `DashboardRebuildQueue` (`workflows/dashboard.py`) reloads three applied-stage query subsets — `get_applied_jobs`, `get_interview_jobs`, and `get_stopped_interview_jobs` — when rendering Applied / Interview / Stopped tabs.

**GUI background sync** (`run_inbox_sync` in `gui_sync.py`):
1. Ingest inbox (or backfill missing descriptions)
2. Delete processed inbox files
3. Company+title position dedupe
4. Queue dashboard rebuild
5. Relevance scoring
6. Skill materialization, description generation, skill-pattern learning
7. Optional antipattern sync (async)
