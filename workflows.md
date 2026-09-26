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
- `list_career_alert_artifacts`, `disable_career_alert_artifact`, `enable_career_alert_artifact`, `promote_career_alert_artifact` — `career_alert_artifacts.py`

**Submodules:**

| Module | Role |
|--------|------|
| `dashboard.py` | Rebuild queue worker + missing-skill helper; re-exports record builders |
| `dashboard_records.py` | Row → dashboard dict + Hidden / Edited-today loaders |
| `gui.py` | GUI/server thread orchestration |
| `gui_sync.py` | Background inbox sync pipeline (portal → ingest → enrichment → shared skill hygiene → bad cloud); append-only sync event log via `sync_log.py` |
| `sync_log.py` | Append-only `{report_dir}/sync.log` writer for GUI sync and `process-inbox`; mirrors events to stdout as `sync …` |
| `skill_hygiene.py` | Shared skill hygiene stages for GUI sync and `process-inbox`: `run_skill_hygiene_stages` (blocked DB cleanup → stale low-share cleanup → bad-cloud seed/recalibrate) plus `run_stale_skill_cleanup` |
| `portal_sync.py` | External job portal sync (IT-DAY); gated by `itday_portal_sync_enabled` |
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
| `user_portrait.py` | User portrait context, LLM generation, diff, file I/O |
| `career_alert_artifacts.py` | CLI list/disable/enable/promote for career-alert format artifacts |

**Context:**
`cli.py` is a thin argparse layer delegating here. Workflows are callable from tests and `spejder.server` without going through the CLI.

**Dashboard rebuild:** `DashboardRebuildQueue` (`workflows/dashboard.py`) reloads three applied-stage query subsets — `get_applied_jobs`, `get_interview_jobs`, and `get_stopped_interview_jobs` — when rendering Applied / Interview / Stopped tabs, plus Hidden via `build_hidden_dashboard_records` and Edited today via `build_viewed_today_dashboard_records`. Those helpers live in `dashboard_records.py`, imported via `dashboard.py`. Inbox report writes (`write_inbox_dashboard_report`) and enrichment report writes (`enrichment.py` after `refresh-descriptions`) use the same helpers and pass `hidden_items` / `viewed_today_items` to `_render_html_dashboard`.

**GUI background sync** (`run_inbox_sync` in `gui_sync.py`):
0. Sync IT-DAY job portal (when `itday_portal_sync_enabled`)
0b. Post-portal company+title dedupe when `inserted_new > 0` (overlaps with existing LinkedIn/Jobindex drop before inbox ingest)
1. Ingest inbox (or backfill missing descriptions)
2. Delete processed inbox files
3. Company+title position dedupe (post-ingest)
4. Skill materialization (+ dashboard rebuild when `skills_updated > 0`)
5. Description generation (+ rebuild when updated)
6. Skill-pattern learning (+ rebuild when new patterns)
7–8. Shared skill hygiene (`run_skill_hygiene_stages` in `skill_hygiene.py`) — same contract as `process-inbox`:
   - 7. Blocked-skills DB cleanup + rescore (+ rebuild when DB changed)
   - 7b. Stale low-share skill cleanup + rescore (+ rebuild)
   - 8. Bad cloud seed / threshold recalibration
   - Profile save/reload once when `profile_dirty` (stale prune and/or cloud/threshold change)
