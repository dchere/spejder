# spejder.workflows.dashboard

**Purpose:**
Dashboard extraction module for GUI workflows. Owns dashboard record shaping, missing-skill materialization helper, and queued rebuild worker behavior.

**API:**
- `coalesce_rebuild_reasons(reasons: list[str]) -> str` — pure helper; returns the last reason with a queued-count suffix when multiple are pending (e.g. `"third (+2 queued)"`). Returns `""` for an empty list.
- `build_dashboard_record(…, translate_title: bool = True)` — when `True` (default) calls `_build_title_fields` to translate/cache the title; pass `False` to read `title`/`title_english` directly from the row (used for applied-jobs where titles are already stored in English). Includes `on_interview`, `interview_stopped`, `company_feedback`, and `applied_at` on dashboard records. Job card `skills` string comes from `_format_skills(cached_skills)` with no per-job count cap.
- `populate_missing_dashboard_skills` — batch materialize via `materialize_jobs_skills` for rebuild scope rows
- `DashboardRebuildQueue` (`queue`, `start_worker`, `rebuild`, `is_idle`, `wait_until_idle`) — rebuild loads three applied-stage subsets (`get_applied_jobs`, `get_interview_jobs`, `get_stopped_interview_jobs`) and passes them to `_render_html_dashboard` as Applied / Interview / Stopped tabs. Skills tab rows come from `_build_skills_tab_items` (`position_pct`, `position_count`, `occurrences`, `added_at`). Portrait panel (opened via toolbar icon `#btn-portrait`, not the mode tab row) embeds committed text from `default_portrait_path` via `user_portrait.load_portrait`. `_rebuild_active` is set in the worker immediately before `rebuild()` so `is_idle()` does not briefly return true between dequeuing and rebuild start. `wait_until_idle` blocks until no rebuild is active and the pending queue is empty (used by inbox sync to keep the **Sync inbox** button disabled until report regeneration finishes); returns `False` on timeout.
- Dashboard templates (`dashboard.html`, `company_dashboard.html`): checking **Applied** removes the card from the current tab and updates counters but does **not** switch tabs; unchecking returns to Relevant or Not relevant. **On interview** / **Stopped** checkboxes switch to the Interview / Stopped tabs when enabled; unchecking returns to Applied.

**Constraints:**
- Use explicit imports only.
- Keep mutable dashboard runtime state per `DashboardRebuildQueue` instance (`_dashboard_lock`, signal/queue primitives, title translation cache); do not use module-level mutable globals.
- Preserve existing dashboard rebuild behavior and logging semantics during extraction.

**Context:**
Extracted from `spejder/workflows/gui.py` so `gui.py` focuses on server orchestration/wiring, while dashboard-specific threading and report rebuild logic stays isolated in this module.
