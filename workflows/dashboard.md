# spejder.workflows.dashboard

**Purpose:**
Dashboard extraction module for GUI workflows. Owns dashboard record shaping, missing-skill materialization helper, and queued rebuild worker behavior.

**API:**
- `coalesce_rebuild_reasons(reasons: list[str]) -> str` — pure helper; returns the last reason with a queued-count suffix when multiple are pending (e.g. `"third (+2 queued)"`). Returns `""` for an empty list.
- `build_dashboard_record(…, translate_title: bool = True)` — when `True` (default) calls `_build_title_fields` to translate/cache the title; pass `False` to read `title`/`title_english` directly from the row (used for applied-jobs where titles are already stored in English).
- `populate_missing_dashboard_skills`
- `DashboardRebuildQueue` (`queue`, `start_worker`, `rebuild`)

**Constraints:**
- Use explicit imports only.
- Keep mutable dashboard runtime state per `DashboardRebuildQueue` instance (`_dashboard_lock`, signal/queue primitives, title translation cache); do not use module-level mutable globals.
- Preserve existing dashboard rebuild behavior and logging semantics during extraction.

**Context:**
Extracted from `spejder/workflows/gui.py` so `gui.py` focuses on server orchestration/wiring, while dashboard-specific threading and report rebuild logic stays isolated in this module.
