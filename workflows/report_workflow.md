# spejder.workflows.report_workflow

**Purpose:**
CLI-oriented report helpers: link frequency reports and JSONL-to-HTML rendering.

**API:**
- `report_links(folder)` — print top link counts from inbox email files
- `render_html(input_val, out, title)` — render JSONL job list via `dashboard_manager`
- `_report_limit_value`, `_report_max_relevant_positions`, `_report_max_not_relevant_positions` — profile limit helpers

**Context:**
Renamed from `workflows/reporting.py` to avoid collision with the removed root `spejder/reporting.py` stub. Interactive dashboard HTML is owned by `managers/dashboard_manager.py`.
