# spejder.managers.dashboard_manager

**Purpose:**
Generate HTML dashboards for the reporting modules.

**Context:**
Used to summarize and render job offers into presentable HTML files.

**Submodules:**
- `dashboard_templates.py` — shared Jinja2 environment
- `dashboard_cards.py` — job card HTML (`_build_job_cards`, `_render_html_from_items`)
- `dashboard_sorting.py` — unviewed/score and applied list ordering
- `dashboard_manager.py` — facade for company and main dashboard renderers
