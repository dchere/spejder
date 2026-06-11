# spejder.managers.dashboard_manager

**Purpose:**
Generate HTML dashboards for the reporting modules.

**Context:**
Used to summarize and render job offers into presentable HTML files.

**Submodules:**
- `dashboard_templates.py` — shared Jinja2 environment
- `dashboard_cards.py` — job card HTML (`_build_job_cards`, `_render_html_from_items`); `card_panel` arg controls Applied / Interview / Stopped controls and company feedback UI; non-empty `company_feedback` sets `data-company-feedback` on the card article
- `dashboard_sorting.py` — unviewed/score and applied list ordering
- `dashboard_manager.py` — facade for company and main dashboard renderers; splits applied-stage items into three tabs: Applied (plain applied), Interview (`on_interview=1`), Stopped (`interview_stopped=1`)
