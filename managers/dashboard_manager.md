# spejder.managers.dashboard_manager

**Purpose:**
Generate HTML dashboards for the reporting modules.

**Context:**
Used to summarize and render job offers into presentable HTML files.

**Submodules:**
- `dashboard_templates.py` — shared Jinja2 environment
- `dashboard_cards.py` — job card HTML (`_build_job_cards`, `_render_html_from_items`); `card_panel` arg controls Applied / Interview / Stopped controls and company feedback UI; applied cards include optional cover-letter checkbox + Save flow (full-width textarea matching manual description input); card UI updates come from background report rebuild, not client-side collapse on save; non-empty `company_feedback` sets `data-company-feedback` on the card article
- `dashboard_sorting.py` — unviewed/score and applied list ordering; applied sort uses completion rule: incomplete manual description or pending cover letter → top, else score
- `dashboard_manager.py` — facade for company and main dashboard renderers; splits applied-stage items into three tabs: Applied (plain applied), Interview (`on_interview=1`), Stopped (`interview_stopped=1`); Skills tab: sortable columns (default name A→Z, `aria-sort` on active header, keyboard Enter/Space on headers), header/cell tooltips, **Job share** (% of jobs with extracted skills), **Learned** (`skill_patterns.occurrences`), **Want to learn** checkbox column; checkbox toggles refresh row sort data client-side; **Portrait** tab: embeds committed text from `default_portrait_path` via `embed_portrait_for_textarea` (textarea-safe, not HTML-escaped); passes `has_portrait` for empty-state hint; live edits via `/api/portrait*` when the server is running
