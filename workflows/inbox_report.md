# spejder.workflows.inbox_report

**Purpose:**
HTML dashboard and per-job summary generation for the `process-inbox` CLI workflow.

**API:**
- `summarize_relevant_jobs_for_inbox(db_path, relevant_jobs, llm, *, max_tokens, max_input_chars)`
- `write_inbox_dashboard_report(db_path, profile, llm, report_dir) -> str` — builds Relevant / Not relevant / Applied-stage records, loads Hidden via `build_hidden_dashboard_records` and Edited today via `build_viewed_today_dashboard_records`, and passes `hidden_items` / `viewed_today_items` to `_render_html_dashboard`

**Context:**
Extracted from `inbox_workflow.py` to keep the inbox orchestrator under the ~300-line module limit.
