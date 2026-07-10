# spejder.server

**Purpose:**
Provides an interactive dashboard (web GUI) to review extracted jobs and view their relevance scores.

**API:**
- `start_server(host, port, profile, ...)`
- `create_app(...)`: FastAPI factory; `get_report_rebuild_idle` callback (default `lambda: True`) drives `idle` on `GET /api/report/status`
- `POST /api/interview` — `{ job_id, on_interview }`; requires `applied=1`; clears `interview_stopped` when enabling
- `POST /api/interview/stopped` — `{ job_id, stopped }`; requires `applied=1`; clears `on_interview` when enabling
- `POST /api/interview/feedback` — `{ job_id, feedback }`; requires `applied=1` and `interview_stopped=1`
- `POST /api/applied/cover-letter/request` — `{ job_id, requested }`; requires `applied=1`; blocked after cover letter saved; queues dashboard rebuild
- `POST /api/applied/cover-letter` — `{ job_id, text }`; requires `applied=1`, `cover_letter_requested=1`, and no existing cover letter; queues dashboard rebuild (no skill rematerialization)
- All interview endpoints queue dashboard rebuild like `/api/applied`
- `POST /api/viewed` with `viewed=false` and `POST /api/feedback` with `signal=not relevant` clear interview fields in DB (same as unapply)
- `POST /api/skill/user` — after profile persist, runs `rescore_active_jobs` then dashboard rebuild
- `POST /api/skill/block` — delegates to shared block runner (`_run_skill_block` with one skill); profile block + `delete_skill_from_db`, `rescore_jobs_if_active` on `affected_job_ids`, dashboard rebuild; response includes `block_info` and `db_deleted`
- `POST /api/skill/delete` — delegates to shared delete runner (`_run_skill_delete` with one skill); profile cleanup + DB delete + rescore + rebuild
- `POST /api/skill/block-batch` — `{ skills: string[] }`; same `_run_skill_block` path as single block
- `POST /api/skill/delete-batch` — same request shape; same `_run_skill_delete` path as single delete
- `POST /api/applied/raw-text` — `{ job_id, text }`; requires `applied=1` and non-empty `text`; appends `[MANUAL_APPLIED_DESCRIPTION]` block to `raw_text`, clears `job_skills`, rematerializes skills, then rescoring via `materialize_job_skills(..., rescore=True, first_materialize=True)` so keyword-only score updates even when LLM returns no skills. Returns 400 when text empty or job not applied; 500 if save succeeded but the job row cannot be loaded for enrichment (skills cache already cleared in that case).
- `POST /api/report/rebuild` — queues dashboard rebuild (`reason="manual rebuild"`); no DB mutation; used by the report page **Regenerate report** button
- `GET /api/report/status` — `{ ok, idle, last_modified }`; `idle` reflects whether the dashboard rebuild queue is idle (`get_report_rebuild_idle`); `last_modified` is the HTTP-date of `report.html` on disk (empty when missing). Used by tab-switch stale reload logic in `dashboard.html`.
- `POST /api/inbox/sync` — starts a background inbox sync when `trigger_inbox_sync` is wired (`serve-gui`); returns 503 when inbox sync is not configured; returns 409 when a sync is already running
- `GET /api/inbox/sync/status` — `{ running, stage_id, stage_message, status, message }`; `status` is `running`, `complete`, `skipped`, `failed`, or `idle`
- `GET /api/portrait` — `{ ok, text }`; committed portrait from `default_portrait_path`
- `POST /api/portrait/generate` — sync LLM regeneration; returns `{ ok, draft, committed, diff_html }`; requires `default_model`; 503 without model; 400 without context; 409 when generation already in progress; does not write file
- `POST /api/portrait/save` — `{ text }`; writes portrait file; no dashboard rebuild

**Context:**
Originally built with the built-in `http.server`, it has been upgraded to a modern asynchronous stack using **FastAPI** and **Uvicorn**. It provides endpoints like `/`, `/company.html`, `/logs`, `/action/submit_job`, etc. It fetches data via `db.py` and renders HTML via `managers/dashboard_manager.py` (Jinja2 templates).

**Dependencies:**
- `fastapi`, `uvicorn`, `pydantic`
- `spejder.db`, `spejder.workflows`, `spejder.managers.dashboard_manager`
