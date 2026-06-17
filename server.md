# spejder.server

**Purpose:**
Provides an interactive dashboard (web GUI) to review extracted jobs and view their relevance scores.

**API:**
- `start_server(host, port, profile, ...)`
- `create_app(...)`: FastAPI factory
- `POST /api/interview` — `{ job_id, on_interview }`; requires `applied=1`; clears `interview_stopped` when enabling
- `POST /api/interview/stopped` — `{ job_id, stopped }`; requires `applied=1`; clears `on_interview` when enabling
- `POST /api/interview/feedback` — `{ job_id, feedback }`; requires `applied=1` and `interview_stopped=1`
- `POST /api/applied/cover-letter/request` — `{ job_id, requested }`; requires `applied=1`; blocked after cover letter saved; queues dashboard rebuild
- `POST /api/applied/cover-letter` — `{ job_id, text }`; requires `applied=1`, `cover_letter_requested=1`, and no existing cover letter; queues dashboard rebuild (no skill rematerialization)
- All interview endpoints queue dashboard rebuild like `/api/applied`
- `POST /api/viewed` with `viewed=false` and `POST /api/feedback` with `signal=not relevant` clear interview fields in DB (same as unapply)
- `POST /api/skill/block` — `{ skill }`; profile-only block via `_block_skill_in_profile`, then `delete_skill_from_db` before dashboard rebuild; response includes `block_info` and `db_deleted` (`skill_rows_deleted`, `job_skill_links_deleted`)

**Context:**
Originally built with the built-in `http.server`, it has been upgraded to a modern asynchronous stack using **FastAPI** and **Uvicorn**. It provides endpoints like `/`, `/company.html`, `/logs`, `/action/submit_job`, etc. It fetches data via `db.py` and renders HTML via `managers/dashboard_manager.py` (Jinja2 templates).

**Dependencies:**
- `fastapi`, `uvicorn`, `pydantic`
- `spejder.db`, `spejder.workflows`, `spejder.managers.dashboard_manager`
