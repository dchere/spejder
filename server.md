# spejder.server

**Purpose:**
Provides an interactive dashboard (web GUI) to review extracted jobs and view their relevance scores.

**API:**
- `start_server(host, port, profile, ...)`
- `create_app(...)`: FastAPI factory

**Context:**
Originally built with the built-in `http.server`, it has been upgraded to a modern asynchronous stack using **FastAPI** and **Uvicorn**. It provides endpoints like `/`, `/company.html`, `/logs`, `/action/submit_job`, etc. It fetches data via `db.py` and renders HTML via `reporting.py` (which uses Jinja2).

**Dependencies:**
- `fastapi`, `uvicorn`, `pydantic`
- `spejder.reporting`, `spejder.db`, `spejder.workflows`
