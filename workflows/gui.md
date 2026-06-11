# spejder.workflows.gui

**Purpose:**
GUI server orchestrator (`serve_gui`) that handles profile/path setup, background sync wiring, dashboard queue wiring, and FastAPI server startup.

**API:**
- `serve_gui`

**Context:**
Extracted workflow module. `gui.py` now focuses on server orchestration + dependency wiring and delegates:
- dashboard record/rebuild queue logic to `spejder/workflows/dashboard.py`
- background inbox synchronization to `spejder/workflows/gui_sync.py`

**Constraints:**
- No wildcard imports (`import *`) anywhere in this module; use explicit imports for all dependencies.
- Keep mutable runtime state invocation-scoped for server orchestration concerns; dashboard mutable state is encapsulated in `DashboardRebuildQueue`.

**Background sync:**
The 7-step background sync pipeline now lives in `spejder/workflows/gui_sync.py` (`GuiSyncContext` + `run_inbox_sync`). `serve-gui` CLI startup validates the language checker and configured translation model slots via `COMMAND_INIT` before the server starts; slots 2–3 are optional.

See `spejder/workflows.md` for the workflow-level summary.
