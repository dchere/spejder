# spejder.workflows

**Purpose:**
Contains the core orchestration logic for all CLI orchestrations and heavy workflows.

**API:**
- High-level orchestrators: `process_inbox`, `report_links`, `serve_gui`, `dedupe_jobs`, `sync_user_skills`, etc.

**Context:**
Extracted from `cli.py` to create a "thin CLI" pattern. `cli.py` now merely sets up `argparse` and delegates execution to the workflow functions here. This decouples the CLI interface from the actual execution logic, making workflows usable directly from tests or Web endpoints.
