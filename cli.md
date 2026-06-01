# spejder.cli

**Purpose:**
Provides the "thin" command-line interface entry point for the application.

**API:**
- Parses `argparse` definitions for tasks like `process-inbox`, `report-links`, `serve-gui`, etc.

**Context:**
As part of the structural refactor, `cli.py` has been completely stripped of business logic. It handles solely formatting descriptions, reading arguments via standard argparse configuration, and forwarding invocations cleanly into their real implementations housed inside `spejder.workflows`.
