# spejder.cli

**Purpose:**
Provides the "thin" command-line interface entry point for the application.

**API:**
- Parses `argparse` definitions for tasks like `process-inbox`, `report-links`, `serve-gui`, etc.

**Context:**
As part of the structural refactor, `cli.py` has been completely stripped of business logic. It handles solely formatting descriptions, reading arguments via standard argparse configuration, and forwarding invocations cleanly into `spejder.workflows`. There is no separate `spejder.commands` package — command handlers live as workflow functions.

**Init orchestration:**
`COMMAND_INIT` maps each command to required subsystems (`language_checker`, `translation`, `llm`). Commands not listed skip pre-init. `serve-gui` runs `language_checker` + `translation` init only (no LLM preload; background sync loads the GGUF lazily). Only `sync-user-skills` receives `args._llm` from `initialize_llm_or_exit` to avoid loading the model twice; the CLI releases the local instance (`del llm`) for other LLM commands so workflows load a single in-memory copy. Other LLM commands validate at the CLI layer and load their own instance in the workflow. `--quiet-model` affects CLI `verbose` only for `sync-user-skills`; `refresh-descriptions` and similar commands use workflow-level quiet flags.

**Paths:**
Relative args listed in `_RELATIVE_ARG_NAMES` are resolved via `core.resolve_user_path()` against `SPEJDER_WORKSPACE` or the current working directory. `--model` is not resolved (often an absolute GGUF path). Profile path for init hooks uses `_profile_path_from_args(cmd_name, args)` so `summarize-file --path` (input file) is never mistaken for a profile.
