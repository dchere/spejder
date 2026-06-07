# spejder.core

**Purpose:**
Central, lightweight utilities for generalized execution behaviors, logging, profile loading, and workspace path resolution.

**API:**
- `load_profile(profile_path)` (Delegates to `config.AppConfig.load()`)
- `save_profile(profile, profile_path)` (Delegates to `config.AppConfig.save()`)
- `load_runtime_profile(profile_path)` — load user profile with bundled-default fallback
- `print_step(message)`
- `fail_init(message)`
- `USER_PROFILE_PATH` — default relative profile path (`./profile.json`)
- `WORKSPACE_ROOT_ENV` — env var name (`SPEJDER_WORKSPACE`)
- `workspace_root()` — absolute workspace anchor (`SPEJDER_WORKSPACE` or cwd)
- `resolve_user_path(path)` — resolve relative paths against workspace root; leave absolute paths unchanged

**Context:**
- Used by all modules that need standard logging steps or need to load config dynamically.
- `load_profile` returns the Pydantic `AppConfig` class objects standardizing configurations project-wide.
- The CLI resolves relative data paths via `resolve_user_path()` before dispatching commands.
- Workflows or scripts invoked outside the CLI should call `resolve_user_path()` for relative paths (`./jobs.db`, `./profile.json`, etc.) so behavior matches the CLI when `SPEJDER_WORKSPACE` is set.
- `SPEJDER_WORKSPACE` may be absolute or relative (relative values resolve against the process cwd).

**Dependencies:**
- `spejder.config`
