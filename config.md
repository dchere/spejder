# spejder.config

**Purpose:**
Defines the central configuration schema (`AppConfig`) using Pydantic. It provides type-hinting, validation, and default values for all application settings.

**API:**
- `AppConfig`: Pydantic BaseModel containing properties like `include_keywords`, `n_ctx`, `server_port`, etc.
- `AppConfig.load(profile_path)`
- `AppConfig.save(profile_path)`

**Context:**
Replaces the old dictionary-based profile system (`FALLBACK_DEFAULT_PROFILE`). All modules now pass around the formalized `AppConfig` object rather than a dictionary, eliminating `profile.get(...)` calls throughout the codebase.

**Dependencies:**
- `pydantic`, `json`, `os`
