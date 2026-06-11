# spejder.config

**Purpose:**
Defines the central configuration schema (`AppConfig`) using Pydantic. It provides type-hinting, validation, and default values for all application settings.

**API:**
- `AppConfig`: Pydantic BaseModel containing properties like `include_keywords`, `n_ctx`, `server_port`, etc.
- `AppConfig.load(profile_path)`
- `AppConfig.save(profile_path)`

**Context:**
Replaces the old dictionary-based profile system (`FALLBACK_DEFAULT_PROFILE`). All modules now pass around the formalized `AppConfig` object rather than a dictionary, eliminating `profile.get(...)` calls throughout the codebase.

**Language / translation profile fields:**
- `language_checker_model_path` — FastText lid model (shared for Danish and Ukrainian detection)
- `language_translation_model_1/2/3` — MarianMT model directories for non-English source languages
- `language_translation_source_1/2/3` — FastText ISO 639-1 source language codes paired with each model slot (e.g. `da`, `uk`, `fr`)
- Legacy `translation_model_path`, `danish_translation_model_path`, and `ukrainian_translation_model_path` migrate into slots 1–2 on load (Danish → slot 1, Ukrainian → slot 2; Ukrainian-only legacy profiles therefore use slot 2)

**Dependencies:**
- `pydantic`, `json`, `os`
