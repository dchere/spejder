# Managers

**Purpose:**
Higher-level services coordinating config, HTML rendering, and language tooling.

**Modules:**
- `profile_manager.py` / `profile_manager.md` — profile defaults and persistence
- `dashboard_manager.py` / `dashboard_manager.md` — interactive HTML dashboard generation (Applied / Interview / Stopped panels; live company-feedback pre-fill via `data-company-feedback` and `ensureCompanyFeedbackUI`)
- `language_manager/` / `language_manager.md` — detection, translation, title normalization
  - `detection.py` — FastText Danish and Ukrainian detection (`translation_source_language`)
  - `engines.py` — MarianMT / ctranslate2 runtime
  - `initialization.py` — CLI init hooks
  - `text_translation.py` — body translation
  - `titles.py` — title translation and compare keys
  - `utils.py` — shared init helpers

**Architectural rule (`No monoliths`):**
No file should exceed 300 lines. Split into domain-specific modules and update this file when adding submodules.
