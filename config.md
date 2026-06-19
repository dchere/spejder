# spejder.config

**Purpose:**
Defines the central configuration schema (`AppConfig`) using Pydantic. It provides type-hinting, validation, and default values for all application settings.

**API:**
- `AppConfig`: Pydantic BaseModel containing properties like `include_keywords`, `n_ctx`, `server_port`, etc.
- `AppConfig.load(profile_path)`
- `AppConfig.save(profile_path)`
- `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT` (`20`) and `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX` (`150`) — single source for the good-skills cap
- `ANTIPATTERN_PROMPT_LIST_MAX` — alias of `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX`; antipattern prompt input limits in `antipattern_synthesis.py` import this neutral name. **Coupling:** intentionally equals `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX` (150); raising the profile max also raises LLM prompt list chunk caps unless split later
- `coerce_skill_antipattern_good_skills_count(raw)` — shared coercion for `skill_antipattern_good_skills_count`: valid `int` or integer-valued `float` → clamped `1`–`SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX`; `None` return → caller uses field default (bool, non-integer floats, strings, and other non-numeric types)
- **Tests:** `spejder.tests.test_config_coercion`
- `@field_validator("skill_antipattern_good_skills_count", mode="before")` on `AppConfig` — delegates to `coerce_skill_antipattern_good_skills_count`; when coercion returns `None`, uses `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT` (bool must not become `1`)
- `_normalize_skill_antipattern_fields(data)` — on profile load, drops `skill_antipattern_good_skills_count` when `coerce_skill_antipattern_good_skills_count` returns `None` (field default applies). Does not rewrite valid values; clamping happens in the field validator on `AppConfig(...)` construction. **Dual normalization:** profile load drops invalid keys only; direct `AppConfig(...)` and post-load construction both clamp via the field validator

**Context:**
Replaces the old dictionary-based profile system (`FALLBACK_DEFAULT_PROFILE`). All modules now pass around the formalized `AppConfig` object rather than a dictionary, eliminating `profile.get(...)` calls throughout the codebase.

**Language / translation profile fields:**
- `language_checker_model_path` — FastText lid model (shared for Danish and Ukrainian detection)
- `language_translation_model_1/2/3` — MarianMT model directories for non-English source languages
- `language_translation_source_1/2/3` — FastText ISO 639-1 source language codes paired with each model slot (e.g. `da`, `uk`, `fr`)
- Legacy `translation_model_path`, `danish_translation_model_path`, and `ukrainian_translation_model_path` migrate into slots 1–2 on load (Danish → slot 1, Ukrainian → slot 2; Ukrainian-only legacy profiles therefore use slot 2)

**Skill extraction profile fields:**
- `skill_new_confidence_threshold` — minimum LLM confidence for novel skill candidates (default `0.9`); quality/evidence checks in `filtering._is_candidate_strong` still apply
- `skill_antipattern_synthesis_count` — antipattern rules to synthesize per sync (default `3`)
- `skill_antipattern_validation_runs` — stable extraction runs per validation step (default `3`)
- `skill_antipattern_prompt_max_items` — max antipatterns injected into the job extraction prompt (default `40`)
- `skill_antipattern_good_skills_count` — top DB skills by job link count woven into per-candidate synthetic validation jobs (default `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT`, clamped `1`–`SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX` via `coerce_skill_antipattern_good_skills_count` on profile load and `AppConfig` construction; bool, non-integer floats, strings, and other invalid types use the default)
- No per-job skill count cap; extraction returns all skills that pass filters
- `skill_new_max_per_job` — **removed**; ignored if still present in an old `profile.json` (dropped on next profile save)

**Portrait profile fields:**
- `default_cv_path` — CV file or folder for portrait generation (default `./CV`)
- `default_portrait_path` — committed portrait text file (default `./portrait.txt`)
- `portrait_max_tokens` — LLM output budget for portrait regeneration (default `1200`)

**Dependencies:**
- `pydantic`, `json`, `os`
