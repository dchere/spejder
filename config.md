# spejder.config

**Purpose:**
Defines the central configuration schema (`AppConfig`) using Pydantic. It provides type-hinting, validation, and default values for all application settings.

**API:**
- `AppConfig`: Pydantic BaseModel containing properties like `include_keywords`, `n_ctx`, `server_port`, etc.
- `AppConfig.load(profile_path)`
- `AppConfig.save(profile_path)`
- `SKILL_BIGRAM_THRESHOLD_MARGIN_DEFAULT` (`0.5`) — default margin for auto threshold calibration
- `_drop_legacy_antipattern_fields(data)` — on profile load, removes deprecated antipattern keys from older `profile.json` files

**Context:**
Replaces the old dictionary-based profile system (`FALLBACK_DEFAULT_PROFILE`). All modules now pass around the formalized `AppConfig` object rather than a dictionary, eliminating `profile.get(...)` calls throughout the codebase.

**Language / translation profile fields:**
- `language_checker_model_path` — FastText lid model (shared for Danish and Ukrainian detection)
- `language_translation_model_1/2/3` — MarianMT model directories for non-English source languages
- `language_translation_source_1/2/3` — FastText ISO 639-1 source language codes paired with each model slot (e.g. `da`, `uk`, `fr`)
- Legacy `translation_model_path`, `danish_translation_model_path`, and `ukrainian_translation_model_path` migrate into slots 1–2 on load (Danish → slot 1, Ukrainian → slot 2; Ukrainian-only legacy profiles therefore use slot 2)

**Skill extraction profile fields:**
- `skill_new_confidence_threshold` — minimum LLM confidence for novel skill candidates (default `0.9`); quality/evidence checks in `filtering._is_candidate_strong` still apply
- `skill_bigram_toxicity_threshold` — last sync-computed toxicity cutoff (auto-written cache for extraction between syncs; not a hand-edit knob). GUI background sync overwrites it via `recalibrate_and_store_threshold`.
- `skill_bigram_threshold_margin` — operator-tunable margin for the p95 good / p50 bad formula (default `0.5`)
- `bad_cloud_seeded` — one-time migration flag; when false, GUI sync seeds `bad_ngram_weights` from existing `blocked_skills`
- No per-job skill count cap; extraction returns all skills that pass filters
- `skill_new_max_per_job` — **removed**; ignored if still present in an old `profile.json` (dropped on next profile save)
- Legacy `skill_extraction_antipatterns` and `skill_antipattern_*` keys are dropped on load

**Portrait profile fields:**
- `default_cv_path` — CV file or folder for portrait generation (default `./CV`)
- `default_portrait_path` — committed portrait text file (default `./portrait.txt`)
- `portrait_max_tokens` — LLM output budget for portrait regeneration (default `1200`)

**Career-alert artifact profile fields:**
- `career_alert_artifacts_dir` — user overlay directory for synthesized/edited JSON artifacts (default `./career_alert_artifacts`)
- `career_alert_artifacts_disabled` — list of artifact ids to skip (default `[]`)
- `career_alert_synth_enabled` — opt-in auto-synthesis on ingest when a file yields zero positions (default `false`)
- `career_alert_synth_link_ratio` — minimum fraction of LLM-proposed links the interpreter must recover (default `0.8`)
- `career_alert_synth_title_ratio` — minimum title-agreement fraction on recovered ∩ proposed links (default `0.8`)

**Dependencies:**
- `pydantic`, `json`, `os`
