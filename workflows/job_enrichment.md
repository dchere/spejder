# spejder.workflows.job_enrichment

**Purpose:**
Per-job text enrichment: translation, description generation, skill materialization. `job_enrichment.py` is a **facade**; logic lives in focused submodules.

**Submodules:**
- `job_translation.py` — `make_translate_job_entry_for_storage`
- `job_text_enrichment.py` — `_enrich_raw_text_with_position_page`, `_build_title_fields`
- `job_descriptions.py` — description LLM generation, quality checks, `_generate_missing_descriptions_for_ingest`
- `job_skills_materialize.py` — `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
- `job_easy_apply.py` — `_is_easy_apply_item`

**API (import from `job_enrichment` facade):**
- `make_translate_job_entry_for_storage(runtime_profile, text_translation_cache, title_translation_cache) -> Callable[[dict], dict]`
- `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
- `_generate_missing_descriptions_for_ingest`
- Description/summary helpers used by inbox and dashboard flows

**Translation cache ownership:**
- `text_translation_cache` and `title_translation_cache` are caller-owned, invocation-scoped dictionaries.
- The factory reuses those dictionaries across records within one ingest run; callers decide lifecycle and must not treat them as module-level globals.
