# spejder.workflows.job_enrichment

**Purpose:**
Per-job text enrichment: translation, description generation, skill materialization. `job_enrichment.py` is a **facade**; logic lives in focused submodules.

**Submodules:**
- `job_translation.py` — `make_translate_job_entry_for_storage`
- `job_text_enrichment.py` — `_enrich_raw_text_with_position_page`, `_build_title_fields`, `_resolve_title_and_place` (spaced ` - ` or trailing `i City` title suffix; not `Social- og`-style hyphens)
- `job_descriptions.py` — description LLM generation, quality checks, `_generate_missing_descriptions_for_ingest`
- `job_skills_materialize.py` — `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
  - Uses `replace_job_skills`; propagates `skills_changed`
  - No per-job skill count cap; LLM novel skills gated by `skill_new_confidence_threshold` and phrase-quality checks only
  - Rescores when `rescore AND job_in_active_rescore_scope(row) AND (skills_changed OR first_materialize)`
  - `first_materialize=True` when the job had no cached `job_skills` before extraction (covers keyword-only score when LLM returns no skills)
  - Batch scope via `get_jobs_for_active_rescore` (unviewed, applied, interview stages)
- `job_easy_apply.py` — `_is_easy_apply_item`

**API (import from `job_enrichment` facade):**
- `make_translate_job_entry_for_storage(runtime_profile, text_translation_cache, title_translation_cache) -> Callable[[dict], dict]`
- `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
- `_generate_missing_descriptions_for_ingest`
- Description/summary helpers used by inbox and dashboard flows

**Translation cache ownership:**
- `text_translation_cache` and `title_translation_cache` are caller-owned, invocation-scoped dictionaries.
- The factory reuses those dictionaries across records within one ingest run; callers decide lifecycle and must not treat them as module-level globals.
