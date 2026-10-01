# spejder.workflows.job_enrichment

**Purpose:**
Per-job text enrichment: translation, description generation, skill materialization. `job_enrichment.py` is a **facade**; logic lives in focused submodules.

**Submodules:**
- `job_translation.py` — `make_translate_job_entry_for_storage`
- `job_text_enrichment.py` — `_enrich_raw_text_with_position_page`, `_build_title_fields`, `_resolve_title_and_place` (spaced ` - ` or trailing `i City` title suffix; not `Social- og`-style hyphens). When `place` is missing and trailing `i City` is parsed, the display title is retained in full and only `place` is filled. Skill materialize calls enrichment with `include_summary=False` and `prefer_page=True` (substantial page ≳800 chars drops listing raw that is mostly duplicated by the scrape; title presence dedupe via `text_prepend`). Description / display paths keep default `include_summary=True`. Page append reserves ≤3000 chars so end-truncation does not cut the page tail.
- `job_descriptions.py` — description LLM generation, quality checks, `_generate_missing_descriptions_for_ingest`
  - Optional `on_progress(checked, total, updated[, eta_s])` and `on_status_message(str)` run after each selected row; 3-arg callbacks still work (`TypeError` fallback). The GUI line is the base sentence, plus `Estimated time left: …` only when remaining work has a usable ETA. The sidecar is `{db_path}.descriptions_eta.json` as version 2 kind `slow` samples (last 32). Old `{total_seconds, count}` files are ignored. Live counters were reset because sample meaning changed. Sync pipelines pass `progress=False` when `on_progress` is wired so cadence is `sync_log` only.
- `job_skills_materialize.py` — `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
  - Uses `replace_job_skills`; propagates `skills_changed`
  - No per-job skill count cap; LLM novel skills gated by `skill_new_confidence_threshold` and phrase-quality checks only
  - Rescores when `rescore AND job_in_active_rescore_scope(row) AND (skills_changed OR first_materialize)`
  - `first_materialize=True` when the job had no cached `job_skills` before extraction (covers keyword-only score when LLM returns no skills)
  - Batch scope via `get_jobs_for_active_rescore` (unviewed, applied, interview stages)
  - A classification pass (`get_job_skills_for_jobs`) drops missing ids and `skip_cached` hits before the loop; only slow rows are timed, counted, and stored. Optional `on_progress(checked, total, updated[, eta_s])` and `on_status_message(str)` run after each slow row; 3-arg callbacks still work (`TypeError` fallback). `checked`/`total` are that slow population. The sidecar `{db_path}.skills_eta.json` loads only `version == 2`, `kind == "slow"`. Rate is the mean of the last 8 in-run slow samples after 3, else the mean of up to 32 stored slow samples when that list has at least 3. The GUI line is the base sentence, plus `Estimated time left: …` only when that rate exists. `progress_label` stdout stays every 25 slow rows and stays silent on skips. Old `{total_seconds, count}` aggregates are ignored. The live file was reset because sample meaning changed. Sync pipelines (`gui_sync` / `process-inbox`) pass `progress_label=""` when `on_progress` is wired so console cadence comes from `sync_log` only; enrichment-only/dashboard paths may still use a non-empty label.
- `progress_eta.py` — slow-sample ETA, compact `format_duration` for sync.log, customer GUI stage lines via `format_skills_stage_message` / `format_descriptions_stage_message` (`SKILLS_STAGE_MESSAGE`, `DESCRIPTIONS_STAGE_MESSAGE`; separate sidecars). Customer lines have no percent. Both histories are slow-sample lists; old `{total_seconds, count}` aggregates do not load.
- `job_easy_apply.py` — `_is_easy_apply_item`

**API (import from `job_enrichment` facade):**
- `make_translate_job_entry_for_storage(runtime_profile, text_translation_cache, title_translation_cache) -> Callable[[dict], dict]`
- `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
- `_generate_missing_descriptions_for_ingest`
- Description/summary helpers used by inbox and dashboard flows

**Translation cache ownership:**
- `text_translation_cache` and `title_translation_cache` are caller-owned, invocation-scoped dictionaries.
- The factory reuses those dictionaries across records within one ingest run; callers decide lifecycle and must not treat them as module-level globals.
