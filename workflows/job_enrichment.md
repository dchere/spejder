# spejder.workflows.job_enrichment

**Purpose:**
Per-job text enrichment: translation, description generation, skill materialization. `job_enrichment.py` is a **facade**; logic lives in focused submodules.

**Submodules:**
- `job_translation.py` — `make_translate_job_entry_for_storage`
- `job_text_enrichment.py` — `_enrich_raw_text_with_position_page`, `_build_title_fields`, `_resolve_title_and_place` (spaced ` - ` or trailing `i City` title suffix; not `Social- og`-style hyphens). When `place` is missing and trailing `i City` is parsed, the display title is retained in full and only `place` is filled. Skill materialize calls enrichment with `include_summary=False` and `prefer_page=True` (substantial page ≳800 chars drops listing raw that is mostly duplicated by the scrape; title presence dedupe via `text_prepend`). Description / display paths keep default `include_summary=True`. Page append reserves ≤3000 chars so end-truncation does not cut the page tail.
- `job_descriptions.py` — description LLM generation, quality checks, `_generate_missing_descriptions_for_ingest`
  - Optional `on_progress(checked, total, updated[, eta_s])` on cadence (`idx % 25 == 0 or idx == total`) in a `finally` so early `continue` still reaches the 100% tick; 3-arg callbacks still work (`TypeError` fallback). Optional `on_status_message(str)` refreshes GUI stage text with pct / ETA. When progress/status/ETA path is active, per-row wall time updates a rolling average sidecar (`{db_path}.descriptions_eta.json` via `progress_eta.py`; same collapse thresholds as skills). Sync pipelines pass `progress=False` when `on_progress` is wired so cadence is `sync_log` only.
- `job_skills_materialize.py` — `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
  - Uses `replace_job_skills`; propagates `skills_changed`
  - No per-job skill count cap; LLM novel skills gated by `skill_new_confidence_threshold` and phrase-quality checks only
  - Rescores when `rescore AND job_in_active_rescore_scope(row) AND (skills_changed OR first_materialize)`
  - `first_materialize=True` when the job had no cached `job_skills` before extraction (covers keyword-only score when LLM returns no skills)
  - Batch scope via `get_jobs_for_active_rescore` (unviewed, applied, interview stages)
  - Optional `on_progress(checked, total, updated[, eta_s])` on cadence (`idx % 25 == 0 or idx == total`) in a `finally` so early `continue` (missing id / skip_cached) still reaches the 100% tick; 3-arg callbacks still work (`TypeError` fallback). Optional `on_status_message(str)` refreshes GUI stage text with pct / ETA. When progress/status/ETA path is active, per-position wall time updates a rolling average sidecar (`{db_path}.skills_eta.json` via `progress_eta.py`; collapses after ~2000 samples to `1000 * avg`). `progress_label` stdout prints only when a row actually ran materialize (skip paths stay silent). Sync pipelines (`gui_sync` / `process-inbox`) pass `progress_label=""` when `on_progress` is wired so console cadence comes from `sync_log` only; enrichment-only/dashboard paths may still use a non-empty label.
- `progress_eta.py` — rolling time-per-position average, duration/ETA formatting, skills / descriptions stage status lines (`SKILLS_STAGE_MESSAGE`, `DESCRIPTIONS_STAGE_MESSAGE`; separate sidecars)
- `job_easy_apply.py` — `_is_easy_apply_item`

**API (import from `job_enrichment` facade):**
- `make_translate_job_entry_for_storage(runtime_profile, text_translation_cache, title_translation_cache) -> Callable[[dict], dict]`
- `materialize_job_skills`, `materialize_jobs_skills`, `materialize_relevant_and_applied_skills`
- `_generate_missing_descriptions_for_ingest`
- Description/summary helpers used by inbox and dashboard flows

**Translation cache ownership:**
- `text_translation_cache` and `title_translation_cache` are caller-owned, invocation-scoped dictionaries.
- The factory reuses those dictionaries across records within one ingest run; callers decide lifecycle and must not treat them as module-level globals.
