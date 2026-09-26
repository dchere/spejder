# spejder.workflows.inbox_workflow

**Purpose:**
Coordinates the ingestion of new job postings from the inbox folder, matching jobs against the database and profile.

**API:**
- `process_inbox`

**Related modules:**
- `spejder.workflows.ingest_utils` — per-file ingest stats + inbox file cleanup
- `spejder.workflows.inbox_report` — relevant-job LLM summaries + HTML dashboard write
- `spejder.workflows.skill_hygiene` — shared `run_skill_hygiene_stages` (blocked → stale → bad cloud) after pattern learning
- `spejder.workflows.sync_log` — append-only `{report_dir}/sync.log` (source=`process_inbox`)

**Sync log:**
Opens `SyncRunLog` at `default_sync_log_path(report_dir)` for the run (default `echo_stdout=True`, so the CLI mirrors each event as `sync …`). Wraps overlapping stages with the same ids as GUI sync (`portal`, `portal_dedupe`, `ingest`, `cleanup`, `descriptions`, `skills`, `patterns`, `blocked_skills`, `stale_skills`, `bad_cloud`) and passes progress callbacks into ingest / descriptions / materialize / pattern learning. Ingest progress uses **job** counts with `total=0` (no pct) plus `files=` via `IngestProgressTracker` (ticks on `inserted_new` change, every 25 processed jobs, and a final line when needed). Materialize / pattern learning use `progress_label=""` / `progress=False` when `on_progress` is wired (stage/progress events cover cadence). Does **not** invent `dedupe` (post-ingest) when the CLI pipeline omits that step. Early empty return logs `pipeline_end status=skipped`; success/failure use `pipeline_end` + `run_end`. Kept non-event prints: empty-inbox notice, ingest/cleanup/description/pattern/hygiene summaries, final rollups, hard failures.

**Ingest flow (ordered):**
0. Sync IT-DAY job portal (`sync_itday_portal(..., enabled=profile.itday_portal_sync_enabled)`) — after `ensure_db` / entry transform; LLM is not required for fetch; skipped when the profile flag is false
0b. When portal `inserted_new > 0`, run company+title dedupe (`run_cross_source_dedupe`) so portal duplicates of existing LinkedIn/Jobindex rows merge before inbox ingest
1. Ingest docs + inbox cleanup (docs may be empty when continuing for portal/backfill)
2. Generate missing descriptions
3. Materialize skills (+ conditional rescore on skill change in active scope)
3b. Learn skill patterns from applied/relevant positions
3c. Shared skill hygiene (`run_skill_hygiene_stages`): same contract as GUI sync — blocked DB cleanup + rescore → stale low-share cleanup + rescore → bad-cloud seed + threshold recalibrate; stages `blocked_skills` / `stale_skills` / `bad_cloud`; `save_profile` when `profile_dirty` (no dashboard queue)
3d. `update_profile_from_db_signals` (learned keywords / missing skills)
4. Summarize relevant jobs + write inbox report

Portal sync runs (when enabled) even when the inbox is empty. Early return only when the inbox is empty, the portal inserted zero new rows, **and** there are no missing descriptions (`get_jobs_for_description_refresh`). If the portal inserted rows (or descriptions are missing), the enrichment/report pipeline continues and still requires a model (`SystemExit` without one — existing constraint).

Scoring is change-driven: jobs are scored when skills are first materialized or change, not via a full-DB `apply_relevance` pass on each run.

**Ingest translation flow:**
- Ingest now builds `entry_transform` through `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage`.
- `process_inbox` owns invocation-scoped `text_translation_cache` and `title_translation_cache` and passes both into the shared factory before `ingest_docs_to_db`.
- `ingest_docs_to_db` receives `runtime_profile=profile` and `llm=` (created before ingest so synth can reuse the same LocalLLM). When `career_alert_synth_enabled` and a file yields **no strong** positions (zero extracts **or** all rows fail the extract quality gate), synthesis may persist an overlay artifact and re-extract before upsert. Only strong rows are upserted. Delete-on-`found>0` still applies to strong counts; unparsed files (`found=0`) are moved to `{report_dir}/parse_quarantine` with a JSON sidecar (not left to re-walk forever). Non-ok per-file outcomes are written to `sync.log` as `event=parse_file` via `log_ingest_parse_outcomes`.

**Context:**
Extracted from `inbox_parser` to place workflow logic into the proper module group.
