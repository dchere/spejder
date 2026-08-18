# spejder.workflows.inbox_workflow

**Purpose:**
Coordinates the ingestion of new job postings from the inbox folder, matching jobs against the database and profile.

**API:**
- `process_inbox`

**Related modules:**
- `spejder.workflows.ingest_utils` — per-file ingest stats + inbox file cleanup
- `spejder.workflows.inbox_report` — relevant-job LLM summaries + HTML dashboard write

**Ingest flow (ordered):**
0. Sync IT-DAY job portal (`sync_itday_portal`) — after `ensure_db` / entry transform; LLM is not required for fetch
1. Ingest docs + inbox cleanup (docs may be empty when continuing for portal/backfill)
2. Generate missing descriptions
3. Materialize skills (+ conditional rescore on skill change in active scope)
4. Summarize relevant jobs + write inbox report

Portal sync runs even when the inbox is empty. Early return only when the inbox is empty, the portal inserted zero new rows, **and** there are no missing descriptions (`get_jobs_for_description_refresh`). If the portal inserted rows (or descriptions are missing), the enrichment/report pipeline continues and still requires a model (`SystemExit` without one — existing constraint).

Scoring is change-driven: jobs are scored when skills are first materialized or change, not via a full-DB `apply_relevance` pass on each run.

**Ingest translation flow:**
- Ingest now builds `entry_transform` through `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage`.
- `process_inbox` owns invocation-scoped `text_translation_cache` and `title_translation_cache` and passes both into the shared factory before `ingest_docs_to_db`.
- `ingest_docs_to_db` receives `runtime_profile=profile` and `llm=` (created before ingest so synth can reuse the same LocalLLM). When `career_alert_synth_enabled` and a file yields zero positions, synthesis may persist an overlay artifact and re-extract before upsert; failed synth does not change delete-on-`found>0` behavior (`.eml` stays).

**Context:**
Extracted from `inbox_parser` to place workflow logic into the proper module group.
