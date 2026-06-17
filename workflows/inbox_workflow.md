# spejder.workflows.inbox_workflow

**Purpose:**
Coordinates the ingestion of new job postings from the inbox folder, matching jobs against the database and profile.

**API:**
- `process_inbox`

**Related modules:**
- `spejder.workflows.ingest_utils` — per-file ingest stats + inbox file cleanup
- `spejder.workflows.inbox_report` — relevant-job LLM summaries + HTML dashboard write

**Ingest flow (ordered):**
1. Ingest docs + inbox cleanup
2. Generate missing descriptions
3. Materialize skills (+ conditional rescore on skill change in active scope)
4. Summarize relevant jobs + write inbox report

Scoring is change-driven: jobs are scored when skills are first materialized or change, not via a full-DB `apply_relevance` pass on each run.

**Ingest translation flow:**
- Ingest now builds `entry_transform` through `spejder.workflows.job_enrichment.make_translate_job_entry_for_storage`.
- `process_inbox` owns invocation-scoped `text_translation_cache` and `title_translation_cache` and passes both into the shared factory before `ingest_docs_to_db`.

**Context:**
Extracted from `inbox_parser` to place workflow logic into the proper module group.
