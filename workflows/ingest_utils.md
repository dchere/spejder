# spejder.workflows.ingest_utils

**Purpose:**
Holds shared ingest-side utility helpers used by inbox and GUI-sync workflows.

**API:**
- `MAX_INGEST_FILE_STATS_LINES = 10` controls how many per-file ingest lines are printed.
- `PARSE_QUARANTINE_DIRNAME = "parse_quarantine"`; `default_parse_quarantine_path(report_dir)` → `{report_dir}/parse_quarantine` (must stay **outside** the inbox tree — `email_parser.load_files` walks recursively).
- `delete_processed_inbox_files(ingest_stats, inbox_root="")` deletes inbox files with matched jobs under the configured inbox root.
- `quarantine_unparsed_inbox_files(ingest_stats, *, inbox_root="", quarantine_dir="")` moves `found=0` / unparsed-status files into quarantine with a `.json` sidecar (`status`, `quality`, `synth_reason`, `artifact_ids`, …).
- `log_ingest_parse_outcomes(sync_log, ingest_stats)` writes one `event=parse_file` sync-log line per non-`ok` file (includes comma-joined `artifact_ids` when present).
- `print_ingest_file_stats(ingest_stats)` logs per-file ingest counters with truncation (includes `weak_dropped` / `status` / `quality` / `synth_reason` / `artifact_ids` when present).

`delete_processed_inbox_files` only suppresses `ValueError` from `os.path.commonpath(...)` when comparing `inbox_root` and candidate paths from incompatible roots, and `OSError` from `os.remove(...)` when deletion fails; other exceptions are not swallowed.

Files with `found=0` are never deleted by cleanup (including after a failed career-alert synth or all-weak extract). Successful synth that yields **strong** positions sets `found>0` and then follows the normal cleanup rule. Unparsed files are quarantined under `{report_dir}/parse_quarantine` so they leave the inbox walk.

**Context:**
Extracted from `inbox_workflow.py` so ingestion reporting and cleanup are reused consistently by both `process_inbox` and `run_inbox_sync`.
