# spejder.workflows.ingest_utils

**Purpose:**
Holds shared ingest-side utility helpers used by inbox and GUI-sync workflows.

**API:**
- `MAX_INGEST_FILE_STATS_LINES = 10` controls how many per-file ingest lines are printed.
- `delete_processed_inbox_files(ingest_stats, inbox_root="")` deletes inbox files with matched jobs under the configured inbox root.
- `print_ingest_file_stats(ingest_stats)` logs per-file ingest counters with truncation.

`delete_processed_inbox_files` only suppresses `ValueError` from `os.path.commonpath(...)` when comparing `inbox_root` and candidate paths from incompatible roots, and `OSError` from `os.remove(...)` when deletion fails; other exceptions are not swallowed.

**Context:**
Extracted from `inbox_workflow.py` so ingestion reporting and cleanup are reused consistently by both `process_inbox` and `run_inbox_sync`.
