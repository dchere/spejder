# spejder.workflows.sync_log

**Purpose:**
Append-only sync event log for GUI background sync and `process-inbox`. Operators and agents can inspect stage timing and progress from the file and matching terminal lines without changing dashboard behavior.

**Default path:**
`{report_dir}/sync.log` (typically `./outbox/sync.log`). Constant `SYNC_LOG_NAME = "sync.log"`. Helper `default_sync_log_path(report_dir)`. No profile field; callers may pass an explicit absolute `sync_log_path` (tests / `GuiSyncContext`). Because `serve_gui` mounts `report_dir` as FastAPI `StaticFiles`, the default file is also reachable in the browser as `/sync.log` on the GUI host/port (operational stage/progress text, not secrets; default bind is often localhost).

**API:**
- `SyncRunLog.open(path, *, echo_stdout=True) -> SyncRunLog | _NullSyncRunLog` — creates parent dirs; empty path → Null (no warning); on `OSError` prints one stderr warning then returns Null (never raises into sync). Default **echoes** each event to stdout; pass `echo_stdout=False` for file-only (tests).
- `SyncRunLog(handle, path, *, echo_stdout=True, console_write=None)` — stores the echo flag; optional `console_write(line)` callable for tests (default `print(..., flush=True)`).
- `SyncRunLogLike` — Protocol covering `SyncRunLog` / `_NullSyncRunLog` for typed callers (`GuiSyncContext.sync_log`).
- `_NullSyncRunLog` — **silent sinks**: no file and no console mirror. Sync callers still attach it and wire `on_progress` / empty `progress_label` / `progress=False`, so legacy enrichment prints stay off; stage/summary prints remain. Prefer documenting this over restoring dual progress paths when open fails.
- `IngestProgressTracker` — shared gate for ingest progress: emit on `inserted_new` change or every 25 processed jobs; `note()` returns **bool**; `needs_final(processed)` so callers write a closing progress line when the last tick was not at the final count. Console follows the same emit gate via `sync_log.progress`.
- `run_start(*, source, run_id=None)` — `run_id` defaults to `uuid4().hex[:12]`.
- `stage_start(stage, message="")` — auto-`stage_end`s any open stage first.
- `stage_end(stage=None, **metrics)` — closes open stage; always includes `elapsed_s` from monotonic start.
- `progress(stage, *, checked, total, **metrics)` — writes `checked={n}/{total}` and `pct=` (one decimal) when `total > 0`. Callers must use the same unit for `checked` and `total`; ingest uses job counts with `total=0` (no pct) plus a `files=` metric.
- `note(event, **fields)` — ad-hoc event line (does not touch open-stage timing). Used for per-file ingest outcomes (`event=parse_file` with `status` / `found` / `weak_dropped` / `quality` / `synth_reason`) and quarantine summaries (`event=parse_quarantine`).
- `pipeline_end(status, message="")` — ends open stage; logs terminal pipeline status (`done` / `failed` / `skipped`).
- `run_end(status, message="")` — ends open stage if any; logs run duration; closes the file. Idempotent when already closed.

**Mid-write I/O failure:**
On `write`/`flush` `OSError`, the file sink is **soft-closed** (handle closed, further lines skip the file). Prints one stderr warning (same family as open-fail). Console mirror stays live when `echo_stdout=True`, including the failing event and later `pipeline_end` / `run_end`. Unlike open failure (Null = silent both sinks), mid-write does **not** set `closed=True` until `run_end` finishes. `echo_stdout=False` → file soft-closed + stderr warning only (no console).

**Line format:**
One field-build path (`_assemble_body` + `_format_field`). Space-separated `key=value`. Values with spaces, `=`, or `"` are double-quoted (internal `\` / `"` escaped). File line prepends UTC ISO-8601 `ts=` with `Z`; console line prepends `sync ` and **omits** `ts=`. Append `"\n"` and **`flush()` every line** (file flush + `print(..., flush=True)`). Wall-clock only in `ts=`; durations use monotonic clocks as `elapsed_s=` (three decimals).

File example:
```
ts=2026-09-22T08:01:00.123Z run=a1b2c3d4e5f6 source=gui_sync event=run_start
ts=2026-09-22T08:01:00.200Z run=a1b2c3d4e5f6 event=stage_start stage=portal message="Checking IT-DAY job portal"
ts=2026-09-22T08:01:12.450Z run=a1b2c3d4e5f6 event=stage_end stage=portal elapsed_s=12.250
ts=2026-09-22T08:02:30.000Z run=a1b2c3d4e5f6 event=progress stage=skills checked=50/200 pct=25.0 updated=12
ts=2026-09-22T08:10:00.000Z run=a1b2c3d4e5f6 event=pipeline_end status=done message="Inbox sync pipeline complete"
ts=2026-09-22T08:10:00.010Z run=a1b2c3d4e5f6 event=stage_start stage=rebuild message="Waiting for dashboard rebuild"
ts=2026-09-22T08:10:05.000Z run=a1b2c3d4e5f6 event=stage_end stage=rebuild elapsed_s=4.990
ts=2026-09-22T08:10:05.001Z run=a1b2c3d4e5f6 event=run_end status=complete elapsed_s=544.878
```

Console mirror (same facts, no `ts=`):
```
sync run=a1b2c3d4e5f6 source=gui_sync event=run_start
sync run=a1b2c3d4e5f6 event=stage_start stage=portal message="Checking IT-DAY job portal"
sync run=a1b2c3d4e5f6 event=progress stage=skills checked=50/200 pct=25.0 updated=12
sync run=a1b2c3d4e5f6 event=progress stage=ingest checked=25/0 inserted=3 skipped_existing=22 files=2
sync run=a1b2c3d4e5f6 event=stage_end stage=portal elapsed_s=12.250
sync run=a1b2c3d4e5f6 event=pipeline_end status=done message="Inbox sync pipeline complete"
sync run=a1b2c3d4e5f6 event=run_end status=complete elapsed_s=544.878
```

**Constraints:**
- No rotation, truncation, or dashboard charts.
- Callers stop inventing progress cadence or stage banners for sync events; they only call this API (non-event operational summary prints may remain).
- All format/I/O lives here — callers only invoke the API.
