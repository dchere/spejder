"""Append-only sync event log for GUI and process-inbox pipelines."""

from __future__ import annotations

import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TextIO

SYNC_LOG_NAME = "sync.log"


class SyncRunLogLike(Protocol):
    """Structural type for SyncRunLog and the null no-op stand-in."""

    closed: bool

    def run_start(self, *, source: str, run_id: str | None = None) -> str: ...

    def stage_start(self, stage: str, message: str = "") -> None: ...

    def stage_end(self, stage: str | None = None, **metrics: Any) -> None: ...

    def progress(self, stage: str, *, checked: int, total: int, **metrics: Any) -> None: ...

    def pipeline_end(self, status: str, message: str = "") -> None: ...

    def run_end(self, status: str, message: str = "") -> None: ...


def default_sync_log_path(report_dir: str) -> str:
    return os.path.join(os.path.abspath(report_dir), SYNC_LOG_NAME)


class IngestProgressTracker:
    """Gate ingest sync-log ticks: insert changes, every ``milestone`` jobs, and a final line.

    Callers emit via ``sync_log.progress`` when :meth:`note` returns True (or
    :meth:`needs_final` for a closing tick). Console mirrors the same events.
    """

    __slots__ = ("last_inserted", "last_processed", "milestone")

    def __init__(self, milestone: int = 25) -> None:
        self.last_inserted = -1
        self.last_processed = 0
        self.milestone = milestone

    def note(self, processed: int, inserted_new: int) -> bool:
        """Return True when callers should emit progress (insert change or milestone)."""
        inserted_changed = inserted_new != self.last_inserted
        at_milestone = processed > 0 and processed % self.milestone == 0
        if not inserted_changed and not at_milestone:
            return False
        self.last_inserted = inserted_new
        self.last_processed = processed
        return True

    def needs_final(self, processed: int) -> bool:
        return int(processed) != self.last_processed


def _utc_ts() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(now.microsecond / 1000):03d}Z"


def _format_field(key: str, value: Any) -> str:
    if isinstance(value, float):
        text = f"{value:.3f}" if key == "elapsed_s" else f"{value:.1f}" if key == "pct" else str(value)
    else:
        text = str(value)
    if any(ch in text for ch in (" ", "=", '"')) or text == "":
        escaped = text.replace("\\", "\\\\").replace('"', '\\"')
        return f'{key}="{escaped}"'
    return f"{key}={text}"


def _default_console_write(line: str) -> None:
    print(line, flush=True)


class _NullSyncRunLog:
    """Silent sinks: no file and no console mirror.

    Returned when the path is empty or open fails. Callers that wire
    ``on_progress`` into this object intentionally suppress legacy
    ``progress_label`` / ``progress=True`` prints — Null means quiet.
    """

    closed = True

    def run_start(self, *, source: str, run_id: str | None = None) -> str:
        return run_id or uuid.uuid4().hex[:12]

    def stage_start(self, stage: str, message: str = "") -> None:
        return None

    def stage_end(self, stage: str | None = None, **metrics: Any) -> None:
        return None

    def progress(self, stage: str, *, checked: int, total: int, **metrics: Any) -> None:
        return None

    def pipeline_end(self, status: str, message: str = "") -> None:
        return None

    def run_end(self, status: str, message: str = "") -> None:
        return None


class SyncRunLog:
    """Append-only, line-oriented sync event writer with optional stdout mirror."""

    def __init__(
        self,
        handle: TextIO,
        path: str,
        *,
        echo_stdout: bool = True,
        console_write: Callable[[str], None] | None = None,
    ) -> None:
        self._handle = handle
        self.path = path
        self.run_id = ""
        self.source = ""
        self._run_mono_start = 0.0
        self._open_stage: str | None = None
        self._stage_mono_start = 0.0
        self.closed = False
        self._file_live = True
        self._echo_stdout = bool(echo_stdout)
        self._console_write = console_write or _default_console_write

    @classmethod
    def open(
        cls, path: str, *, echo_stdout: bool = True
    ) -> SyncRunLog | _NullSyncRunLog:
        if not path:
            return _NullSyncRunLog()
        try:
            abs_path = os.path.abspath(path)
            parent = os.path.dirname(abs_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            handle = open(abs_path, "a", encoding="utf-8")
            return cls(handle, abs_path, echo_stdout=echo_stdout)
        except OSError as exc:
            print(
                f"sync_log: failed to open {path!r}: {exc}; "
                "continuing with silent sinks (no file, no console progress)",
                file=sys.stderr,
                flush=True,
            )
            return _NullSyncRunLog()

    def run_start(self, *, source: str, run_id: str | None = None) -> str:
        self.run_id = run_id or uuid.uuid4().hex[:12]
        self.source = source
        self._run_mono_start = time.monotonic()
        self._open_stage = None
        self._write(
            event="run_start",
            source=source,
        )
        return self.run_id

    def stage_start(self, stage: str, message: str = "") -> None:
        if self._open_stage is not None:
            self.stage_end(self._open_stage)
        self._open_stage = stage
        self._stage_mono_start = time.monotonic()
        fields: dict[str, Any] = {"event": "stage_start", "stage": stage}
        if message:
            fields["message"] = message
        self._write(**fields)

    def stage_end(self, stage: str | None = None, **metrics: Any) -> None:
        if self._open_stage is None:
            return
        ended = stage or self._open_stage
        if ended != self._open_stage:
            ended = self._open_stage
        elapsed = time.monotonic() - self._stage_mono_start
        fields: dict[str, Any] = {
            "event": "stage_end",
            "stage": ended,
            "elapsed_s": elapsed,
        }
        fields.update(metrics)
        self._open_stage = None
        self._write(**fields)

    def progress(self, stage: str, *, checked: int, total: int, **metrics: Any) -> None:
        fields: dict[str, Any] = {
            "event": "progress",
            "stage": stage,
            "checked": f"{checked}/{total}",
        }
        if total > 0:
            fields["pct"] = (100.0 * checked) / total
        fields.update(metrics)
        self._write(**fields)

    def pipeline_end(self, status: str, message: str = "") -> None:
        if self._open_stage is not None:
            self.stage_end()
        fields: dict[str, Any] = {"event": "pipeline_end", "status": status}
        if message:
            fields["message"] = message
        self._write(**fields)

    def run_end(self, status: str, message: str = "") -> None:
        if self.closed:
            return
        if self._open_stage is not None:
            self.stage_end()
        elapsed = (
            time.monotonic() - self._run_mono_start if self._run_mono_start else 0.0
        )
        fields: dict[str, Any] = {
            "event": "run_end",
            "status": status,
            "elapsed_s": elapsed,
        }
        if message:
            fields["message"] = message
        self._write(**fields)
        self.closed = True
        self._close_file()

    def _assemble_body(self, **fields: Any) -> str:
        ordered: list[tuple[str, Any]] = []
        if self.run_id:
            ordered.append(("run", self.run_id))
        event = fields.pop("event", "")
        source = fields.pop("source", None)
        if source is not None:
            ordered.append(("source", source))
        if event:
            ordered.append(("event", event))
        for key, value in fields.items():
            if value is None:
                continue
            ordered.append((key, value))
        return " ".join(_format_field(k, v) for k, v in ordered)

    def _close_file(self) -> None:
        if not self._file_live:
            return
        self._file_live = False
        try:
            self._handle.close()
        except OSError:
            pass

    def _soft_close_file(self, exc: OSError) -> None:
        """Drop the file sink after mid-run I/O failure; console may stay live."""
        self._close_file()
        print(
            f"sync_log: failed to write {self.path!r}: {exc}; "
            "continuing with console-only progress (file closed)",
            file=sys.stderr,
            flush=True,
        )

    def _write(self, **fields: Any) -> None:
        if self.closed:
            return
        body = self._assemble_body(**fields)
        if self._file_live:
            file_line = f"{_format_field('ts', _utc_ts())} {body}\n"
            try:
                self._handle.write(file_line)
                self._handle.flush()
            except OSError as exc:
                self._soft_close_file(exc)
        if self._echo_stdout:
            self._console_write(f"sync {body}")
