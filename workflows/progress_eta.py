"""Rolling per-position timing and ETA helpers for sync progress UX."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

SKILLS_STAGE_MESSAGE = "Materializing skills and rescoring jobs"
DESCRIPTIONS_STAGE_MESSAGE = "Generating missing descriptions"

# Collapse when the sample grows large so history still influences but storage stays bounded.
_DEFAULT_COLLAPSE_ABOVE = 2000
_DEFAULT_COLLAPSE_TO = 1000
# Prefer this-run average once we have a few samples; else fall back to persisted history.
_MIN_RUN_SAMPLES_FOR_ETA = 3


@dataclass
class RollingTimeAverage:
    """Accumulate total seconds + count; collapse when count grows large."""

    total_seconds: float = 0.0
    count: int = 0
    collapse_above: int = _DEFAULT_COLLAPSE_ABOVE
    collapse_to: int = _DEFAULT_COLLAPSE_TO

    def record(self, seconds: float, n: int = 1) -> None:
        if n <= 0:
            return
        self.total_seconds += max(0.0, float(seconds))
        self.count += int(n)
        self._maybe_collapse()

    def _maybe_collapse(self) -> None:
        if self.count <= self.collapse_above:
            return
        avg = self.average
        if avg is None:
            self.total_seconds = 0.0
            self.count = 0
            return
        self.count = self.collapse_to
        self.total_seconds = avg * self.collapse_to

    @property
    def average(self) -> Optional[float]:
        if self.count <= 0:
            return None
        return self.total_seconds / self.count


def skills_eta_store_path(db_path: str) -> str:
    """Sidecar JSON next to the jobs DB (not profile — high-churn runtime stats)."""
    return f"{os.path.abspath(db_path)}.skills_eta.json"


def descriptions_eta_store_path(db_path: str) -> str:
    """Sidecar JSON for description-generation wall times (separate from skills)."""
    return f"{os.path.abspath(db_path)}.descriptions_eta.json"


def load_rolling_average(path: str) -> RollingTimeAverage:
    avg = RollingTimeAverage()
    if not path or not os.path.isfile(path):
        return avg
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return avg
    if not isinstance(data, dict):
        return avg
    try:
        avg.total_seconds = max(0.0, float(data.get("total_seconds", 0) or 0))
        avg.count = max(0, int(data.get("count", 0) or 0))
    except (TypeError, ValueError):
        return RollingTimeAverage()
    avg._maybe_collapse()
    return avg


def save_rolling_average(path: str, avg: RollingTimeAverage) -> None:
    if not path:
        return
    payload = {
        "total_seconds": float(avg.total_seconds),
        "count": int(avg.count),
    }
    abs_path = os.path.abspath(path)
    parent = os.path.dirname(abs_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{abs_path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp_path, abs_path)


def format_duration(seconds: float) -> str:
    """Human duration for ETA / elapsed (matches description-progress style)."""
    secs = max(0, int(round(float(seconds))))
    mins, secs = divmod(secs, 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs}h {mins}m {secs}s"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def format_eta_minutes_left(seconds: float) -> str:
    """Customer-facing ETA in whole minutes (or “less than a minute”)."""
    secs = max(0.0, float(seconds))
    if secs < 60:
        return "less than a minute"
    mins = int(round(secs / 60.0))
    if mins < 1:
        return "less than a minute"
    if mins == 1:
        return "1 minute"
    return f"{mins} minutes"


def estimate_remaining_seconds(
    *,
    remaining: int,
    run_total_seconds: float,
    run_count: int,
    historical: RollingTimeAverage,
    min_run_samples: int = _MIN_RUN_SAMPLES_FOR_ETA,
) -> Optional[float]:
    """ETA for remaining positions from this-run avg (preferred) or persisted history."""
    remaining = max(0, int(remaining))
    if remaining == 0:
        return 0.0
    avg: Optional[float] = None
    if run_count >= min_run_samples and run_count > 0:
        avg = run_total_seconds / run_count
    elif historical.average is not None:
        avg = historical.average
    elif run_count > 0:
        avg = run_total_seconds / run_count
    if avg is None:
        return None
    return remaining * avg


def format_skills_stage_message(
    *,
    checked: int,
    total: int,
    eta_s: Optional[float],
    base: str = SKILLS_STAGE_MESSAGE,
) -> str:
    """GUI / stage status line: base message plus optional pct and ETA.

    Examples:
    - ``Materializing skills and rescoring jobs — 25% done. Estimated time left: 3 minutes.``
    - Cold start (no ETA yet): ``… — 25% done.``
    """
    if total <= 0:
        return base
    pct = (100.0 * checked) / total
    # Integer pct when whole; one decimal otherwise (matches sync_log pct feel).
    if abs(pct - round(pct)) < 0.05:
        pct_label = f"{int(round(pct))}%"
    else:
        pct_label = f"{pct:.1f}%"
    message = f"{base} — {pct_label} done."
    if eta_s is not None and checked < total:
        message += f" Estimated time left: {format_eta_minutes_left(eta_s)}."
    return message


def format_descriptions_stage_message(
    *,
    checked: int,
    total: int,
    eta_s: Optional[float],
    base: str = DESCRIPTIONS_STAGE_MESSAGE,
) -> str:
    """GUI / stage status line for description generation (same shape as skills)."""
    return format_skills_stage_message(
        checked=checked, total=total, eta_s=eta_s, base=base
    )
