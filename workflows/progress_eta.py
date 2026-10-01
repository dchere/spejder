"""Slow-sample timing and ETA helpers for sync progress UX."""

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
# Prefer this-run window once we have a few slow samples; else fall back to persisted history.
_MIN_RUN_SAMPLES_FOR_ETA = 3
_ETA_WINDOW = 8
_SLOW_HISTORY_CAP = 32
_SLOW_STORE_VERSION = 2
_SLOW_STORE_KIND = "slow"


@dataclass
class RollingTimeAverage:
    """In-memory total seconds + count. Persistence is the slow-sample list, not this shape."""

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


def load_slow_samples(path: str) -> list[float]:
    """Load a versioned slow-sample sidecar.

    Returns ``[]`` when the file is missing, corrupt, or is not
    ``version == 2`` and ``kind == "slow"`` (including an old
    ``{total_seconds, count}`` aggregate).
    """
    if not path or not os.path.isfile(path):
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return []
    if not isinstance(data, dict):
        return []
    if data.get("version") != _SLOW_STORE_VERSION or data.get("kind") != _SLOW_STORE_KIND:
        return []
    raw = data.get("samples")
    if not isinstance(raw, list):
        return []
    samples: list[float] = []
    for item in raw:
        try:
            samples.append(max(0.0, float(item)))
        except (TypeError, ValueError):
            continue
    return samples[-_SLOW_HISTORY_CAP:]


def save_slow_samples(path: str, samples: list[float]) -> None:
    """Persist up to the last 32 slow-work durations. Replaces any older aggregate."""
    if not path:
        return
    trimmed = [float(sample) for sample in samples[-_SLOW_HISTORY_CAP:]]
    payload = {
        "version": _SLOW_STORE_VERSION,
        "kind": _SLOW_STORE_KIND,
        "samples": trimmed,
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


def historical_from_slow_samples(samples: list[float]) -> RollingTimeAverage:
    """Mean of a loaded slow-sample list, or empty when fewer than 3 samples."""
    if len(samples) < _MIN_RUN_SAMPLES_FOR_ETA:
        return RollingTimeAverage()
    return RollingTimeAverage(total_seconds=float(sum(samples)), count=len(samples))


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
    samples: list[float],
    historical: RollingTimeAverage,
    min_run_samples: int = _MIN_RUN_SAMPLES_FOR_ETA,
) -> Optional[float]:
    """Expected seconds left for remaining slow work.

    Rate is the mean of the last 8 in-run samples once there are at least
    ``min_run_samples``. Otherwise the historical mean, when that prior has
    at least ``min_run_samples``. ``None`` when remaining is 0 or no rate exists.
    """
    remaining = max(0, int(remaining))
    if remaining <= 0:
        return None
    avg: Optional[float] = None
    if len(samples) >= min_run_samples:
        window = samples[-_ETA_WINDOW:]
        avg = sum(window) / len(window)
    elif historical.count >= min_run_samples and historical.average is not None:
        avg = historical.average
    if avg is None:
        return None
    return remaining * avg


def format_skills_stage_message(
    *,
    eta_s: Optional[float],
    base: str = SKILLS_STAGE_MESSAGE,
) -> str:
    """GUI / stage status line: base sentence plus optional minutes left.

    Examples:
    - ``Materializing skills and rescoring jobs. Estimated time left: 3 minutes.``
    - No usable ETA: ``Materializing skills and rescoring jobs``
    """
    if eta_s is not None and eta_s > 0:
        return f"{base}. Estimated time left: {format_eta_minutes_left(eta_s)}."
    return base


def format_descriptions_stage_message(
    *,
    eta_s: Optional[float],
    base: str = DESCRIPTIONS_STAGE_MESSAGE,
) -> str:
    """GUI / stage status line for description generation (same shape as skills)."""
    return format_skills_stage_message(eta_s=eta_s, base=base)
