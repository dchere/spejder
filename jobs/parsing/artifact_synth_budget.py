"""Per-host daily cap for career-alert LLM synth attempts."""

from __future__ import annotations

import json
import logging
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.jobs.parsing.artifact_store import resolve_overlay_dir
from spejder.jobs.parsing.links import unwrap_track_link

logger = logging.getLogger(__name__)

_BUDGET_FILENAME = ".synth_budget.json"
_HOST_KEY_RE = re.compile(r"[^a-z0-9]+")


def host_budget_key(host: str) -> str:
    """Normalize a hostname into a stable budget key."""
    cleaned = _HOST_KEY_RE.sub("", (host or "").lower())
    return cleaned or "unknown"


def primary_host_from_html(html_text: str, links: Optional[list[str]] = None) -> str:
    """Most common unwrapped link host in the digest (fallback: unknown)."""
    counts: Counter[str] = Counter()
    for raw in links or []:
        host = urlparse(unwrap_track_link(str(raw or ""))).netloc or ""
        if host:
            counts[host_budget_key(host)] += 1
    if html_text:
        soup = BeautifulSoup(html_text, "html.parser")
        for anchor in soup.find_all("a", href=True):
            host = urlparse(unwrap_track_link(str(anchor.get("href") or ""))).netloc or ""
            if host:
                counts[host_budget_key(host)] += 1
    if not counts:
        return "unknown"
    return counts.most_common(1)[0][0]


def _budget_path(overlay_dir: Optional[str]) -> str:
    return os.path.join(resolve_overlay_dir(overlay_dir), _BUDGET_FILENAME)


def _utc_day(now: Optional[datetime] = None) -> str:
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _load_budget(path: str) -> dict:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("unreadable synth budget %s: %s", path, exc)
        return {}
    return data if isinstance(data, dict) else {}


def _save_budget(path: str, data: dict) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, path)


def remaining_synth_attempts(
    overlay_dir: Optional[str],
    host_key: str,
    *,
    max_per_day: int,
    now: Optional[datetime] = None,
) -> int:
    """How many LLM synth attempts remain for host_key today. ``max_per_day<=0`` → unbounded."""
    if max_per_day <= 0:
        return 10**9
    key = host_budget_key(host_key)
    day = _utc_day(now)
    data = _load_budget(_budget_path(overlay_dir))
    used = int(((data.get(day) or {}) if isinstance(data.get(day), dict) else {}).get(key) or 0)
    return max(0, int(max_per_day) - used)


def consume_synth_attempt(
    overlay_dir: Optional[str],
    host_key: str,
    *,
    max_per_day: int,
    now: Optional[datetime] = None,
) -> tuple[bool, str]:
    """
    Reserve one LLM synth attempt for ``host_key`` on the UTC day.

    Returns ``(ok, reason)``. ``max_per_day <= 0`` disables the cap.
    """
    if max_per_day <= 0:
        return True, "ok"
    key = host_budget_key(host_key)
    if key == "unknown":
        # Still count unknowns together so empty digests cannot burn the LLM unbounded.
        pass
    day = _utc_day(now)
    path = _budget_path(overlay_dir)
    data = _load_budget(path)
    # Keep only today + yesterday to bound file growth.
    keep_days = {day}
    try:
        yday = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        keep_days.add((yday - timedelta(days=1)).strftime("%Y-%m-%d"))
    except ValueError:
        pass
    pruned = {k: v for k, v in data.items() if k in keep_days and isinstance(v, dict)}
    day_map = dict(pruned.get(day) or {})
    used = int(day_map.get(key) or 0)
    if used >= int(max_per_day):
        return False, "host_budget"
    day_map[key] = used + 1
    pruned[day] = day_map
    try:
        _save_budget(path, pruned)
    except OSError as exc:
        logger.warning("synth budget persist failed: %s", exc)
        # Fail open on I/O so a bad overlay dir does not block learning entirely.
        return True, "ok"
    return True, "ok"
