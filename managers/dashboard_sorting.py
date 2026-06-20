"""Sort helpers for dashboard job lists."""

from datetime import datetime

from spejder.core import MANUAL_APPLIED_RAW_MARKER


def _applied_position_is_complete(item: dict) -> bool:
    has_manual = MANUAL_APPLIED_RAW_MARKER in str(item.get("raw_text", ""))
    has_cover_letter = bool(str(item.get("cover_letter", "") or "").strip())
    cover_requested = int(item.get("cover_letter_requested", 0) or 0) == 1
    return has_manual and (not cover_requested or has_cover_letter)


def _sort_positions_unviewed_then_score(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (viewed, -score)

    return sorted(list(items), key=_key)


def _applied_at_desc_sort_key(item: dict) -> tuple[int, float]:
    s = str(item.get("applied_at", "") or "").strip()
    if not s:
        return (1, 0.0)
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        return (0, -ts)
    except ValueError:
        return (1, 0.0)


def _sort_applied_positions(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        is_complete = _applied_position_is_complete(item)
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (is_complete, viewed, -score, _applied_at_desc_sort_key(item))

    return sorted(list(items), key=_key)
