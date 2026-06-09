"""Sort helpers for dashboard job lists."""

from spejder.core import MANUAL_APPLIED_RAW_MARKER


def _sort_positions_unviewed_then_score(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (viewed, -score)

    return sorted(list(items), key=_key)


def _sort_applied_positions(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        has_manual_applied_text = MANUAL_APPLIED_RAW_MARKER in str(item.get("raw_text", ""))
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (has_manual_applied_text, viewed, -score)

    return sorted(list(items), key=_key)
