"""Sort helpers for dashboard job lists."""

from datetime import datetime


def _updated_at_desc_sort_key(item: dict) -> tuple[int, float]:
    """Match SQL ``updated_at DESC`` (empty/invalid last)."""
    s = str(item.get("updated_at", "") or "").strip()
    if not s:
        return (1, 0.0)
    try:
        ts = datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        return (0, -ts)
    except ValueError:
        return (1, 0.0)


def _sort_positions_unviewed_then_score(items: list[dict]) -> list[dict]:
    """Unviewed first, then score DESC, updated_at DESC, id ASC (SQL triage parity)."""

    def _key(item: dict):
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        job_id = int(item.get("id", 0) or 0)
        return (viewed, -score, _updated_at_desc_sort_key(item), job_id)

    return sorted(list(items), key=_key)


def slice_triage_by_non_hidden_limit(items: list[dict], limit: int) -> list[dict]:
    """Walk-slice triage cards: only non-hidden count toward L.

    Soft-hidden encountered before the L-th non-hidden are included; stop after
    L non-hidden (or list end). Soft-hidden never consume L.
    """
    capped = max(1, int(limit or 7))
    out: list[dict] = []
    non_hidden = 0
    for item in items:
        is_hidden = int(item.get("hidden", 0) or 0) == 1
        if is_hidden:
            out.append(item)
            continue
        if non_hidden >= capped:
            break
        out.append(item)
        non_hidden += 1
        if non_hidden >= capped:
            break
    return out


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
    """applied_at DESC (missing last), updated_at DESC (missing last), id ASC."""

    def _key(item: dict):
        job_id = int(item.get("id", 0) or 0)
        return (
            _applied_at_desc_sort_key(item),
            _updated_at_desc_sort_key(item),
            job_id,
        )

    return sorted(list(items), key=_key)
