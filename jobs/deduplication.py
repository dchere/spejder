from datetime import datetime, timezone

from spejder.db.deduplication_utils import (
    DEDUPE_SNIPPET_MARKER,
    DEFAULT_RAW_TEXT_SIMILARITY,
    RAW_TEXT_MAX_CHARS,
    _cross_source_dedupe_key,
    _keeper_sort_key,
    _merge_duplicate_into_keeper,
    _merge_raw_text,
    _normalize_company_key,
    _normalize_title_key,
    _position_dedupe_key,
)


def _row_to_dedupe_item(row: tuple) -> dict:
    return {
        "id": int(row[0] or 0),
        "source": str(row[1] or ""),
        "company": str(row[2] or ""),
        "title": str(row[3] or ""),
        "place": str(row[4] or ""),
        "work_type": str(row[5] or ""),
        "position_link": str(row[6] or ""),
        "raw_text": str(row[7] or ""),
        "viewed": int(row[8] or 0),
        "applied": int(row[9] or 0),
        "created_at": str(row[10] or ""),
    }


def merge_duplicate_positions(db_path: str) -> dict[str, int]:
    from spejder.db.mutations import batch_update_and_delete_jobs
    from spejder.db.queries_signals import get_all_jobs_for_dedupe

    rows = get_all_jobs_for_dedupe(db_path)
    by_key: dict[str, list[dict]] = {}
    for row in rows:
        item = _row_to_dedupe_item(row)
        key = _position_dedupe_key(item["company"], item["title"], item["place"])
        if not key:
            continue
        by_key.setdefault(key, []).append(item)

    list_to_delete: set[int] = set()
    pending_updates: dict[int, dict] = {}
    merged_groups = 0

    for items in by_key.values():
        if len(items) < 2:
            continue
        merged_groups += 1
        items.sort(key=_keeper_sort_key)
        keep = dict(items[0])
        for duplicate in items[1:]:
            _merge_duplicate_into_keeper(keep, duplicate)
        pending_updates[keep["id"]] = keep
        list_to_delete.update({item["id"] for item in items[1:]})

    now = datetime.now(timezone.utc).isoformat()
    update_tuples = [
        (
            u["company"],
            u["title"],
            u["place"],
            u["work_type"],
            u["raw_text"],
            u["viewed"],
            u["applied"],
            now,
            job_id,
        )
        for job_id, u in pending_updates.items()
    ]

    batch_update_and_delete_jobs(db_path, update_tuples, list(list_to_delete))
    return {
        "rows_deleted": len(list_to_delete),
        "rows_updated": len(update_tuples),
        "groups_merged": merged_groups,
    }


def merge_cross_source_duplicates(db_path: str) -> dict[str, int]:
    """Deprecated alias for merge_duplicate_positions."""
    return merge_duplicate_positions(db_path)
