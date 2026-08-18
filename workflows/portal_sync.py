"""Sync external job portals into the SQLite jobs table."""

from __future__ import annotations

from collections.abc import Callable
from typing import Optional

from spejder.db import ensure_db
from spejder.jobs.ingestion import ingest_entries_to_db
from spejder.parsers.itday_portal import fetch_itday_portal_entries


def sync_itday_portal(
    db_path: str,
    *,
    entry_transform: Optional[Callable[[dict], dict]] = None,
) -> dict[str, object]:
    ensure_db(db_path)
    try:
        entries = fetch_itday_portal_entries()
    except RuntimeError as exc:
        print(f"IT-DAY portal sync skipped: {exc}")
        return {
            "processed": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "found": 0,
            "error": str(exc),
        }

    stats = ingest_entries_to_db(
        db_path,
        entries,
        entry_transform=entry_transform,
    )
    stats["found"] = len(entries)
    print(
        "IT-DAY portal sync: "
        f"found={stats.get('found', 0)}, "
        f"inserted_new={stats.get('inserted_new', 0)}, "
        f"skipped_existing={stats.get('skipped_existing', 0)}"
    )
    return stats
