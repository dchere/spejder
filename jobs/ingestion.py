from collections.abc import Callable
from typing import Optional

from spejder.db import upsert_job
from spejder.jobs.parsing.core import extract_job_entries


def ingest_docs_to_db(
    db_path: str,
    docs: list[dict],
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    positions_by_file: list[dict[str, object]] = []
    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        entries = extract_job_entries(doc)
        file_found = 0
        file_inserted = 0
        file_skipped = 0
        for entry in entries:
            if not entry.get("position_link"):
                continue
            if entry_transform is not None:
                entry = entry_transform(dict(entry))
            file_found += 1
            is_new_record = upsert_job(db_path, entry)
            if is_new_record and on_new_record:
                on_new_record()
            if is_new_record:
                inserted_new += 1
                file_inserted += 1
            else:
                skipped_existing += 1
                file_skipped += 1
            processed += 1
            if on_progress:
                on_progress(processed, inserted_new, skipped_existing)
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(file_found),
                "inserted_new": int(file_inserted),
                "skipped_existing": int(file_skipped),
            }
        )
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }

