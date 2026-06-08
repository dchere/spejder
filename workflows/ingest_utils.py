import os

MAX_INGEST_FILE_STATS_LINES = 10


def delete_processed_inbox_files(ingest_stats: dict, inbox_root: str = "") -> dict[str, int]:
    rows = ingest_stats.get("positions_by_file") or []
    if not isinstance(rows, list) or not rows:
        return {"eligible": 0, "deleted": 0, "missing": 0, "failed": 0}

    root = os.path.abspath(inbox_root) if inbox_root else ""
    eligible = 0
    deleted = 0
    missing = 0
    failed = 0

    for row in rows:
        found = int(row.get("found", 0) or 0)
        file_path = str(row.get("file", "") or "").strip()
        if found <= 0 or not file_path:
            continue

        abs_path = os.path.abspath(file_path)
        if root:
            try:
                if os.path.commonpath([abs_path, root]) != root:
                    continue
            except ValueError:
                continue

        eligible += 1

        if not os.path.exists(abs_path):
            missing += 1
            continue
        if not os.path.isfile(abs_path):
            failed += 1
            continue

        try:
            os.remove(abs_path)
            deleted += 1
        except OSError:
            failed += 1

    return {
        "eligible": int(eligible),
        "deleted": int(deleted),
        "missing": int(missing),
        "failed": int(failed),
    }


def print_ingest_file_stats(ingest_stats: dict) -> None:
    rows = ingest_stats.get("positions_by_file") or []
    if not isinstance(rows, list) or not rows:
        return

    print("Positions found by file:")
    for shown, row in enumerate(rows):
        if shown >= MAX_INGEST_FILE_STATS_LINES:
            remaining = len(rows) - shown
            print(f"  ... and {remaining} more files")
            break
        file_path = str(row.get("file", "") or "")
        file_label = file_path if file_path else "(unknown file)"
        found = int(row.get("found", 0) or 0)
        inserted = int(row.get("inserted_new", 0) or 0)
        skipped = int(row.get("skipped_existing", 0) or 0)
        print(
            f"  - {file_label}: found={found}, inserted_new={inserted}, skipped_existing={skipped}"
        )
