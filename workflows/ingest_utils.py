import json
import os
import shutil
from datetime import datetime, timezone

MAX_INGEST_FILE_STATS_LINES = 10
PARSE_QUARANTINE_DIRNAME = "parse_quarantine"

# Per-file statuses that mean no strong positions were upserted.
_UNPARSED_STATUSES = frozenset({"empty", "weak", "synth_failed", "synth_empty"})


def default_parse_quarantine_path(report_dir: str) -> str:
    return os.path.join(os.path.abspath(report_dir), PARSE_QUARANTINE_DIRNAME)


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


def _unique_dest_path(directory: str, basename: str) -> str:
    candidate = os.path.join(directory, basename)
    if not os.path.exists(candidate):
        return candidate
    stem, ext = os.path.splitext(basename)
    for index in range(1, 1000):
        alt = os.path.join(directory, f"{stem}_{index}{ext}")
        if not os.path.exists(alt):
            return alt
    raise OSError(f"Could not find free quarantine name for {basename!r}")


def quarantine_unparsed_inbox_files(
    ingest_stats: dict,
    *,
    inbox_root: str = "",
    quarantine_dir: str = "",
) -> dict[str, int]:
    """Move inbox files with no strong parses into quarantine_dir + JSON sidecars.

    Quarantine must live outside the inbox tree (``load_files`` walks recursively).
    Default callers use ``{report_dir}/parse_quarantine``.
    """
    rows = ingest_stats.get("positions_by_file") or []
    if not isinstance(rows, list) or not rows or not quarantine_dir:
        return {"eligible": 0, "moved": 0, "missing": 0, "failed": 0}

    root = os.path.abspath(inbox_root) if inbox_root else ""
    dest_root = os.path.abspath(quarantine_dir)
    try:
        os.makedirs(dest_root, exist_ok=True)
    except OSError:
        return {"eligible": 0, "moved": 0, "missing": 0, "failed": 1}

    eligible = 0
    moved = 0
    missing = 0
    failed = 0
    quarantined_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for row in rows:
        if not isinstance(row, dict):
            continue
        found = int(row.get("found", 0) or 0)
        status = str(row.get("status") or "")
        file_path = str(row.get("file", "") or "").strip()
        if found > 0 or not file_path:
            continue
        if status and status not in _UNPARSED_STATUSES:
            continue
        if not status and found <= 0:
            # Legacy rows without status still quarantine on found=0.
            pass

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

        basename = os.path.basename(abs_path)
        try:
            dest_path = _unique_dest_path(dest_root, basename)
            shutil.move(abs_path, dest_path)
            artifact_ids = [
                str(item)
                for item in (row.get("artifact_ids") or [])
                if str(item).strip()
            ]
            sidecar = {
                "file": abs_path,
                "quarantined_as": dest_path,
                "quarantined_at": quarantined_at,
                "found": found,
                "weak_dropped": int(row.get("weak_dropped", 0) or 0),
                "quality": str(row.get("quality") or ""),
                "synth_reason": str(row.get("synth_reason") or ""),
                "status": status or "empty",
                "artifact_ids": artifact_ids,
            }
            sidecar_path = dest_path + ".json"
            with open(sidecar_path, "w", encoding="utf-8") as handle:
                json.dump(sidecar, handle, indent=2, sort_keys=True)
                handle.write("\n")
            moved += 1
        except OSError:
            failed += 1

    return {
        "eligible": int(eligible),
        "moved": int(moved),
        "missing": int(missing),
        "failed": int(failed),
    }


def log_ingest_parse_outcomes(sync_log, ingest_stats: dict) -> int:
    """Write one sync-log ``parse_file`` event per file with a non-ok outcome.

    Returns the number of events written. ``sync_log`` may be Null (no-op note).
    """
    if sync_log is None:
        return 0
    note = getattr(sync_log, "note", None)
    if not callable(note):
        return 0

    rows = ingest_stats.get("positions_by_file") or []
    written = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "ok")
        if status == "ok":
            continue
        file_path = str(row.get("file") or "") or "(unknown)"
        artifact_ids = [
            str(item)
            for item in (row.get("artifact_ids") or [])
            if str(item).strip()
        ]
        note(
            "parse_file",
            stage="ingest",
            file=file_path,
            status=status,
            found=int(row.get("found", 0) or 0),
            weak_dropped=int(row.get("weak_dropped", 0) or 0),
            quality=str(row.get("quality") or ""),
            synth_reason=str(row.get("synth_reason") or ""),
            artifact_ids=",".join(artifact_ids) if artifact_ids else "",
        )
        written += 1
    return written


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
        weak_dropped = int(row.get("weak_dropped", 0) or 0)
        status = str(row.get("status") or "")
        extras = ""
        if weak_dropped:
            extras += f", weak_dropped={weak_dropped}"
        if status and status != "ok":
            extras += f", status={status}"
        quality = str(row.get("quality") or "")
        if quality:
            extras += f", quality={quality}"
        synth_reason = str(row.get("synth_reason") or "")
        if synth_reason:
            extras += f", synth_reason={synth_reason}"
        artifact_ids = [
            str(item)
            for item in (row.get("artifact_ids") or [])
            if str(item).strip()
        ]
        if artifact_ids:
            extras += f", artifact_ids={','.join(artifact_ids)}"
        print(
            f"  - {file_label}: found={found}, inserted_new={inserted}, "
            f"skipped_existing={skipped}{extras}"
        )
