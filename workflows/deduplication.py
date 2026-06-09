from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import ensure_db
from spejder.jobs import merge_cross_source_duplicates


def run_cross_source_dedupe(
    db_path: str,
    *,
    log_prefix: str = "Job dedupe complete",
    include_db: bool = False,
) -> dict[str, int]:
    result = merge_cross_source_duplicates(db_path)
    db_suffix = f", db={db_path}" if include_db else ""
    print(
        f"{log_prefix}: "
        f"groups_merged={result.get('groups_merged', 0)}, "
        f"rows_updated={result.get('rows_updated', 0)}, "
        f"rows_deleted={result.get('rows_deleted', 0)}"
        f"{db_suffix}"
    )
    return result


def dedupe_jobs(profile: str = None, db: str = None):
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"

    ensure_db(db_path)
    run_cross_source_dedupe(db_path, log_prefix="Job dedupe complete", include_db=True)



