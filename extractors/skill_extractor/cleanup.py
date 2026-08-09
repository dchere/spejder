"""Skill database cleanup CLI."""

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import delete_skill_from_db, ensure_db
from spejder.db import get_skill_patterns as get_db_skill_patterns
from spejder.managers.profile_manager import _block_skill_in_profile, _save_profile

from .bad_cloud import on_skills_blocked

from .filtering import _blocked_skill_keys, _protected_skill_keys, _skill_cleanup_reason
from .normalization import _normalize_skill_name
from .patterns import _ensure_skill_pattern_seed_migration


def _collect_skill_cleanup_candidates(db_path: str, profile: AppConfig) -> list[dict]:
    protected_keys = _protected_skill_keys(profile)
    blocked_keys = _blocked_skill_keys(profile)
    rows = get_db_skill_patterns(db_path, enabled_only=False)
    candidates = []
    seen = set()

    for row in rows:
        name = _normalize_skill_name(str(row.get("name", "")))
        key = name.lower()
        if not key or key in seen or key in blocked_keys:
            continue
        seen.add(key)

        reason = _skill_cleanup_reason(name, str(row.get("source", "")), protected_keys)
        if not reason:
            continue

        candidates.append(
            {
                "name": name,
                "reason": reason,
                "source": str(row.get("source", "")),
                "occurrences": int(row.get("occurrences", 0) or 0),
                "weight": float(row.get("weight", 0.0) or 0.0),
            }
        )

    candidates.sort(key=lambda item: (-item["occurrences"], -item["weight"], item["name"]))
    return candidates


def cleanup_skills(profile: str = None, db: str = None, limit: int = 0, dry_run: bool = False):
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)

    candidates = _collect_skill_cleanup_candidates(db_path, runtime_profile)
    if limit and int(limit) > 0:
        candidates = candidates[: int(limit)]

    if not candidates:
        print("Skill cleanup: nothing to remove.")
        return

    print(f"Skill cleanup: found {len(candidates)} candidate skills")
    for item in candidates[:20]:
        print(
            f"  - {item['name']} [{item['reason']}; source={item['source']}; "
            f"occurrences={item['occurrences']}]"
        )
    if len(candidates) > 20:
        print(f"  ... and {len(candidates) - 20} more")

    if dry_run:
        print("Skill cleanup: dry run only, no changes applied.")
        return

    blocked_added = 0
    profile_removed = 0
    db_skill_rows_deleted = 0
    db_job_links_deleted = 0
    newly_blocked: list[str] = []

    for item in candidates:
        skill_name = item["name"]
        block_info = _block_skill_in_profile(runtime_profile, skill_name)
        delete_info = delete_skill_from_db(db_path, skill_name)
        blocked_added += int(block_info.get("blocked_added", 0))
        profile_removed += int(block_info.get("removed", 0))
        if block_info.get("blocked_added"):
            newly_blocked.append(skill_name)
        db_skill_rows_deleted += int(delete_info.get("skill_rows_deleted", 0))
        db_job_links_deleted += int(delete_info.get("job_skill_links_deleted", 0))

    if newly_blocked:
        on_skills_blocked(runtime_profile, db_path, newly_blocked)

    _save_profile(profile_path, runtime_profile)

    print(
        "Skill cleanup complete: "
        f"blocked={blocked_added}, "
        f"profile_removed={profile_removed}, "
        f"db_skill_rows_deleted={db_skill_rows_deleted}, "
        f"db_job_links_deleted={db_job_links_deleted}, "
        f"profile={profile_path}, db={db_path}"
    )
