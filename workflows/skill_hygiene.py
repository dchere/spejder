"""Shared skill hygiene stages for GUI sync and process-inbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from spejder.config import AppConfig
from spejder.db import cleanup_blocked_skills_from_db, cleanup_stale_low_share_skills_from_db, ensure_db
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor.bad_cloud import (
    ensure_bad_cloud_initialized,
    recalibrate_and_store_threshold,
)
from spejder.jobs import rescore_jobs_if_active
from spejder.managers.profile_manager import _remove_skill_from_profile

_PROTECTED_FLAG_FIELDS = (
    "user_skills",
    "missing_skills_suggestions",
    "unwanted_skills",
    "blocked_skills",
)

StageCallback = Callable[[str, str], None]


@dataclass(frozen=True)
class SkillHygieneResult:
    """Outcome of ``run_skill_hygiene_stages`` (blocked → stale → bad cloud)."""

    blocked_cleanup: dict
    blocked_rescored: int
    stale_cleanup: dict
    stale_rescored: int
    cloud_stats: dict
    new_threshold: float
    previous_threshold: Optional[float]
    threshold_changed: bool

    @property
    def profile_dirty(self) -> bool:
        """True when callers should persist (and GUI-reload) the runtime profile."""
        return (
            int(self.stale_cleanup.get("profile_removed", 0) or 0) > 0
            or bool(self.cloud_stats.get("seeded"))
            or bool(self.cloud_stats.get("pruned"))
            or self.threshold_changed
        )

    def blocked_needs_rebuild(self) -> bool:
        return (
            self.blocked_rescored > 0
            or int(self.blocked_cleanup.get("job_skill_links_deleted", 0) or 0) > 0
            or int(self.blocked_cleanup.get("skill_rows_deleted", 0) or 0) > 0
        )

    def stale_needs_rebuild(self) -> bool:
        return (
            self.stale_rescored > 0
            or int(self.stale_cleanup.get("job_skill_links_deleted", 0) or 0) > 0
            or int(self.stale_cleanup.get("skill_rows_deleted", 0) or 0) > 0
        )

    def cloud_needs_rebuild(self) -> bool:
        return bool(self.cloud_stats.get("pruned")) or self.threshold_changed


def _stale_cleanup_protected_keys(profile: AppConfig) -> set[str]:
    """Normalized keys from flag lists (not known_skill_patterns)."""
    protected: set[str] = set()
    for field in _PROTECTED_FLAG_FIELDS:
        values = getattr(profile, field, None) or []
        if not isinstance(values, list):
            continue
        for item in values:
            key = _normalize_skill_name_key(str(item))
            if key:
                protected.add(key)
    return protected


def run_stale_skill_cleanup(db_path: str, profile: AppConfig) -> dict:
    """Delete stale low-share DB skills and prune matching profile entries.

    Does not append ``blocked_skills`` or call bad-cloud hooks.
    """
    stats = cleanup_stale_low_share_skills_from_db(
        db_path,
        _stale_cleanup_protected_keys(profile),
    )
    deleted_names = list(stats.get("deleted_skill_names") or [])
    profile_removed = 0
    for name in deleted_names:
        info = _remove_skill_from_profile(profile, name)
        profile_removed += int(info.get("removed", 0) or 0)

    return {
        **stats,
        "profile_removed": profile_removed,
        "deleted_skill_names": deleted_names,
    }


def run_skill_hygiene_stages(
    db_path: str,
    profile: AppConfig,
    *,
    on_stage: Optional[StageCallback] = None,
) -> SkillHygieneResult:
    """Run blocked cleanup → stale cleanup → bad-cloud seed/recalibrate.

    Shared contract for GUI background sync and CLI ``process-inbox``. Stage
    order and DB/profile side effects must stay identical in both pipelines;
    callers own logging, profile save/reload, and dashboard rebuild.
    """
    if on_stage is not None:
        on_stage("blocked_skills", "Cleaning blocked skills from database")
    blocked_cleanup = cleanup_blocked_skills_from_db(
        db_path,
        list(profile.blocked_skills or []),
    )
    blocked_rescored = rescore_jobs_if_active(
        db_path,
        profile,
        list(blocked_cleanup.get("affected_job_ids", [])),
    )

    if on_stage is not None:
        on_stage("stale_skills", "Cleaning stale low-share skills")
    stale_cleanup = run_stale_skill_cleanup(db_path, profile)
    stale_rescored = rescore_jobs_if_active(
        db_path,
        profile,
        list(stale_cleanup.get("affected_job_ids", [])),
    )

    if on_stage is not None:
        on_stage("bad_cloud", "Initializing bad cloud")
    ensure_db(db_path)
    cloud_stats = ensure_bad_cloud_initialized(profile, db_path)
    previous_threshold = getattr(profile, "skill_bigram_toxicity_threshold", None)
    new_threshold = recalibrate_and_store_threshold(profile, db_path)
    threshold_changed = previous_threshold != profile.skill_bigram_toxicity_threshold

    return SkillHygieneResult(
        blocked_cleanup=blocked_cleanup,
        blocked_rescored=int(blocked_rescored or 0),
        stale_cleanup=stale_cleanup,
        stale_rescored=int(stale_rescored or 0),
        cloud_stats=cloud_stats,
        new_threshold=float(new_threshold),
        previous_threshold=previous_threshold,
        threshold_changed=threshold_changed,
    )
