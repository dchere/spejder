"""Sync blocked skills into LLM antipatterns via synthetic job validation."""

import os
from typing import Optional

from spejder.config import (
    AppConfig,
    SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
    coerce_skill_antipattern_good_skills_count,
)
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import delete_skill_from_db
from spejder.llm import LocalLLM
from spejder.managers.profile_manager import _save_profile

from .antipattern_synthesis import (
    ANTIPATTERN_PROMPT_INPUT_MAX,
    SYNTHESIS_PATTERN_COUNT,
    _blocked_skills_for_synthesis,
    _merge_antipatterns,
    _remove_from_blocked_skills,
    _synthesize_antipatterns_via_llm,
)
from .antipattern_validation import (
    VALIDATION_RUNS,
    _generate_synthetic_job_posting,
    _match_blocked_skills_for_antipattern,
    _top_position_skills,
    _validate_antipattern_candidate,
)
from .normalization import _normalize_skill_name

SYNC_MIN_BLOCKED = 15


def _save_antipattern_sync_profile(
    profile_path: str,
    sync_profile: AppConfig,
    loaded_mtime: float,
) -> bool:
    if not profile_path or not os.path.exists(profile_path):
        return False
    if os.path.getmtime(profile_path) != loaded_mtime:
        return False

    disk_profile = load_runtime_profile(profile_path)
    disk_profile.skill_extraction_antipatterns = list(
        sync_profile.skill_extraction_antipatterns or []
    )
    disk_profile.blocked_skills = list(sync_profile.blocked_skills or [])
    _save_profile(profile_path, disk_profile)
    return True


def should_sync_skill_antipatterns(profile: AppConfig, llm: Optional[LocalLLM] = None) -> bool:
    if llm is None:
        return False
    return len(profile.blocked_skills or []) >= SYNC_MIN_BLOCKED


def _pattern_count(profile: AppConfig) -> int:
    return int(getattr(profile, "skill_antipattern_synthesis_count", SYNTHESIS_PATTERN_COUNT) or SYNTHESIS_PATTERN_COUNT)


def _validation_runs(profile: AppConfig) -> int:
    return int(getattr(profile, "skill_antipattern_validation_runs", VALIDATION_RUNS) or VALIDATION_RUNS)


def _good_skills_count(profile: AppConfig) -> int:
    # model_construct skips validators; re-coerce defensively.
    value = getattr(profile, "skill_antipattern_good_skills_count", None)
    coerced = coerce_skill_antipattern_good_skills_count(value)
    if coerced is None:
        return SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT
    return coerced


def _skip_candidate_result(
    rule: str,
    reason: str,
    matched_blocked: Optional[list[str]] = None,
) -> dict:
    matched_keys: list[str] = []
    for item in matched_blocked or []:
        if normalized := _normalize_skill_name(str(item)):
            matched_keys.append(normalized.lower())
    matched_keys.sort()
    return {
        "rule": _normalize_skill_name(str(rule)),
        "accepted": False,
        "skip_reason": reason,
        "matched_blocked": matched_keys,
        "baseline_blocked": [],
        "with_blocked": [],
        "pruned_blocked": [],
    }


def _log_candidate_result(result: dict) -> None:
    print(
        "Antipattern sync candidate: "
        f'rule="{result.get("rule", "")}", '
        f"accepted={result.get('accepted', False)}, "
        f"reason={result.get('skip_reason', '') or 'accepted'}, "
        f"matched_blocked={len(result.get('matched_blocked', []))}, "
        f"baseline_blocked={len(result.get('baseline_blocked', []))}, "
        f"with_blocked={len(result.get('with_blocked', []))}, "
        f"pruned={len(result.get('pruned_blocked', []))}"
    )


def sync_skill_extraction_antipatterns(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    profile_path: str = None,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """Distill blocked skills into prompt antipatterns using synthetic job validation."""
    stats = {
        "skipped": False,
        "skip_reason": "",
        "committed": False,
        "blocked_input": 0,
        "synthesized": [],
        "synthetic_job_len": 0,
        "candidates_tested": 0,
        "candidates_accepted": 0,
        "candidates_skipped": 0,
        "candidate_results": [],
        "merged": 0,
        "would_prune_blocked": 0,
        "pruned_blocked": 0,
        "db_skill_rows_deleted": 0,
        "db_job_links_deleted": 0,
        "profile_save_skipped": False,
        "synthesis_error": False,
        "dry_run": bool(dry_run),
    }

    if not force and not should_sync_skill_antipatterns(profile, llm):
        stats["skipped"] = True
        stats["skip_reason"] = "gate_failed"
        return stats

    blocked_skills = _blocked_skills_for_synthesis(profile)
    stats["blocked_input"] = len(blocked_skills)
    if not blocked_skills:
        stats["skipped"] = True
        stats["skip_reason"] = "no_blocked_input"
        return stats

    pattern_count = _pattern_count(profile)
    validation_runs = _validation_runs(profile)
    good_skills_count = _good_skills_count(profile)
    top_skills = _top_position_skills(db_path, profile, good_skills_count)
    if not top_skills:
        stats["skipped"] = True
        stats["skip_reason"] = "no_top_skills"
        print("Antipattern sync: skipped (no_top_skills — no ranked DB skills for validation).")
        return stats

    top_skill_keys = {
        normalized.lower()
        for skill in top_skills
        if (normalized := _normalize_skill_name(skill))
    }

    try:
        synthesized = _synthesize_antipatterns_via_llm(
            llm, blocked_skills, pattern_count=pattern_count
        )
    except Exception:
        stats["synthesis_error"] = True
        stats["skipped"] = True
        stats["skip_reason"] = "synthesis_error"
        return stats

    stats["synthesized"] = synthesized
    print(f"Antipattern sync: synthesized={synthesized}")

    if not synthesized:
        stats["skipped"] = True
        stats["skip_reason"] = "synthesis_empty"
        return stats

    save_path = profile_path or DEFAULT_PROFILE_PATH
    loaded_mtime = os.path.getmtime(save_path) if os.path.exists(save_path) else 0.0
    original_antipatterns = list(profile.skill_extraction_antipatterns or [])
    original_blocked_skills = list(profile.blocked_skills or [])
    committed = False
    accepted_rules: list[str] = []
    prune_targets: list[str] = []

    try:
        working_antipatterns = list(profile.skill_extraction_antipatterns or [])
        for candidate in synthesized:
            stats["candidates_tested"] += 1
            try:
                matched_blocked = _match_blocked_skills_for_antipattern(
                    llm, candidate, blocked_skills
                )
            except Exception:
                result = _skip_candidate_result(candidate, "match_error")
                stats["candidate_results"].append(result)
                stats["candidates_skipped"] += 1
                _log_candidate_result(result)
                continue

            if not matched_blocked:
                result = _skip_candidate_result(candidate, "no_matched_blocked")
                stats["candidate_results"].append(result)
                stats["candidates_skipped"] += 1
                _log_candidate_result(result)
                continue

            if len(matched_blocked) > ANTIPATTERN_PROMPT_INPUT_MAX:
                print(
                    "Antipattern sync: matched blocked truncated "
                    f"from {len(matched_blocked)} to {ANTIPATTERN_PROMPT_INPUT_MAX}"
                )
                matched_blocked = matched_blocked[:ANTIPATTERN_PROMPT_INPUT_MAX]

            try:
                synthetic_job, job_blocked_truncated, job_good_skills_truncated = (
                    _generate_synthetic_job_posting(llm, matched_blocked, top_skills)
                )
            except Exception:
                result = _skip_candidate_result(
                    candidate, "synthetic_job_error", matched_blocked
                )
                stats["candidate_results"].append(result)
                stats["candidates_skipped"] += 1
                _log_candidate_result(result)
                continue

            stats["synthetic_job_len"] = max(stats["synthetic_job_len"], len(synthetic_job))
            if job_blocked_truncated:
                print(
                    "Antipattern sync: synthetic job blocked input truncated "
                    f"to {ANTIPATTERN_PROMPT_INPUT_MAX} phrases"
                )
            if job_good_skills_truncated:
                print(
                    "Antipattern sync: synthetic job good-skills input truncated "
                    f"to {ANTIPATTERN_PROMPT_INPUT_MAX} skills"
                )

            if not synthetic_job:
                result = _skip_candidate_result(
                    candidate, "synthetic_job_empty", matched_blocked
                )
                stats["candidate_results"].append(result)
                stats["candidates_skipped"] += 1
                _log_candidate_result(result)
                continue

            result = _validate_antipattern_candidate(
                db_path,
                profile,
                llm,
                synthetic_job,
                candidate,
                working_antipatterns,
                matched_blocked,
                good_skill_keys=top_skill_keys,
                validation_runs=validation_runs,
            )
            stats["candidate_results"].append(result)
            _log_candidate_result(result)

            if not result.get("accepted"):
                stats["candidates_skipped"] += 1
                continue

            stats["candidates_accepted"] += 1
            merged = _merge_antipatterns(profile, [str(result.get("rule", ""))])
            if merged:
                accepted_rules.extend(merged)
                working_antipatterns = list(profile.skill_extraction_antipatterns or [])
            for skill_key in result.get("pruned_blocked", []):
                for blocked in profile.blocked_skills or []:
                    if _normalize_skill_name(str(blocked)).lower() == skill_key:
                        if blocked not in prune_targets:
                            prune_targets.append(blocked)
                        break
            if dry_run:
                stats["would_prune_blocked"] += len(result.get("pruned_blocked", []))

        stats["merged"] = len(accepted_rules)
        if stats["candidates_accepted"] == 0:
            stats["skipped"] = True
            stats["skip_reason"] = "no_candidates_accepted"
            return stats

        if dry_run:
            return stats

        for skill_name in prune_targets:
            if _remove_from_blocked_skills(profile, skill_name):
                stats["pruned_blocked"] += 1

        saved = _save_antipattern_sync_profile(save_path, profile, loaded_mtime)
        if saved:
            for skill_name in prune_targets:
                delete_info = delete_skill_from_db(db_path, skill_name)
                stats["db_skill_rows_deleted"] += int(delete_info.get("skill_rows_deleted", 0))
                stats["db_job_links_deleted"] += int(delete_info.get("job_skill_links_deleted", 0))
            committed = True
            stats["committed"] = True
        else:
            stats["profile_save_skipped"] = True
            stats["merged"] = 0
            stats["pruned_blocked"] = 0
    finally:
        if not committed:
            profile.skill_extraction_antipatterns = original_antipatterns
            profile.blocked_skills = original_blocked_skills

    return stats


def sync_skill_antipatterns(
    profile: str = None,
    db: str = None,
    model: str = "",
    dry_run: bool = False,
    force: bool = False,
    llm: Optional[LocalLLM] = None,
) -> dict:
    """CLI entry: load profile/db and run antipattern sync."""
    from spejder.db import ensure_db

    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"
    ensure_db(db_path)

    if llm is None:
        model_path = model or runtime_profile.default_model or ""
        if not model_path:
            print("Antipattern sync: no model configured, skipping.")
            return {"skipped": True}
        llm = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=False)

    stats = sync_skill_extraction_antipatterns(
        db_path,
        runtime_profile,
        llm,
        profile_path=profile_path,
        dry_run=dry_run,
        force=force,
    )
    if stats.get("skipped"):
        reason = stats.get("skip_reason") or "unknown"
        print(f"Antipattern sync: skipped (reason={reason}).")
    else:
        save_skipped = ", profile_save_skipped=True" if stats.get("profile_save_skipped") else ""
        print(
            "Antipattern sync complete: "
            f"blocked_input={stats.get('blocked_input', 0)}, "
            f"synthesized={stats.get('synthesized', [])}, "
            f"synthetic_job_len={stats.get('synthetic_job_len', 0)}, "
            f"candidates_tested={stats.get('candidates_tested', 0)}, "
            f"candidates_accepted={stats.get('candidates_accepted', 0)}, "
            f"candidates_skipped={stats.get('candidates_skipped', 0)}, "
            f"merged={stats.get('merged', 0)}, "
            f"would_prune_blocked={stats.get('would_prune_blocked', 0)}, "
            f"pruned_blocked={stats.get('pruned_blocked', 0)}, "
            f"db_skill_rows_deleted={stats.get('db_skill_rows_deleted', 0)}, "
            f"db_job_links_deleted={stats.get('db_job_links_deleted', 0)}, "
            f"committed={stats.get('committed', False)}, "
            f"dry_run={stats.get('dry_run', False)}"
            f"{save_skipped}"
        )
    return stats
