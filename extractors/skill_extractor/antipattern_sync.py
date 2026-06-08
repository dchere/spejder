"""Sync blocked junk skills into LLM antipatterns and prune validated entries."""

import os
from typing import Optional

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import (
    count_job_links_for_skills,
    delete_skill_from_db,
    get_job_for_rescoring,
    get_job_ids_for_skill,
)
from spejder.llm import LocalLLM
from spejder.managers.profile_manager import _save_profile

from .extraction import _extract_job_skills_llm_path
from .filtering import _protected_skill_keys, _skill_cleanup_reason
from .normalization import _normalize_skill_name
from .utils import _extract_json_object, _to_items

SYNC_MIN_BLOCKED = 15
SYNC_MIN_DELTA = 10
SYNC_MIN_JUNK_CANDIDATES = 5
BATCH_INPUT_MAX = 120
SYNTHESIS_INPUT_MAX = 40
SYNTHESIS_MAX_TOKENS = 1024
VALIDATION_SKILLS_MAX = 30
VALIDATION_JOBS_PER_SKILL = 2
PROFILE_ANTIPATTERNS_MAX = 200
FILTER_RULES_BEFORE_MERGE = True
JOB_LINK_COUNT_LIMIT = 500


def _antipattern_keys(profile: AppConfig) -> set[str]:
    keys = set()
    for item in profile.skill_extraction_antipatterns or []:
        normalized = _normalize_skill_name(str(item))
        if normalized:
            keys.add(normalized.lower())
    return keys


def _job_link_count(
    db_path: str,
    skill_name: str,
    link_counts: Optional[dict[str, int]] = None,
) -> int:
    if link_counts is not None:
        return int(link_counts.get(skill_name, 0))
    return len(get_job_ids_for_skill(db_path, skill_name, limit=JOB_LINK_COUNT_LIMIT))


def _token_overlap_score(rule: str, candidate: str) -> int:
    rule_tokens = set(str(rule).lower().split())
    candidate_tokens = set(str(candidate).lower().split())
    return len(rule_tokens & candidate_tokens)


def _pick_probe_skill(
    rule: str,
    candidates: list[str],
    db_path: str,
    link_counts: Optional[dict[str, int]] = None,
) -> Optional[str]:
    best: Optional[str] = None
    best_score = -1
    for candidate in candidates:
        if _job_link_count(db_path, candidate, link_counts=link_counts) <= 0:
            continue
        score = _token_overlap_score(rule, candidate)
        if score > best_score:
            best_score = score
            best = candidate
    return best


def _filter_synthesized_antipatterns(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    existing: list[str],
    synthesized: list[str],
    candidates: list[str],
    link_counts: Optional[dict[str, int]] = None,
) -> tuple[list[str], dict]:
    candidate_keys = {
        _normalize_skill_name(str(item)).lower()
        for item in candidates
        if _normalize_skill_name(str(item))
    }
    kept: list[str] = []
    rules_filtered = 0

    for rule in synthesized:
        normalized = _normalize_skill_name(str(rule))
        if not normalized:
            rules_filtered += 1
            continue
        key = normalized.lower()

        if key in candidate_keys:
            kept.append(normalized)
            continue

        probe_skill = _pick_probe_skill(normalized, candidates, db_path, link_counts=link_counts)
        if not probe_skill:
            rules_filtered += 1
            continue

        if _validate_skill_filtered_by_prompt(
            db_path,
            profile,
            llm,
            probe_skill,
            antipatterns_override=existing,
        ):
            rules_filtered += 1
            continue

        if _validate_skill_filtered_by_prompt(
            db_path,
            profile,
            llm,
            probe_skill,
            antipatterns_override=existing + [normalized],
        ):
            kept.append(normalized)
        else:
            rules_filtered += 1

    return kept, {"rules_filtered": rules_filtered, "rules_kept": len(kept)}


def _select_junk_blocked_candidates(
    profile: AppConfig,
    db_path: Optional[str] = None,
    max_items: int = BATCH_INPUT_MAX,
    link_counts: Optional[dict[str, int]] = None,
) -> list[str]:
    protected_keys = _protected_skill_keys(profile)
    existing = _antipattern_keys(profile)
    candidates = []

    for item in profile.blocked_skills or []:
        name = _normalize_skill_name(str(item))
        key = name.lower()
        if not name or key in existing:
            continue
        reason = _skill_cleanup_reason(name, "blocked", protected_keys)
        if not reason:
            continue
        candidates.append(name)

    if db_path:
        candidates.sort(
            key=lambda name: _job_link_count(db_path, name, link_counts=link_counts),
            reverse=True,
        )

    if len(candidates) > max_items:
        return candidates[:max_items]
    return candidates


def _synthesize_antipatterns_via_llm(llm: LocalLLM, candidates: list[str]) -> list[str]:
    if not candidates:
        return []

    sample = candidates[:SYNTHESIS_INPUT_MAX]
    phrases_text = "\n".join(f"- {item}" for item in sample)
    prompt = (
        "Given these rejected non-skill phrases extracted from job ads, produce:\n"
        "1) rules: 10-20 short generic antipattern rules "
        "(categories: hiring narrative, pronoun-led fragments, company fluff, malformed text)\n"
        "2) examples: up to 15 representative literal phrases (max 6 words each)\n"
        "Do NOT include legitimate tools, languages, frameworks, or domain skills.\n"
        "Output strict JSON with keys rules and examples, each an array of strings.\n\n"
        f"Rejected phrases:\n{phrases_text}\n\n"
        "JSON:"
    )
    out = llm.generate(prompt, max_tokens=SYNTHESIS_MAX_TOKENS)
    parsed = _extract_json_object(out)

    merged: list[str] = []
    seen = set()
    for key in ("rules", "examples"):
        for item in _to_items(parsed.get(key)):
            if isinstance(item, str):
                text = _normalize_skill_name(item)
            elif isinstance(item, dict):
                text = _normalize_skill_name(str(item.get("name", item.get("text", ""))))
            else:
                text = _normalize_skill_name(str(item))
            item_key = text.lower()
            if not text or item_key in seen:
                continue
            seen.add(item_key)
            merged.append(text)
    return merged


def _merge_antipatterns(profile: AppConfig, new_items: list[str]) -> list[str]:
    if not new_items:
        return []

    existing = list(profile.skill_extraction_antipatterns or [])
    seen = {_normalize_skill_name(str(item)).lower() for item in existing if _normalize_skill_name(str(item))}
    added = []
    for item in new_items:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        existing.append(normalized)
        added.append(normalized)

    if len(existing) > PROFILE_ANTIPATTERNS_MAX:
        existing = existing[-PROFILE_ANTIPATTERNS_MAX:]

    profile.skill_extraction_antipatterns = existing
    return added


def _remove_from_blocked_skills(profile: AppConfig, skill_name: str) -> bool:
    target = _normalize_skill_name(skill_name).lower()
    if not target:
        return False
    blocked = profile.blocked_skills or []
    kept = []
    removed = False
    seen = set()
    for item in blocked:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        if key == target:
            removed = True
            continue
        kept.append(normalized)
    profile.blocked_skills = kept
    return removed


def _extracted_skill_keys(skills_text: str) -> set[str]:
    keys = set()
    for part in (skills_text or "").split(","):
        normalized = _normalize_skill_name(part.strip())
        if normalized:
            keys.add(normalized.lower())
    return keys


def _validate_skill_filtered_by_prompt(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    skill_name: str,
    antipatterns_override: Optional[list[str]] = None,
) -> bool:
    job_ids = get_job_ids_for_skill(db_path, skill_name, limit=VALIDATION_JOBS_PER_SKILL)
    if not job_ids:
        return False

    target_key = _normalize_skill_name(skill_name).lower()
    conclusive_llm_jobs = 0
    for job_id in job_ids:
        job = get_job_for_rescoring(db_path, job_id)
        if not job:
            continue
        raw_text = str(job.get("raw_text", "") or "")
        if not raw_text.strip():
            continue
        extracted = _extract_job_skills_llm_path(
            db_path,
            raw_text,
            llm=llm,
            profile=profile,
            skip_blocked_filter=True,
            antipatterns_override=antipatterns_override,
        )
        if extracted is None:
            continue
        conclusive_llm_jobs += 1
        if target_key in _extracted_skill_keys(extracted):
            return False
    return conclusive_llm_jobs > 0


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
    disk_profile.skill_antipattern_last_sync_blocked_count = int(
        getattr(sync_profile, "skill_antipattern_last_sync_blocked_count", 0) or 0
    )
    _save_profile(profile_path, disk_profile)
    return True


def should_sync_skill_antipatterns(profile: AppConfig, llm: Optional[LocalLLM] = None) -> bool:
    if llm is None:
        return False

    blocked_count = len(profile.blocked_skills or [])
    if blocked_count < SYNC_MIN_BLOCKED:
        return False

    last_count = int(getattr(profile, "skill_antipattern_last_sync_blocked_count", 0) or 0)
    delta = blocked_count - last_count
    if last_count > 0 and delta < SYNC_MIN_DELTA:
        return False

    junk_candidates = _select_junk_blocked_candidates(profile)
    return len(junk_candidates) >= SYNC_MIN_JUNK_CANDIDATES


def sync_skill_extraction_antipatterns(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    profile_path: str = None,
    dry_run: bool = False,
    force: bool = False,
) -> dict:
    """Distill junk blocked skills into prompt antipatterns and prune validated entries."""
    stats = {
        "skipped": False,
        "skip_reason": "",
        "junk_candidates": 0,
        "synthesized": 0,
        "rules_synthesized": 0,
        "rules_filtered": 0,
        "rules_kept": 0,
        "merged": 0,
        "validated": 0,
        "validation_targets": 0,
        "candidates_with_db_jobs": 0,
        "batch_rejected": False,
        "synthesis_error": False,
        "would_prune_blocked": 0,
        "pruned_blocked": 0,
        "db_skill_rows_deleted": 0,
        "db_job_links_deleted": 0,
        "profile_save_skipped": False,
        "dry_run": bool(dry_run),
    }

    if not force and not should_sync_skill_antipatterns(profile, llm):
        stats["skipped"] = True
        stats["skip_reason"] = "gate_failed"
        return stats

    raw_candidates = _select_junk_blocked_candidates(profile)
    link_counts = count_job_links_for_skills(db_path, raw_candidates)
    junk_candidates = _select_junk_blocked_candidates(
        profile, db_path=db_path, link_counts=link_counts
    )
    stats["junk_candidates"] = len(junk_candidates)
    stats["candidates_with_db_jobs"] = sum(
        1 for name in junk_candidates if link_counts.get(name, 0) > 0
    )
    if len(junk_candidates) < SYNC_MIN_JUNK_CANDIDATES:
        stats["skipped"] = True
        stats["skip_reason"] = "insufficient_junk_candidates"
        return stats

    try:
        synthesized = _synthesize_antipatterns_via_llm(llm, junk_candidates)
    except Exception:
        stats["synthesis_error"] = True
        stats["skipped"] = True
        stats["skip_reason"] = "synthesis_error"
        return stats
    stats["rules_synthesized"] = len(synthesized)
    stats["synthesized"] = len(synthesized)
    if not synthesized:
        stats["skipped"] = True
        stats["skip_reason"] = "synthesis_empty"
        return stats

    existing_antipatterns = list(profile.skill_extraction_antipatterns or [])
    to_merge = synthesized
    if FILTER_RULES_BEFORE_MERGE:
        to_merge, filter_stats = _filter_synthesized_antipatterns(
            db_path,
            profile,
            llm,
            existing_antipatterns,
            synthesized,
            junk_candidates,
            link_counts=link_counts,
        )
        stats["rules_filtered"] = filter_stats["rules_filtered"]
        stats["rules_kept"] = filter_stats["rules_kept"]
    else:
        stats["rules_kept"] = len(synthesized)

    if not to_merge:
        stats["skipped"] = True
        stats["skip_reason"] = "rules_filtered_empty"
        return stats

    validation_targets = junk_candidates[:VALIDATION_SKILLS_MAX]
    stats["validation_targets"] = len(validation_targets)

    save_path = profile_path or DEFAULT_PROFILE_PATH
    loaded_mtime = os.path.getmtime(save_path) if os.path.exists(save_path) else 0.0
    original_antipatterns = list(profile.skill_extraction_antipatterns or [])
    original_blocked_skills = list(profile.blocked_skills or [])
    original_last_sync_count = int(
        getattr(profile, "skill_antipattern_last_sync_blocked_count", 0) or 0
    )
    committed = False
    try:
        merged = _merge_antipatterns(profile, to_merge)
        stats["merged"] = len(merged)
        if not merged:
            stats["skipped"] = True
            stats["skip_reason"] = "merge_empty"
            return stats

        validated_skills: list[str] = []
        for skill_name in validation_targets:
            if not _validate_skill_filtered_by_prompt(db_path, profile, llm, skill_name):
                continue
            stats["validated"] += 1
            validated_skills.append(skill_name)
            if dry_run:
                stats["would_prune_blocked"] += 1

        if stats["validated"] == 0:
            stats["batch_rejected"] = True
            return stats

        if dry_run:
            return stats

        for skill_name in validated_skills:
            if _remove_from_blocked_skills(profile, skill_name):
                stats["pruned_blocked"] += 1

        profile.skill_antipattern_last_sync_blocked_count = len(profile.blocked_skills or [])
        saved = _save_antipattern_sync_profile(save_path, profile, loaded_mtime)
        if saved:
            for skill_name in validated_skills:
                delete_info = delete_skill_from_db(db_path, skill_name)
                stats["db_skill_rows_deleted"] += int(delete_info.get("skill_rows_deleted", 0))
                stats["db_job_links_deleted"] += int(delete_info.get("job_skill_links_deleted", 0))
            committed = True
        else:
            stats["profile_save_skipped"] = True
            stats["merged"] = 0
            stats["pruned_blocked"] = 0
    finally:
        if not committed:
            profile.skill_extraction_antipatterns = original_antipatterns
            profile.blocked_skills = original_blocked_skills
            profile.skill_antipattern_last_sync_blocked_count = original_last_sync_count

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
    from spejder.core import load_runtime_profile
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
        print("Antipattern sync: skipped (gate or no synthesis output).")
    else:
        prune_parts = []
        if stats.get("would_prune_blocked", 0):
            prune_parts.append(f"would_prune_blocked={stats.get('would_prune_blocked', 0)}")
        if stats.get("pruned_blocked", 0):
            prune_parts.append(f"pruned_blocked={stats.get('pruned_blocked', 0)}")
        prune_summary = ", ".join(prune_parts) if prune_parts else "pruned_blocked=0"
        save_skipped = ", profile_save_skipped=True" if stats.get("profile_save_skipped") else ""
        print(
            "Antipattern sync complete: "
            f"junk_candidates={stats.get('junk_candidates', 0)}, "
            f"candidates_with_db_jobs={stats.get('candidates_with_db_jobs', 0)}, "
            f"rules_synthesized={stats.get('rules_synthesized', 0)}, "
            f"rules_filtered={stats.get('rules_filtered', 0)}, "
            f"rules_kept={stats.get('rules_kept', 0)}, "
            f"merged={stats.get('merged', 0)}, "
            f"validation_targets={stats.get('validation_targets', 0)}, "
            f"validated={stats.get('validated', 0)}, "
            f"batch_rejected={stats.get('batch_rejected', False)}, "
            f"synthesis_error={stats.get('synthesis_error', False)}, "
            f"{prune_summary}, "
            f"db_skill_rows_deleted={stats.get('db_skill_rows_deleted', 0)}, "
            f"db_job_links_deleted={stats.get('db_job_links_deleted', 0)}, "
            f"dry_run={stats.get('dry_run', False)}"
            f"{save_skipped}"
        )
    return stats
