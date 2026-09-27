"""Batch skill pattern learning from applied and relevant job positions."""

from collections import Counter
from typing import Callable, Optional

from spejder.config import AppConfig
from spejder.db import (
    get_all_applied_jobs,
    get_jobs_by_category,
    reconcile_skill_pattern_learning_scores,
    upsert_skill_pattern,
)
from spejder.llm import LocalLLM

from .filtering import _blocked_skill_keys
from .job_skills_read import get_job_skills_filtered
from .normalization import _normalize_skill_name
from .patterns import _get_skill_patterns
from .utils import _skill_to_regex


def _select_learning_rows(
    applied_rows: list,
    relevant_rows: list,
    max_positions: int,
) -> list[tuple[dict, int]]:
    """Build weighted learning rows without starving relevant evidence.

    Applied positions weight 3; relevant (non-applied) weight 1. When the
    combined set exceeds ``max_positions``, reserve about one-sixth of the
    budget for relevant so a large applied set cannot drop all +1 signals.
    """
    applied: list[tuple[dict, int]] = []
    relevant: list[tuple[dict, int]] = []
    seen_ids: set[int] = set()

    for row in applied_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        applied.append((row, 3))

    for row in relevant_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        relevant.append((row, 1))

    if max_positions <= 0:
        return []
    if len(applied) + len(relevant) <= max_positions:
        return applied + relevant

    relevant_budget = min(len(relevant), max(0, max_positions // 6))
    applied_budget = max_positions - relevant_budget
    applied_take = applied[:applied_budget]
    remaining = max_positions - len(applied_take)
    relevant_take = relevant[:remaining]
    return applied_take + relevant_take


def _learn_skill_patterns_from_positions(
    db_path: str,
    runtime_profile: AppConfig,
    llm: Optional[LocalLLM] = None,
    progress: bool = False,
    progress_label: str = "Skill pattern learning",
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> dict:
    applied_rows = get_all_applied_jobs(db_path, limit=0)
    relevant_rows = get_jobs_by_category(
        db_path, "relevant", limit=0, unviewed_only=False, exclude_hidden=False
    )

    max_positions = int(runtime_profile.skill_learning_max_positions or 180)
    rows = _select_learning_rows(applied_rows, relevant_rows, max_positions)

    if not rows:
        if progress:
            print(f"{progress_label}: no applied/relevant positions found")
        # Still reconcile to zero so Learned tracks empty current evidence.
        reconcile_skill_pattern_learning_scores(db_path, {})
        return {
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": len(_get_skill_patterns(db_path, runtime_profile)),
        }

    min_occurrences = int(runtime_profile.skill_learning_min_occurrences or 3)
    max_new = int(runtime_profile.skill_learning_max_new_patterns or 20)

    blocked_keys = _blocked_skill_keys(runtime_profile)
    counts: Counter[str] = Counter()
    considered = 0

    def _skills_for_learning(skill_names: list[str]) -> list[str]:
        out = []
        for raw in skill_names:
            normalized = _normalize_skill_name(raw)
            if not normalized or normalized.lower() in blocked_keys:
                continue
            out.append(normalized)
        return out

    if progress:
        print(f"{progress_label}: starting (positions={len(rows)})")

    learn_total = len(rows)
    for row, weight in rows:
        job_id = int(row.get("id", 0) or 0)
        cached = (
            get_job_skills_filtered(db_path, job_id, runtime_profile)
            if job_id
            else []
        )
        if cached:
            skills = _skills_for_learning(cached)
        else:
            from spejder.workflows.job_enrichment import materialize_job_skills

            page_context_cache: dict[str, str] = {}
            title_translation_cache: dict[str, str] = {}
            skills_text, _, _ = materialize_job_skills(
                db_path,
                row,
                llm=llm,
                runtime_profile=runtime_profile,
                page_context_cache=page_context_cache,
                title_translation_cache=title_translation_cache,
                rescore=False,
            )
            skills = _skills_for_learning(
                [s.strip() for s in skills_text.split(",") if s.strip()]
            )

        for skill in skills:
            counts[skill] += int(weight)
        considered += 1
        if considered % 10 == 0 or considered == learn_total:
            if progress:
                print(f"{progress_label}: {considered}/{learn_total} processed")
            if on_progress is not None:
                on_progress(considered, learn_total)

    # Persist current batch scores (set, not accumulate). Also zeros rows that
    # no longer appear in applied/relevant evidence — including any pre-fix
    # inflated additive totals on the first pass after upgrade.
    reconcile_skill_pattern_learning_scores(db_path, dict(counts))

    existing_patterns = _get_skill_patterns(db_path, runtime_profile)
    existing_names = {name.strip().lower() for name, _ in existing_patterns}

    candidates = [
        name
        for name, score in counts.most_common()
        if score >= min_occurrences and name.strip().lower() not in existing_names
    ]
    to_add = candidates[:max_new]
    if not to_add:
        if progress:
            print(f"{progress_label}: done (no new patterns)")
        return {
            "considered_positions": considered,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": len(existing_patterns),
        }

    added = 0
    for skill in to_add:
        key = skill.strip().lower()
        if not key:
            continue
        pattern = _skill_to_regex(skill)
        if not pattern:
            continue
        score = int(counts.get(skill, 0))
        ok = upsert_skill_pattern(
            db_path,
            name=skill,
            pattern=pattern,
            source="learned",
            occurrences_inc=score,
            weight_inc=float(score),
            enabled=True,
        )
        if ok:
            added += 1

    total_patterns = len(_get_skill_patterns(db_path, runtime_profile))
    if progress:
        print(
            f"{progress_label}: done (new_patterns={int(added)}, total_patterns={int(total_patterns)})"
        )
    return {
        "considered_positions": considered,
        "new_skill_patterns": int(added),
        "total_known_skill_patterns": int(total_patterns),
    }
