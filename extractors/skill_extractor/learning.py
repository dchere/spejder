"""Batch skill pattern learning from applied and relevant job positions."""

from collections import Counter
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    get_all_applied_jobs,
    get_job_skills,
    get_jobs_by_category,
    upsert_skill_pattern,
)
from spejder.llm import LocalLLM

from .filtering import _blocked_skill_keys
from .normalization import _normalize_skill_name
from .patterns import _get_skill_patterns
from .utils import _skill_to_regex


def _learn_skill_patterns_from_positions(
    db_path: str,
    runtime_profile: AppConfig,
    llm: Optional[LocalLLM] = None,
    progress: bool = False,
    progress_label: str = "Skill pattern learning",
) -> dict:
    applied_rows = get_all_applied_jobs(db_path, limit=0)
    relevant_rows = get_jobs_by_category(
        db_path, "relevant", limit=0, unviewed_only=False, exclude_hidden=False
    )

    rows = []
    seen_ids = set()
    for row in applied_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        rows.append((row, 3))
    for row in relevant_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        rows.append((row, 1))

    if not rows:
        if progress:
            print(f"{progress_label}: no applied/relevant positions found")
        return {
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": len(_get_skill_patterns(db_path, runtime_profile)),
        }

    max_positions = int(runtime_profile.skill_learning_max_positions or 180)
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
        print(f"{progress_label}: starting (positions={min(len(rows), max_positions)})")

    for row, weight in rows[:max_positions]:
        job_id = int(row.get("id", 0) or 0)
        cached = get_job_skills(db_path, job_id) if job_id else []
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
        if progress and (considered % 10 == 0 or considered == min(len(rows), max_positions)):
            print(f"{progress_label}: {considered}/{min(len(rows), max_positions)} processed")

    existing_patterns = _get_skill_patterns(db_path, runtime_profile)
    existing_names = {name.strip().lower() for name, _ in existing_patterns}
    existing_map = {name.strip().lower(): pattern for name, pattern in existing_patterns}

    for skill, score in counts.items():
        key = skill.strip().lower()
        if key not in existing_names:
            continue
        pattern = existing_map.get(key, "")
        if not pattern:
            continue
        upsert_skill_pattern(
            db_path,
            name=skill,
            pattern=pattern,
            source="learned",
            occurrences_inc=int(score),
            weight_inc=float(score),
            enabled=True,
        )

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
        ok = upsert_skill_pattern(
            db_path,
            name=skill,
            pattern=pattern,
            source="learned",
            occurrences_inc=int(counts.get(skill, 0)),
            weight_inc=float(counts.get(skill, 0)),
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
