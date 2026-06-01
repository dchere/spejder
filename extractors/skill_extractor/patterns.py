from .normalization import _normalize_skill_name
# pylint: disable=all
"""
Skill extractor for parsing...
"""
import json
import os
import re
from collections import Counter
from contextlib import suppress
from typing import Optional

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH, load_profile, load_runtime_profile
from spejder.db import (
    ensure_db,
    delete_skill_from_db,
    get_job_skills,
    set_job_skills,
    get_skill_patterns as get_db_skill_patterns,
    get_applied_jobs,
    get_jobs_by_category,
    upsert_skill_pattern,
    migrate_profile_skill_patterns_to_db
)
from spejder.llm import LocalLLM
from spejder.managers.language_manager import translate_text_to_english_if_needed as _translate_text_to_english_if_needed
from spejder.managers.profile_manager import _save_profile, _block_skill_in_profile
from spejder.parsers.cv_parser import load_cv_text

SKILL_CLEANUP_GENERIC_PHRASES = set()
SKILL_CLEANUP_STOPWORDS = set()
SKILL_CLEANUP_PREFIXES = ()

def _get_skill_patterns(
    db_path: str, profile: Optional[AppConfig] = None
) -> list[tuple[str, str]]:
    from .filtering import _blocked_skill_keys
    blocked_keys = _blocked_skill_keys(profile)
    db_rows = get_db_skill_patterns(db_path, enabled_only=True)
    if db_rows:
        patterns = []
        for row in db_rows:
            name = str(row.get("name", "")).strip()
            pattern = str(row.get("pattern", "")).strip()
            if name and pattern and _normalize_skill_name(name).lower() not in blocked_keys:
                patterns.append((name, pattern))
        if patterns:
            return patterns

    raw = profile.known_skill_patterns if profile else []
    if not isinstance(raw, list) or not raw:
        raw = []

    patterns: list[tuple[str, str]] = []
    for item in raw:
        # Pydantic may give dict or objects for known_skill_patterns
        if hasattr(item, "name"):
            name = str(getattr(item, "name", "")).strip()
            pattern = str(getattr(item, "pattern", "")).strip()
        elif isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            pattern = str(item.get("pattern", "")).strip()
        else:
            continue
        if not name or not pattern or _normalize_skill_name(name).lower() in blocked_keys:
            continue
        patterns.append((name, pattern))

    return patterns


def _skill_to_regex(skill_name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(r"[A-Za-z0-9+#.]+", skill_name or "") if t]
    if not tokens:
        return ""
    return r"\b" + r"\s+".join(tokens) + r"\b"


def _learn_skill_patterns_from_positions(
    db_path: str,
    runtime_profile: AppConfig,
    llm: LocalLLM = None,
    progress: bool = False,
    progress_label: str = "Skill pattern learning",
) -> dict:
    # Learn from user-positive signals first: applied jobs, then relevant jobs.
    applied_rows = get_applied_jobs(db_path, limit=0)
    relevant_rows = get_jobs_by_category(db_path, "relevant", limit=0, unviewed_only=False)

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

    counts: Counter[str] = Counter()
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    considered = 0

    if progress:
        print(f"{progress_label}: starting (positions={min(len(rows), max_positions)})")

    for row, weight in rows[:max_positions]:
        from spejder.parsers.web_parser import _extract_position_page_text
        raw = _extract_position_page_text(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        )
        skills_text = _get_or_extract_job_skills(
            db_path,
            row.get("id", 0),
            raw,
            llm=llm,
            profile=runtime_profile,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
            limit=10,
        )
        skills = [
            _normalize_skill_name(s) for s in skills_text.split(",") if _normalize_skill_name(s)
        ]
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


def _ensure_skill_pattern_seed_migration(db_path: str, profile_path: str):
    """Ensure that skill patterns are migrated from the profile to the db."""
    with suppress(Exception):
        migrate_profile_skill_patterns_to_db(db_path, profile_path)

