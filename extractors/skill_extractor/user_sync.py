from .normalization import _normalize_skill_name
from .utils import _split_skills_from_text
from .extraction import _extract_skills_fallback
from .patterns import _get_skill_patterns, _ensure_skill_pattern_seed_migration
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

def _extract_user_skills_from_cv(
    cv_text: str,
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM = None,
    limit: int = 80,
) -> list[str]:
    compact = " ".join((cv_text or "").split())
    if not compact:
        return []

    def _cleanup(items: list[str], max_items: int) -> list[str]:
        out = []
        seen = set()
        for item in items:
            name = _normalize_skill_name(item)
            key = name.lower()
            if not name or key in seen:
                continue
            seen.add(key)
            out.append(name)
            if len(out) >= max_items:
                break
        return out

    if llm:
        prompt = (
            f"Extract up to {int(limit)} technical and professional skills from this CV. "
            "Return only a comma-separated list of skill names (1-4 words each). "
            "Do not include job titles, companies, or generic soft phrases.\n\n"
            f"CV:\n{compact[:40000]}\n\n"
            "Skills:"
        )
        try:
            out = llm.generate(prompt, max_tokens=320)
            parsed = _split_skills_from_text(out)
            cleaned = _cleanup(parsed, int(limit))
            if cleaned:
                return cleaned
        except Exception:
            pass

    fallback = _extract_skills_fallback(
        compact,
        skill_patterns=_get_skill_patterns(db_path, profile),
        limit=max(20, int(limit)),
    )
    return _cleanup(fallback, int(limit))


def sync_user_skills(profile: str = None, db: str = None, model: str = "", cv: str = "./CV", limit: int = 80, max_chars: int = 40000, replace: bool = False, quiet_model: bool = False, llm: LocalLLM = None):
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"
    model_path = model or runtime_profile.default_model or ""
    cv_path = cv or "./CV"

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)

    print(f"Sync user skills: loading CV from {cv_path}")

    cv_text = load_cv_text(cv_path, max_chars=int(max_chars))
    if not cv_text.strip():
        print(f"CV not found or empty: {cv_path}")
        return

    print(f"Sync user skills: CV text loaded (chars={len(cv_text)})")

    cv_text = _translate_text_to_english_if_needed(
        cv_text,
        runtime_profile=runtime_profile,
    )

    if llm is None:
        llm = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=not quiet_model) if model_path else None
    if not llm:
        raise SystemExit("Model init: model is required for sync-user-skills")
    print("Sync user skills: extracting with model")
    extracted = _extract_user_skills_from_cv(
        cv_text,
        db_path=db_path,
        profile=runtime_profile,
        llm=llm,
        limit=int(limit),
    )

    if not extracted:
        print("No skills extracted from CV.")
        return

    print(f"Sync user skills: extracted {len(extracted)} skills, updating profile")

    profile_data = load_profile(DEFAULT_PROFILE_PATH)
    if profile_path and os.path.exists(profile_path):
        try:
            with open(profile_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                profile_data.update(loaded)
        except Exception:
            pass

    existing = [_normalize_skill_name(s) for s in (profile_data.get("user_skills") or [])]
    existing = [s for s in existing if s]

    if replace:
        merged = extracted
    else:
        merged = existing + extracted
        dedup = []
        seen = set()
        for s in merged:
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(s)
        merged = dedup

    profile_data["user_skills"] = merged
    _save_profile(profile_path, profile_data)

    print(
        f"User skills synced from CV: extracted={len(extracted)}, "
        f"total_user_skills={len(merged)}, profile={profile_path}"
    )
    print("Top extracted:", ", ".join(extracted[:20]))

