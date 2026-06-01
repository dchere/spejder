SKILL_CLEANUP_GENERIC_SINGLE = __import__('re').compile(r'^([a-z])\\1*$')
SKILL_CUE_PATTERN = __import__('re').compile(r'(?i)(?:skills?|requirements|requirements?|qualifications?|you have|your profile|your background|about you|we expect|what you bring|who you are|we are looking for|you bring)')
from .normalization import _normalize_skill_name
from .patterns import _skill_to_regex
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

def _blocked_skill_keys(profile: Optional[AppConfig] = None) -> set[str]:
    values = profile.blocked_skills if profile and profile.blocked_skills else []
    return {
        _normalize_skill_name(str(item)).lower()
        for item in values
        if _normalize_skill_name(str(item))
    }


def _filter_blocked_skill_names(skills: list[str], profile: Optional[AppConfig] = None) -> list[str]:
    blocked = _blocked_skill_keys(profile)
    out = []
    seen = set()
    for skill in skills:
        normalized = _normalize_skill_name(skill)
        key = normalized.lower()
        if not normalized or key in blocked or key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def _protected_skill_keys(profile: AppConfig) -> set[str]:
    protected = set()

    for item in profile.known_skill_patterns or []:
        if not isinstance(item, dict):
            continue
        name = _normalize_skill_name(str(item.get("name", "")))
        if name:
            protected.add(name.lower())

    for field in ("user_skills", "missing_skills_suggestions"):
        for item in getattr(profile, field, []) or []:
            name = _normalize_skill_name(str(item))
            if name:
                protected.add(name.lower())

    return protected


def _skill_cleanup_reason(name: str, source: str, protected_keys: set[str]) -> str:
    skill = _normalize_skill_name(name)
    key = skill.lower()
    source_key = (source or "").strip().lower()

    if not key:
        return "empty"
    if key in protected_keys or source_key.startswith("profile"):
        return ""

    reason = ""
    tokens = re.findall(r"[a-z0-9+#./-]+", key)

    if key in SKILL_CLEANUP_GENERIC_PHRASES:
        reason = "generic phrase"
    elif any(char in key for char in "?[]{}"):
        reason = "malformed text"
    elif re.search(r"\b(?:we|our|you|your|they|them|their)\b", key):
        reason = "sentence fragment"
    elif not tokens:
        reason = "empty"
    elif len(tokens) > 4:
        reason = "too many words"
    elif any(token in SKILL_CLEANUP_STOPWORDS for token in tokens):
        reason = "contains stopword"
    elif tokens[0] in SKILL_CLEANUP_PREFIXES:
        reason = "sentence fragment"
    elif len(tokens) == 1 and tokens[0] in SKILL_CLEANUP_GENERIC_SINGLE:
        reason = "generic term"

    return reason


def _is_candidate_strong(skill: str, evidence: str, confidence: float, new_skill_conf_threshold: float, cleaned: str) -> bool:
    if not skill or len(skill.split()) > 4 or confidence < new_skill_conf_threshold:
        return False
    corpus = cleaned.lower()
    skill_low = skill.lower()
    if skill_low not in corpus:
        token_pattern = _skill_to_regex(skill)
        if token_pattern and not re.search(token_pattern, corpus, flags=re.IGNORECASE):
            return False
    if evidence:
        evidence_low = " ".join(evidence.split()).lower()
        if evidence_low and evidence_low not in corpus:
            return False
    cue_window = 200
    idx = corpus.find(skill_low)
    if idx != -1:
        window_start = max(0, idx - cue_window)
        window_end = min(len(corpus), idx + len(skill_low) + cue_window)
        window = corpus[window_start:window_end]
        if not SKILL_CUE_PATTERN.search(window):
            return False
    return True


def _passes_phrase_quality(skill_name: str) -> bool:
    skill = _normalize_skill_name(skill_name)
    if not skill:
        return False
    if re.match(r"^(?:our|we|you|they|it|this|that|these|those|and|or|but)\b", skill):
        return False
    tokens = [t for t in re.findall(r"[a-z0-9+#.]+", skill) if t]
    if not tokens:
        return False
    stop_tokens = {
        "our", "we", "you", "their", "team", "colleague", "company", "role",
        "position", "work", "used", "across", "with", "for", "and", "or",
        "but", "the", "a", "an", "as", "at", "to",
    }
    if all(t in stop_tokens for t in tokens):
        return False
    return True

