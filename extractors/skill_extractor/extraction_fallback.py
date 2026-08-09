"""Regex and phrase fallback for job skill extraction."""

import re
from typing import Optional

from spejder.config import AppConfig

from .constants import SKILL_CUE_PATTERN
from .filtering import _filter_extracted_skills
from .normalization import _normalize_skill_name


def _extract_skills_fallback(
    text: str, skill_patterns: list[tuple[str, str]]
) -> list[str]:
    source = " ".join((text or "").split())
    if not source:
        return []

    hits = []
    low = source.lower()
    for label, pattern in skill_patterns:
        m = re.search(pattern, low, flags=re.IGNORECASE)
        if m:
            hits.append((m.start(), label))
    hits.sort(key=lambda x: x[0])

    ordered = []
    seen = set()
    for _, label in hits:
        key = label.lower()
        if key not in seen:
            seen.add(key)
            ordered.append(label)

    sentences = re.split(r"(?<=[.!?])\s+", source)
    phrase_candidates = []
    for sentence in sentences:
        if not sentence:
            continue
        if not SKILL_CUE_PATTERN.search(sentence):
            continue
        cleaned_sentence = re.sub(
            r"^.*?\b(?:requirements?|qualifications?)\b\s*:?",
            "",
            sentence,
            flags=re.IGNORECASE,
        )
        chunks = re.split(r",|\band\b|\bor\b", cleaned_sentence, flags=re.IGNORECASE)
        for chunk in chunks:
            chunk = _normalize_skill_name(chunk)
            if chunk:
                phrase_candidates.append(chunk)

    for skill in phrase_candidates:
        key = skill.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(skill)

    return ordered


def _filter_fallback_skills(
    skills: list[str],
    profile: Optional[AppConfig],
    db_path: str,
    known_keys: set[str],
) -> list[str]:
    return _filter_extracted_skills(skills, profile, db_path, known_keys)
