"""LLM path for job skill extraction."""

import re
from typing import Optional

from spejder.config import AppConfig
from spejder.llm import LocalLLM

from .extraction_prompt import KNOWN_SKILLS_PROMPT_LIMIT, _build_job_skill_extraction_prompt
from .filtering import _filter_extracted_skills, _is_candidate_strong, _passes_phrase_quality
from .normalization import _normalize_skill_name
from .patterns import _get_skill_patterns
from .short_token_gates import _short_token_evidence_ok
from .utils import (
    _clean_model_output,
    _extract_json_object,
    _format_skills,
    _split_skills_from_text,
    _to_items,
)


def _pattern_hits_cleaned(pattern: str, cleaned_lower: str) -> bool:
    """True when a skill pattern matches cleaned job text (fallback-style search)."""
    if not pattern or not cleaned_lower:
        return False
    try:
        return re.search(pattern, cleaned_lower, flags=re.IGNORECASE) is not None
    except re.error:
        return False


def _select_known_skills_for_prompt(
    skill_patterns: list[tuple[str, str]],
    cleaned: str,
    *,
    limit: int = KNOWN_SKILLS_PROMPT_LIMIT,
) -> tuple[dict[str, str], list[str]]:
    """Build known map + prompt vocabulary.

    Preserves weight/occurrences order from ``skill_patterns`` (no alphabetical
    re-sort). Prefers pattern text-hits in the job text, then pads with the
    remaining weight-ordered names up to ``limit``.
    """
    known_by_key: dict[str, str] = {}
    ordered: list[tuple[str, str]] = []
    for name, pattern in skill_patterns:
        skill = _normalize_skill_name(name)
        if not skill:
            continue
        key = skill.lower()
        if key in known_by_key:
            continue
        known_by_key[key] = skill
        ordered.append((skill, pattern or ""))

    cleaned_lower = cleaned.lower()
    hits: list[str] = []
    rest: list[str] = []
    for skill, pattern in ordered:
        if _pattern_hits_cleaned(pattern, cleaned_lower):
            hits.append(skill)
        else:
            rest.append(skill)

    known_list = (hits + rest)[: max(0, int(limit))]
    return known_by_key, known_list


def _extract_job_skills_llm_path(
    db_path: str,
    raw_text: str,
    llm: Optional[LocalLLM] = None,
    profile: Optional[AppConfig] = None,
) -> Optional[str]:
    cleaned = " ".join((raw_text or "").split())
    if not llm or not cleaned:
        return None

    skill_patterns = _get_skill_patterns(db_path, profile)
    profile_data = profile.model_dump() if profile else {}
    new_skill_conf_threshold = float(
        profile_data.get("skill_new_confidence_threshold", 0.9) or 0.9
    )
    known_by_key, known_list = _select_known_skills_for_prompt(skill_patterns, cleaned)
    known_keys = set(known_by_key.keys())
    user_skills = []
    for item in profile_data.get("user_skills", []) or []:
        skill = _normalize_skill_name(str(item))
        if skill:
            user_skills.append(skill)
    user_skills = user_skills[:200]

    prompt = _build_job_skill_extraction_prompt(
        known_list=known_list,
        user_skills=user_skills,
        cleaned=cleaned,
    )
    try:
        out = llm.generate(prompt, max_tokens=320)
        parsed_json = _extract_json_object(out)

        selected: list[str] = []
        seen = set()

        for item in _to_items(parsed_json.get("matched_known")):
            skill = _normalize_skill_name(str(item.get("name", "")))
            key = skill.lower()
            if not key or key not in known_by_key or key in seen:
                continue
            if not _passes_phrase_quality(skill):
                continue
            if key not in cleaned.lower():
                continue
            if not _short_token_evidence_ok(key, cleaned):
                continue
            selected.append(known_by_key[key])
            seen.add(key)

        for item in _to_items(parsed_json.get("new_candidates")):
            skill = _normalize_skill_name(str(item.get("name", "")))
            key = skill.lower()
            if not key or key in seen or key in known_by_key:
                continue
            confidence_raw = item.get("confidence", 0.0)
            try:
                confidence = float(confidence_raw)
            except (TypeError, ValueError):
                confidence = 0.0
            evidence = str(item.get("evidence", ""))
            if not _is_candidate_strong(
                skill, evidence, confidence, new_skill_conf_threshold, cleaned
            ):
                continue
            if not _passes_phrase_quality(skill):
                continue
            selected.append(skill)
            seen.add(key)

        if selected:
            filtered_selected = _filter_extracted_skills(
                selected, profile, db_path, known_keys
            )
            if filtered_selected:
                return _format_skills(filtered_selected)

        parsed_text = _split_skills_from_text(_clean_model_output(out))
        constrained = []
        for skill in parsed_text:
            key = skill.lower()
            if key not in known_by_key or not _passes_phrase_quality(skill):
                continue
            if not _short_token_evidence_ok(key, cleaned):
                continue
            constrained.append(known_by_key[key])
        filtered_constrained = _filter_extracted_skills(
            constrained, profile, db_path, known_keys
        )
        if filtered_constrained:
            return _format_skills(filtered_constrained)
    except (RuntimeError, ValueError, TypeError, KeyError):
        pass
    return None
