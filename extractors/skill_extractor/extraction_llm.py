"""LLM path for job skill extraction."""

from typing import Optional

from spejder.config import AppConfig
from spejder.llm import LocalLLM

from .extraction_prompt import _build_job_skill_extraction_prompt
from .filtering import _filter_extracted_skills, _is_candidate_strong, _passes_phrase_quality
from .normalization import _normalize_skill_name
from .patterns import _get_skill_patterns
from .utils import (
    _clean_model_output,
    _extract_json_object,
    _format_skills,
    _split_skills_from_text,
    _to_items,
)


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
    known_by_key = {
        _normalize_skill_name(name).lower(): _normalize_skill_name(name)
        for name, _ in skill_patterns
        if _normalize_skill_name(name)
    }
    known_keys = set(known_by_key.keys())
    known_list = [known_by_key[key] for key in sorted(known_by_key.keys())]
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
            if key in cleaned.lower():
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
            if key in known_by_key and _passes_phrase_quality(skill):
                constrained.append(known_by_key[key])
        filtered_constrained = _filter_extracted_skills(
            constrained, profile, db_path, known_keys
        )
        if filtered_constrained:
            return _format_skills(filtered_constrained)
    except (RuntimeError, ValueError, TypeError, KeyError):
        pass
    return None
