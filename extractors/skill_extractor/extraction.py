"""Job skill extraction via LLM and regex fallback."""

import re
from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_job_skills, set_job_skills
from spejder.llm import LocalLLM

from .constants import SKILL_CUE_PATTERN
from .filtering import (
    _filter_blocked_skill_names,
    _is_candidate_strong,
    _passes_phrase_quality,
)
from .normalization import _normalize_skill_name
from .patterns import _get_skill_patterns
from .utils import (
    _clean_model_output,
    _extract_json_object,
    _format_skills,
    _split_skills_from_text,
    _to_items,
)


def _prompt_antipatterns(profile: Optional[AppConfig]) -> list[str]:
    if not profile:
        return []
    items = profile.skill_extraction_antipatterns or []
    prompt_max = int(getattr(profile, "skill_antipattern_prompt_max_items", 40) or 40)
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    return cleaned[-max(0, prompt_max) :]


def _build_job_skill_extraction_prompt(
    *,
    known_list: list[str],
    user_skills: list[str],
    cleaned: str,
    antipatterns: Optional[list[str]] = None,
) -> str:
    known_skills_prompt = ", ".join(known_list[:300])
    user_skills_prompt = ", ".join(user_skills)
    antipattern_section = ""
    if antipatterns:
        antipattern_section = (
            "Antipatterns (never return these or similar phrasing):\n"
            + "\n".join(f"- {item}" for item in antipatterns)
            + "\n\n"
        )
    return (
        "Task: Extract required professional/technical skills from the job text.\n"
        "Known skills (prefer these): "
        f"{known_skills_prompt}\n\n"
        "Candidate skills from user profile (extra context): "
        f"{user_skills_prompt}\n\n"
        "Hard rules:\n"
        "1) Return only concrete skill entities (tools, languages, frameworks, methods, domains, certifications).\n"
        "2) Exclude all narrative, hiring, company, and generic phrases.\n"
        "3) Exclude pronoun-led, hiring, marketing, editorial, and business narrative phrasing.\n"
        "4) Do NOT return sentence fragments or clauses.\n"
        "5) Every returned skill must be supported by exact evidence from the text.\n"
        "6) First select explicitly required skills from Known skills.\n"
        "7) Add new skills only if strongly relevant and explicitly required.\n"
        "8) Skill names only (up to 4 words), translated to english and lowercase.\n"
        "9) Remove qualitative adjectives from skill names (example: 'good software design' -> 'software design').\n\n"
        "10) Remove education prefixes from skill names (example: 'degree in electrical engineering' -> 'electrical engineering').\n\n"
        "11) Remove qualification prefixes from skill names (example: 'experience with microsoft dynamics 365' -> 'microsoft dynamics 365').\n\n"
        f"{antipattern_section}"
        "Validation before output:\n"
        "- If name has stopwords-only business phrasing, drop it.\n"
        "- If no concrete skills found, return empty arrays.\n\n"
        "Output format (strict JSON) with keys matched_known and new_candidates, each an array of objects: "
        "{\"name\": string, \"confidence\": number, \"evidence\": string}.\n\n"
        f"Description:\n{cleaned}\n\n"
        "JSON:"
    )


def _apply_blocked_filter(skills: list[str], profile: Optional[AppConfig], skip_blocked_filter: bool) -> list[str]:
    if skip_blocked_filter:
        return skills
    return _filter_blocked_skill_names(skills, profile)


def _extract_skills_fallback(
    text: str, skill_patterns: list[tuple[str, str]], limit: int = 10
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

    if len(ordered) >= limit:
        return ordered[:limit]

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
        if len(ordered) >= limit:
            break

    return ordered[:limit]


def _cap_antipatterns_for_prompt(
    items: list[str], profile: Optional[AppConfig]
) -> list[str]:
    prompt_max = int(getattr(profile, "skill_antipattern_prompt_max_items", 40) or 40)
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    return cleaned[-max(0, prompt_max) :]


def _extract_job_skills_llm_path(
    db_path: str,
    raw_text: str,
    llm: Optional[LocalLLM] = None,
    profile: Optional[AppConfig] = None,
    limit: int = 10,
    skip_blocked_filter: bool = False,
    antipatterns_override: Optional[list[str]] = None,
) -> Optional[str]:
    cleaned = " ".join((raw_text or "").split())
    if not llm or not cleaned:
        return None

    skill_patterns = _get_skill_patterns(db_path, profile)
    profile_data = profile.model_dump() if profile else {}
    new_skill_conf_threshold = float(
        profile_data.get("skill_new_confidence_threshold", 0.9) or 0.9
    )
    new_skill_max_per_job = int(profile_data.get("skill_new_max_per_job", 2) or 2)
    known_by_key = {
        _normalize_skill_name(name).lower(): _normalize_skill_name(name)
        for name, _ in skill_patterns
        if _normalize_skill_name(name)
    }
    known_list = [known_by_key[k] for k in sorted(known_by_key.keys())]
    user_skills = []
    for item in profile_data.get("user_skills", []) or []:
        skill = _normalize_skill_name(str(item))
        if skill:
            user_skills.append(skill)
    user_skills = user_skills[:200]

    if antipatterns_override is not None:
        antipatterns = _cap_antipatterns_for_prompt(antipatterns_override, profile)
    else:
        antipatterns = _prompt_antipatterns(profile)
    prompt = _build_job_skill_extraction_prompt(
        known_list=known_list,
        user_skills=user_skills,
        cleaned=cleaned,
        antipatterns=antipatterns,
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
            if len(selected) >= int(limit):
                break

        if len(selected) < int(limit):
            added_new = 0
            for item in _to_items(parsed_json.get("new_candidates")):
                skill = _normalize_skill_name(str(item.get("name", "")))
                key = skill.lower()
                if not key or key in seen or key in known_by_key:
                    continue
                confidence_raw = item.get("confidence", 0.0)
                try:
                    confidence = float(confidence_raw)
                except Exception:
                    confidence = 0.0
                evidence = str(item.get("evidence", ""))
                if not _is_candidate_strong(skill, evidence, confidence, new_skill_conf_threshold, cleaned):
                    continue
                if not _passes_phrase_quality(skill):
                    continue
                selected.append(skill)
                seen.add(key)
                added_new += 1
                if added_new >= max(0, int(new_skill_max_per_job)):
                    break
                if len(selected) >= int(limit):
                    break

        if selected:
            filtered_selected = _apply_blocked_filter(selected, profile, skip_blocked_filter)
            if filtered_selected:
                return _format_skills(filtered_selected, limit=limit)

        parsed_text = _split_skills_from_text(_clean_model_output(out))
        constrained = []
        for skill in parsed_text:
            key = skill.lower()
            if key in known_by_key and _passes_phrase_quality(skill):
                constrained.append(known_by_key[key])
        filtered_constrained = _apply_blocked_filter(constrained, profile, skip_blocked_filter)
        if filtered_constrained:
            return _format_skills(filtered_constrained, limit=limit)
    except Exception:
        pass
    return None


def _extract_job_skills(
    db_path: str,
    raw_text: str,
    llm: Optional[LocalLLM] = None,
    profile: Optional[AppConfig] = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
    limit: int = 10,
    skip_blocked_filter: bool = False,
    antipatterns_override: Optional[list[str]] = None,
) -> str:
    cleaned = " ".join((raw_text or "").split())
    skill_patterns = _get_skill_patterns(db_path, profile)

    llm_result = _extract_job_skills_llm_path(
        db_path,
        raw_text,
        llm=llm,
        profile=profile,
        limit=limit,
        skip_blocked_filter=skip_blocked_filter,
        antipatterns_override=antipatterns_override,
    )
    if llm_result is not None:
        return llm_result

    fallback_source = cleaned
    if (
        not fallback_source
        and position_link
        and page_context_cache
        and position_link in page_context_cache
    ):
        fallback_source = page_context_cache.get(position_link, "")
    fallback_skills = _apply_blocked_filter(
        _extract_skills_fallback(fallback_source, skill_patterns=skill_patterns, limit=limit),
        profile,
        skip_blocked_filter,
    )
    return _format_skills(fallback_skills, limit=limit)


def _get_or_extract_job_skills(
    db_path: str,
    job_id: int,
    raw_text: str,
    llm: Optional[LocalLLM] = None,
    profile: Optional[AppConfig] = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
    limit: int = 10,
) -> str:
    """Return skill tags for a job, reading from the job_skills cache or extracting + caching."""
    if job_id:
        cached = get_job_skills(db_path, job_id)
        if cached:
            return _format_skills(_filter_blocked_skill_names(cached, profile), limit=limit)
    skills_text = _extract_job_skills(
        db_path,
        raw_text,
        llm=llm,
        profile=profile,
        position_link=position_link,
        page_context_cache=page_context_cache,
        limit=limit,
    )
    if job_id and skills_text:
        set_job_skills(db_path, job_id, [s.strip() for s in skills_text.split(",") if s.strip()])
    return skills_text
