SKILL_CUE_PATTERN = __import__('re').compile(r'(?i)(?:skills?|requirements|requirements?|qualifications?|you have|your profile|your background|about you|we expect|what you bring|who you are|we are looking for|you bring)')
from .normalization import _normalize_skill_name
from .utils import _extract_json_object, _to_items, _format_skills, _split_skills_from_text, _clean_model_output
from .filtering import _is_candidate_strong, _filter_blocked_skill_names
from .patterns import _get_skill_patterns
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


def _extract_job_skills(
    db_path: str,
    raw_text: str,
    llm: LocalLLM = None,
    profile: Optional[AppConfig] = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
    limit: int = 10,
) -> str:
    cleaned = " ".join((raw_text or "").split())
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


    def _passes_phrase_quality(skill_name: str) -> bool:
        skill = _normalize_skill_name(skill_name)
        if not skill:
            return False

        # Reject pronoun/connector-led fragments that are usually narrative clauses.
        if re.match(r"^(?:our|we|you|they|it|this|that|these|those|and|or|but)\b", skill):
            return False

        tokens = [t for t in re.findall(r"[a-z0-9+#.]+", skill) if t]
        if not tokens:
            return False

        stop_tokens = {
            "our",
            "we",
            "you",
            "their",
            "team",
            "colleague",
            "company",
            "role",
            "position",
            "work",
            "used",
            "across",
            "with",
            "for",
            "and",
            "or",
            "but",
            "the",
            "a",
            "an",
            "as",
            "at",
            "to",
        }

        # If all tokens are generic business stop words, treat as non-skill phrase.
        if all(t in stop_tokens for t in tokens):
            return False

        return True

    if llm and cleaned:
        known_skills_prompt = ", ".join(known_list[:300])
        user_skills_prompt = ", ".join(user_skills)
        prompt = (
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
            "Validation before output:\n"
            "- If name has stopwords-only business phrasing, drop it.\n"
            "- If no concrete skills found, return empty arrays.\n\n"
            "Output format (strict JSON) with keys matched_known and new_candidates, each an array of objects: "
            "{\"name\": string, \"confidence\": number, \"evidence\": string}.\n\n"
            f"Description:\n{cleaned}\n\n"
            "JSON:"
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
                filtered_selected = _filter_blocked_skill_names(selected, profile)
                if filtered_selected:
                    return _format_skills(filtered_selected, limit=limit)

            parsed_text = _split_skills_from_text(_clean_model_output(out))
            constrained = []
            for skill in parsed_text:
                key = skill.lower()
                if key in known_by_key and _passes_phrase_quality(skill):
                    constrained.append(known_by_key[key])
            filtered_constrained = _filter_blocked_skill_names(constrained, profile)
            if filtered_constrained:
                return _format_skills(filtered_constrained, limit=limit)
        except Exception:
            pass

    fallback_source = cleaned
    if (
        not fallback_source
        and position_link
        and page_context_cache
        and position_link in page_context_cache
    ):
        fallback_source = page_context_cache.get(position_link, "")
    fallback_skills = _filter_blocked_skill_names(
        _extract_skills_fallback(fallback_source, skill_patterns=skill_patterns, limit=limit),
        profile,
    )
    return _format_skills(fallback_skills, limit=limit)


def _get_or_extract_job_skills(
    db_path: str,
    job_id: int,
    raw_text: str,
    llm: LocalLLM = None,
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

