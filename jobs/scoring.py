import re
from typing import Optional

from spejder.config import AppConfig
from spejder.db.deduplication_utils import (
    _canonicalize_company_for_dedupe,
    _normalize_company_key,
)
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor.extraction_fallback import _extract_skills_fallback
from spejder.extractors.skill_extractor.patterns import _get_skill_patterns
from spejder.jobs.parsing import _has_easy_apply_signal, _has_linkedin_public_easy_apply

EASY_APPLY_PATTERN = re.compile(r"\beasy\s*apply\b", flags=re.IGNORECASE)


def score_relevance(
    text: str,
    profile: AppConfig,
    skill_patterns: Optional[list[tuple[str, str]]] = None,
    source: str = "",
    position_link: str = "",
    easy_apply_cache: Optional[dict[str, bool]] = None,
    cached_required_skills: Optional[list[str]] = None,
    company: str = "",
    applied_company_keys: Optional[set[str]] = None,
) -> tuple[float, str, int, str]:
    include = [
        k.lower().strip() for k in profile.include_keywords if k.strip()
    ]
    exclude = [
        k.lower().strip() for k in profile.exclude_keywords if k.strip()
    ]
    min_score = profile.min_score

    corpus = text.lower()
    score = 0.0
    hit_inc = []
    hit_exc = []

    for kw in include:
        if kw in corpus:
            score += 1.5
            hit_inc.append(kw)
    for kw in exclude:
        if kw in corpus:
            score -= 2.0
            hit_exc.append(kw)

    user_skills = {
        _normalize_skill_name_key(s)
        for s in (profile.user_skills or [])
        if _normalize_skill_name_key(str(s))
    }
    if cached_required_skills:
        extracted_required = list(cached_required_skills)
    else:
        extracted_required = _extract_skills_fallback(
            text, skill_patterns or [])
    required_keys = {_normalize_skill_name_key(s) for s in extracted_required}

    unwanted_keys = {
        _normalize_skill_name_key(s)
        for s in (profile.unwanted_skills or [])
        if _normalize_skill_name_key(str(s))
    }
    unwanted_hits = sorted(
        [s for s in extracted_required if _normalize_skill_name_key(s) in unwanted_keys]
    )
    matched = sorted(
        [
            s for s in extracted_required
            if _normalize_skill_name_key(s) in user_skills
            and _normalize_skill_name_key(s) not in unwanted_keys
        ]
    )
    missing = sorted(
        [
            s for s in extracted_required
            if _normalize_skill_name_key(s) not in user_skills
            and _normalize_skill_name_key(s) not in unwanted_keys
        ]
    )

    skill_match_weight = profile.skill_match_weight
    skill_missing_penalty = profile.skill_missing_penalty
    skill_unwanted_penalty = profile.skill_unwanted_penalty

    if user_skills:
        score += float(len(matched)) * skill_match_weight
        score -= float(len(missing)) * skill_missing_penalty
    if unwanted_hits and skill_unwanted_penalty:
        score -= float(len(unwanted_hits)) * skill_unwanted_penalty

    easy_apply_bonus = profile.easy_apply_bonus
    source_low = (source or "").strip().lower()
    link_low = (position_link or "").strip().lower()
    is_linkedin = source_low == "linkedin" or "linkedin.com/" in link_low
    has_easy_apply = bool(is_linkedin and _has_easy_apply_signal(text))
    if is_linkedin and not has_easy_apply:
        has_easy_apply = _has_linkedin_public_easy_apply(
            position_link, easy_apply_cache=easy_apply_cache
        )
    if has_easy_apply and easy_apply_bonus:
        score += easy_apply_bonus

    applied_company_bonus = profile.applied_company_bonus
    company_key = _normalize_company_key(_canonicalize_company_for_dedupe(company or ""))
    has_applied_company = bool(
        company_key and applied_company_keys and company_key in applied_company_keys
    )
    if has_applied_company and applied_company_bonus:
        score += applied_company_bonus

    relevant = 1 if score >= min_score else 0
    category = "relevant" if score >= min_score else "not relevant"

    skill_source = "cached" if cached_required_skills else "regex"
    reason = (
        f"score={score:.1f}; include={hit_inc[:6]}; exclude={hit_exc[:6]}; "
        f"required_skills={list(required_keys)[:8]}; matched_skills={matched[:8]}; missing_skills={missing[:8]}; "
        f"skill_source={skill_source}; "
        f"easy_apply={has_easy_apply}; easy_apply_bonus={easy_apply_bonus if has_easy_apply else 0}; "
        f"applied_company={has_applied_company}; "
        f"applied_company_bonus={applied_company_bonus if has_applied_company else 0}; "
        f"unwanted_skills={unwanted_hits[:8]}; "
        f"skill_unwanted_penalty={skill_unwanted_penalty if unwanted_hits else 0}"
    )
    return score, reason, relevant, category


def _load_skill_patterns(db_path: str, profile: AppConfig) -> list[tuple[str, str]]:
    return _get_skill_patterns(db_path, profile)


def _skill_to_regex_simple(name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(
        r"[A-Za-z0-9+#.]+", name or "") if t]
    if not tokens:
        return name
    return r"\b" + r"\s+".join(tokens) + r"\b"


_RESCORE_EXPORTS = {
    "apply_relevance",
    "job_in_active_rescore_scope",
    "rescore_active_jobs",
    "rescore_job_by_id",
    "rescore_jobs_if_active",
}


def __getattr__(name: str):
    if name in _RESCORE_EXPORTS:
        from . import rescore
        return getattr(rescore, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
