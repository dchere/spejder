# pylint: disable=all
import re
from typing import Optional

from spejder.config import AppConfig
from spejder.db import *
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor.extraction import _extract_skills_fallback
from spejder.extractors.skill_extractor.patterns import _get_skill_patterns
from spejder.jobs.parsing import _has_easy_apply_signal, _has_linkedin_public_easy_apply

EASY_APPLY_PATTERN = re.compile(r'\beasy\s*apply\b', flags=re.IGNORECASE)


def score_relevance(
    text: str,
    profile: AppConfig,
    skill_patterns: Optional[list[tuple[str, str]]] = None,
    source: str = "",
    position_link: str = "",
    easy_apply_cache: Optional[dict[str, bool]] = None,
    cached_required_skills: Optional[list[str]] = None,
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

    matched = sorted(
        [s for s in extracted_required if _normalize_skill_name_key(
            s) in user_skills]
    )
    missing = sorted(
        [s for s in extracted_required if _normalize_skill_name_key(
            s) not in user_skills]
    )

    skill_match_weight = profile.skill_match_weight
    skill_missing_penalty = profile.skill_missing_penalty

    if user_skills:
        score += float(len(matched)) * skill_match_weight
        score -= float(len(missing)) * skill_missing_penalty

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

    relevant = 1 if score >= min_score else 0
    category = "relevant" if score >= min_score else "not relevant"

    skill_source = "cached" if cached_required_skills else "regex"
    reason = (
        f"score={score:.1f}; include={hit_inc[:6]}; exclude={hit_exc[:6]}; "
        f"required_skills={list(required_keys)[:8]}; matched_skills={matched[:8]}; missing_skills={missing[:8]}; "
        f"skill_source={skill_source}; "
        f"easy_apply={has_easy_apply}; easy_apply_bonus={easy_apply_bonus if has_easy_apply else 0}"
    )
    return score, reason, relevant, category


def _load_skill_patterns(db_path: str, profile: AppConfig) -> list[tuple[str, str]]:
    return _get_skill_patterns(db_path, profile)


def apply_relevance(
    db_path: str, profile: AppConfig, prune_irrelevant: bool = False
) -> tuple[int, int]:
    rows_dict = get_jobs_for_scoring(db_path)
    rows = [(r["id"], r["source"], r["title"], r["company"], r["position_link"],
             r["raw_text"], r["relevance_reason"]) for r in rows_dict]
    relevant_count = 0

    skill_patterns = _load_skill_patterns(db_path, profile)

    easy_apply_cache: dict[str, bool] = {}
    pending_updates: list[tuple[int, float, str, int, str]] = []

    for rid, source, title, company, position_link, raw_text, relevance_reason in rows:
        manual_reason = (relevance_reason or "").strip().lower()
        if manual_reason == "manual_feedback=relevant":
            relevant_count += 1
            continue
        if manual_reason == "manual_feedback=not relevant":
            continue

        cached_skills = get_job_skills(db_path, rid) if rid else []
        composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
        score, reason, relevant, category = score_relevance(
            composed,
            profile,
            skill_patterns=skill_patterns,
            source=source or "",
            position_link=position_link or "",
            easy_apply_cache=easy_apply_cache,
            cached_required_skills=cached_skills if cached_skills else None,
        )
        pending_updates.append((rid, score, reason, relevant, category))
        if relevant:
            relevant_count += 1

    update_jobs_relevance(db_path, pending_updates, prune_irrelevant)
    return len(rows), relevant_count


def rescore_job_by_id(db_path: str, profile: AppConfig, job_id: int) -> bool:
    """Re-score one job and persist relevance score/reason.

    For applied jobs, keeps category/relevant as relevant while updating score/reason.
    """
    job_dict = get_job_for_rescoring(db_path, job_id)
    if not job_dict:
        return False
    rid = job_dict["id"]
    source = job_dict["source"]
    title = job_dict["title"]
    company = job_dict["company"]
    position_link = job_dict["position_link"]
    raw_text = job_dict["raw_text"]
    applied = job_dict["applied"]

    skill_patterns = _load_skill_patterns(db_path, profile)
    cached_skills = get_job_skills(db_path, rid) if rid else []

    composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
    score, reason, relevant, category = score_relevance(
        composed,
        profile,
        skill_patterns=skill_patterns,
        source=source or "",
        position_link=position_link or "",
        easy_apply_cache={},
        cached_required_skills=cached_skills if cached_skills else None,
    )

    if int(applied or 0) == 1:
        relevant = 1
        category = "relevant"

    update_jobs_relevance(db_path, [(rid, score, reason, relevant, category)])
    return True


def _skill_to_regex_simple(name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(
        r"[A-Za-z0-9+#.]+", name or "") if t]
    if not tokens:
        return name
    return r"\b" + r"\s+".join(tokens) + r"\b"
