"""Job skill extraction orchestration (LLM + fallback)."""

from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_job_skills, set_job_skills
from spejder.llm import LocalLLM

from .extraction_fallback import (
    _apply_blocked_filter,
    _extract_skills_fallback,
)
from .extraction_llm import _extract_job_skills_llm_path
from .extraction_prompt import _build_job_skill_extraction_prompt
from .filtering import _filter_blocked_skill_names
from .patterns import _get_skill_patterns
from .utils import _format_skills

__all__ = [
    "_build_job_skill_extraction_prompt",
    "_extract_skills_fallback",
    "_extract_job_skills_llm_path",
    "_extract_job_skills",
    "_get_or_extract_job_skills",
]


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
        _extract_skills_fallback(
            fallback_source, skill_patterns=skill_patterns, limit=limit
        ),
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
            return _format_skills(
                _filter_blocked_skill_names(cached, profile), limit=limit
            )
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
        set_job_skills(
            db_path,
            job_id,
            [s.strip() for s in skills_text.split(",") if s.strip()],
        )
    return skills_text
