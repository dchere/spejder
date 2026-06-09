from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_applied_jobs, get_jobs_by_category
from spejder.extractors.skill_extractor import _get_or_extract_job_skills
from spejder.llm import LocalLLM
from spejder.workflows.job_text_enrichment import _enrich_raw_text_with_position_page


def materialize_job_skills(
    db_path: str,
    row: dict,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    page_context_cache: Optional[dict] = None,
    title_translation_cache: Optional[dict] = None,
    limit: int = 10,
    rescore: bool = False,
) -> tuple[str, str]:
    """Enrich job text, extract skills, persist to job_skills. Returns (skills_text, enriched_raw)."""
    job_id = int(row.get("id", 0) or 0)
    raw_text = _enrich_raw_text_with_position_page(
        db_path,
        row,
        page_context_cache=page_context_cache,
        llm=llm,
        runtime_profile=runtime_profile,
        title_translation_cache=title_translation_cache,
    )
    skills_text = _get_or_extract_job_skills(
        db_path,
        job_id,
        raw_text,
        llm=llm,
        profile=runtime_profile,
        position_link=row.get("position_link", ""),
        page_context_cache=page_context_cache,
        limit=limit,
    )
    if rescore and job_id and runtime_profile is not None and skills_text:
        from spejder.jobs.scoring import rescore_job_by_id

        rescore_job_by_id(db_path, runtime_profile, job_id)
    return skills_text, raw_text


def materialize_jobs_skills(
    db_path: str,
    rows: list[dict],
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    limit: int = 10,
    rescore: bool = False,
    skip_cached: bool = False,
    progress_label: str = "",
) -> int:
    """Materialize skills for multiple jobs. Returns count of jobs that received skills."""
    if not rows:
        return 0

    from spejder.db import get_job_skills

    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    updated = 0
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        job_id = int(row.get("id", 0) or 0)
        if not job_id:
            continue
        if skip_cached and get_job_skills(db_path, job_id):
            continue

        skills_text, _ = materialize_job_skills(
            db_path,
            row,
            llm=llm,
            runtime_profile=runtime_profile,
            page_context_cache=page_context_cache,
            title_translation_cache=title_translation_cache,
            limit=limit,
            rescore=rescore,
        )
        if skills_text:
            updated += 1
        if progress_label and (idx % 25 == 0 or idx == total):
            print(f"{progress_label}: checked={idx}/{total}, updated={updated}")
    return updated


def _collect_relevant_and_applied_rows(db_path: str) -> list[dict]:
    rows: list[dict] = []
    seen_ids: set[int] = set()
    for row in get_applied_jobs(db_path, limit=0):
        rid = int(row.get("id", 0) or 0)
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            rows.append(row)
    for cat in ("relevant", "not relevant"):
        for row in get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True):
            rid = int(row.get("id", 0) or 0)
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                rows.append(row)
    return rows


def materialize_relevant_and_applied_skills(
    db_path: str,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    limit: int = 10,
    rescore: bool = True,
    skip_cached: bool = True,
    progress_label: str = "Skill materialization",
) -> int:
    """Phase-2 batch: enrich, extract, persist, and optionally rescore scoped jobs."""
    rows = _collect_relevant_and_applied_rows(db_path)
    if progress_label:
        print(f"{progress_label}: starting (jobs={len(rows)})")
    updated = materialize_jobs_skills(
        db_path,
        rows,
        llm=llm,
        runtime_profile=runtime_profile,
        limit=limit,
        rescore=rescore,
        skip_cached=skip_cached,
        progress_label=progress_label,
    )
    if progress_label:
        print(f"{progress_label}: done (updated={updated})")
    return updated
