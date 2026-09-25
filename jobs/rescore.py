"""Active-scope rescore batch helpers."""
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    get_applied_pipeline_company_keys,
    get_job_for_rescoring,
    get_jobs_for_active_rescore,
    get_jobs_for_scoring,
    update_jobs_relevance,
)
from spejder.extractors.skill_extractor import get_job_skills_filtered
from spejder.jobs.scoring import score_relevance, _load_skill_patterns

def job_in_active_rescore_scope(row: dict) -> bool:
    if int(row.get("applied", 0) or 0) == 1:
        return True
    if int(row.get("on_interview", 0) or 0) == 1:
        return True
    if int(row.get("interview_stopped", 0) or 0) == 1:
        return True
    return int(row.get("viewed", 0) or 0) == 0


def _is_manual_feedback_reason(relevance_reason: str) -> bool:
    manual_reason = (relevance_reason or "").strip().lower()
    return manual_reason in {"manual_feedback=relevant", "manual_feedback=not relevant"}


def _rescore_row(
    db_path: str,
    profile: AppConfig,
    row: dict,
    *,
    skill_patterns: list[tuple[str, str]],
    easy_apply_cache: dict[str, bool],
    applied_company_keys: Optional[set[str]] = None,
) -> bool:
    rid = int(row.get("id", 0) or 0)
    if not rid:
        return False
    if _is_manual_feedback_reason(str(row.get("relevance_reason", ""))):
        return False

    cached_skills = get_job_skills_filtered(db_path, rid, profile)
    composed = f"{row.get('title') or ''}\n{row.get('company') or ''}\n{row.get('raw_text') or ''}"
    score, reason, relevant, category = score_relevance(
        composed,
        profile,
        skill_patterns=skill_patterns,
        source=str(row.get("source") or ""),
        position_link=str(row.get("position_link") or ""),
        easy_apply_cache=easy_apply_cache,
        cached_required_skills=cached_skills if cached_skills else None,
        company=str(row.get("company") or ""),
        applied_company_keys=applied_company_keys,
    )

    if int(row.get("applied", 0) or 0) == 1:
        relevant = 1
        category = "relevant"

    update_jobs_relevance(db_path, [(rid, score, reason, relevant, category)])
    return True


def rescore_jobs_if_active(db_path: str, profile: AppConfig, job_ids: list[int]) -> int:
    if not job_ids:
        return 0

    target_ids = {int(job_id) for job_id in job_ids if int(job_id or 0) > 0}
    if not target_ids:
        return 0

    rows = [
        row for row in get_jobs_for_active_rescore(db_path)
        if int(row.get("id", 0) or 0) in target_ids and job_in_active_rescore_scope(row)
    ]
    if not rows:
        return 0

    skill_patterns = _load_skill_patterns(db_path, profile)
    easy_apply_cache: dict[str, bool] = {}
    applied_company_keys = get_applied_pipeline_company_keys(db_path)
    rescored = 0
    for row in rows:
        if _rescore_row(
            db_path,
            profile,
            row,
            skill_patterns=skill_patterns,
            easy_apply_cache=easy_apply_cache,
            applied_company_keys=applied_company_keys,
        ):
            rescored += 1
    return rescored


def rescore_active_jobs(db_path: str, profile: AppConfig) -> int:
    rows = get_jobs_for_active_rescore(db_path)
    if not rows:
        return 0

    skill_patterns = _load_skill_patterns(db_path, profile)
    easy_apply_cache: dict[str, bool] = {}
    applied_company_keys = get_applied_pipeline_company_keys(db_path)
    rescored = 0
    for row in rows:
        if not job_in_active_rescore_scope(row):
            continue
        if _rescore_row(
            db_path,
            profile,
            row,
            skill_patterns=skill_patterns,
            easy_apply_cache=easy_apply_cache,
            applied_company_keys=applied_company_keys,
        ):
            rescored += 1
    return rescored


def apply_relevance(
    db_path: str, profile: AppConfig, prune_irrelevant: bool = False
) -> tuple[int, int]:
    rows_dict = get_jobs_for_scoring(db_path)
    rows = [(r["id"], r["source"], r["title"], r["company"], r["position_link"],
             r["raw_text"], r["relevance_reason"]) for r in rows_dict]
    relevant_count = 0

    skill_patterns = _load_skill_patterns(db_path, profile)

    easy_apply_cache: dict[str, bool] = {}
    applied_company_keys = get_applied_pipeline_company_keys(db_path)
    pending_updates: list[tuple[int, float, str, int, str]] = []

    for rid, source, title, company, position_link, raw_text, relevance_reason in rows:
        manual_reason = (relevance_reason or "").strip().lower()
        if manual_reason == "manual_feedback=relevant":
            relevant_count += 1
            continue
        if manual_reason == "manual_feedback=not relevant":
            continue

        cached_skills = get_job_skills_filtered(db_path, rid, profile) if rid else []
        composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
        score, reason, relevant, category = score_relevance(
            composed,
            profile,
            skill_patterns=skill_patterns,
            source=source or "",
            position_link=position_link or "",
            easy_apply_cache=easy_apply_cache,
            cached_required_skills=cached_skills if cached_skills else None,
            company=company or "",
            applied_company_keys=applied_company_keys,
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
    cached_skills = get_job_skills_filtered(db_path, rid, profile) if rid else []
    applied_company_keys = get_applied_pipeline_company_keys(db_path)

    composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
    score, reason, relevant, category = score_relevance(
        composed,
        profile,
        skill_patterns=skill_patterns,
        source=source or "",
        position_link=position_link or "",
        easy_apply_cache={},
        cached_required_skills=cached_skills if cached_skills else None,
        company=company or "",
        applied_company_keys=applied_company_keys,
    )

    if int(applied or 0) == 1:
        relevant = 1
        category = "relevant"

    update_jobs_relevance(db_path, [(rid, score, reason, relevant, category)])
    return True
