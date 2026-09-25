"""Skill block/delete API routes (single and batch)."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.db import delete_skill_from_db, ensure_db
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.extractors.skill_extractor.bad_cloud import on_skills_blocked, on_skills_forgiven
from spejder.jobs import rescore_jobs_if_active
from spejder.managers.profile_manager import (
    _block_skill_in_profile,
    _remove_skill_from_profile,
)
from spejder.server.context import ServerRuntime, get_runtime

router = APIRouter()


class SkillBlockRequest(BaseModel):
    skill: str


class SkillDeleteRequest(BaseModel):
    skill: str


class SkillForgiveRequest(BaseModel):
    skill: str


class SkillBatchRequest(BaseModel):
    skills: list[str]


def _normalize_skill_batch(skills: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized_skills: list[str] = []
    for raw in skills or []:
        skill = _normalize_skill_name(str(raw))
        key = _normalize_skill_name_key(skill)
        if not key or key in seen:
            continue
        seen.add(key)
        normalized_skills.append(skill)
    return normalized_skills


def _merge_db_deleted(totals: dict, deleted: dict) -> None:
    totals["skill_rows_deleted"] += int(deleted.get("skill_rows_deleted", 0))
    totals["job_skill_links_deleted"] += int(deleted.get("job_skill_links_deleted", 0))
    for job_id in deleted.get("affected_job_ids", []):
        totals["affected_job_ids"].add(int(job_id))


def _delete_skills_from_db(db_path: str, skills: list[str]) -> dict:
    db_deleted = {
        "skill_rows_deleted": 0,
        "job_skill_links_deleted": 0,
        "affected_job_ids": set(),
    }
    for skill in skills:
        _merge_db_deleted(db_deleted, delete_skill_from_db(db_path, skill))
    affected_job_ids = sorted(db_deleted.pop("affected_job_ids"))
    db_deleted["affected_job_ids"] = affected_job_ids
    return db_deleted


def _run_skill_block(runtime: ServerRuntime, skills: list[str], rebuild_reason: str, log_label: str) -> dict:
    block_info = {"blocked_added": 0, "removed": 0}
    newly_blocked: list[str] = []
    for skill in skills:
        info = _block_skill_in_profile(runtime.runtime_profile, skill)
        block_info["blocked_added"] += int(info.get("blocked_added", 0))
        block_info["removed"] += int(info.get("removed", 0))
        if info.get("blocked_added"):
            newly_blocked.append(skill)

    if newly_blocked:
        ensure_db(runtime.db_path)
        on_skills_blocked(runtime.runtime_profile, runtime.db_path, newly_blocked)

    runtime.persist_runtime_profile()
    runtime.reload_runtime_profile()
    db_deleted = _delete_skills_from_db(runtime.db_path, skills)
    rescored = rescore_jobs_if_active(
        runtime.db_path,
        runtime.runtime_profile,
        list(db_deleted.get("affected_job_ids", [])),
    )
    print(
        f"API: rescore_jobs_if_active after {log_label} "
        f"(rescored={rescored}, count={len(skills)})"
    )
    runtime.queue_dashboard_rebuild(reason=rebuild_reason)
    return {
        "skills": skills,
        "count": len(skills),
        "block_info": block_info,
        "db_deleted": db_deleted,
    }


def _run_skill_delete(runtime: ServerRuntime, skills: list[str], rebuild_reason: str, log_label: str) -> dict:
    profile_removed = {"removed": 0}
    for skill in skills:
        info = _remove_skill_from_profile(runtime.runtime_profile, skill)
        profile_removed["removed"] += int(info.get("removed", 0))

    runtime.persist_runtime_profile()
    runtime.reload_runtime_profile()
    db_deleted = _delete_skills_from_db(runtime.db_path, skills)
    rescored = rescore_jobs_if_active(
        runtime.db_path,
        runtime.runtime_profile,
        list(db_deleted.get("affected_job_ids", [])),
    )
    print(
        f"API: rescore_jobs_if_active after {log_label} "
        f"(rescored={rescored}, count={len(skills)})"
    )
    runtime.queue_dashboard_rebuild(reason=rebuild_reason)
    return {
        "skills": skills,
        "count": len(skills),
        "profile_removed": profile_removed,
        "db_deleted": db_deleted,
    }


@router.post("/api/skill/block")
def api_skill_block(req: SkillBlockRequest, runtime: ServerRuntime = Depends(get_runtime)):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    result = _run_skill_block(runtime, [skill], f"skill blocked {skill}", "skill block")
    return {
        "ok": True,
        "skill": skill,
        "block_info": result["block_info"],
        "db_deleted": result["db_deleted"],
    }


@router.post("/api/skill/delete")
def api_skill_delete(req: SkillDeleteRequest, runtime: ServerRuntime = Depends(get_runtime)):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    result = _run_skill_delete(
        runtime, [skill], f"skill deleted cleanup {skill}", "skill delete"
    )
    return {
        "ok": True,
        "skill": skill,
        "profile_removed": result["profile_removed"],
        "db_deleted": result["db_deleted"],
    }


@router.post("/api/skill/block-batch")
def api_skill_block_batch(
    req: SkillBatchRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    skills = _normalize_skill_batch(req.skills)
    if not skills:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skills is required"})

    result = _run_skill_block(
        runtime,
        skills,
        f"skill block batch ({len(skills)})",
        "skill block batch",
    )
    return {"ok": True, **result}


@router.post("/api/skill/delete-batch")
def api_skill_delete_batch(
    req: SkillBatchRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    skills = _normalize_skill_batch(req.skills)
    if not skills:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skills is required"})

    result = _run_skill_delete(
        runtime,
        skills,
        f"skill delete batch ({len(skills)})",
        "skill delete batch",
    )
    return {"ok": True, **result}


def _run_skill_forgive(runtime: ServerRuntime, skills: list[str], rebuild_reason: str) -> dict:
    ensure_db(runtime.db_path)
    forgive_info = on_skills_forgiven(runtime.runtime_profile, runtime.db_path, skills)
    runtime.persist_runtime_profile()
    runtime.reload_runtime_profile()
    runtime.queue_dashboard_rebuild(reason=rebuild_reason)
    return {
        "skills": forgive_info.get("skills", skills),
        "count": len(forgive_info.get("skills", skills)),
        "forgive_info": forgive_info,
    }


@router.post("/api/skill/forgive")
def api_skill_forgive(req: SkillForgiveRequest, runtime: ServerRuntime = Depends(get_runtime)):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    result = _run_skill_forgive(runtime, [skill], f"skill forgiven {skill}")
    return {
        "ok": True,
        "skill": skill,
        "forgive_info": result["forgive_info"],
    }


@router.post("/api/skill/forgive-batch")
def api_skill_forgive_batch(
    req: SkillBatchRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    skills = _normalize_skill_batch(req.skills)
    if not skills:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skills is required"})

    result = _run_skill_forgive(
        runtime,
        skills,
        f"skill forgive batch ({len(skills)})",
    )
    return {"ok": True, **result}
