"""Skill list API routes (user/learn/unwanted/block/delete and batches)."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.db import delete_skill_from_db, ensure_db
from spejder.db.utils import _normalize_skill_name_key
from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.extractors.skill_extractor.bad_cloud import on_skills_blocked
from spejder.jobs import rescore_active_jobs, rescore_jobs_if_active
from spejder.managers.profile_manager import (
    _block_skill_in_profile,
    _remove_skill_from_profile,
    _toggle_exclusive_profile_skill,
)
from spejder.server.context import ServerRuntime, get_runtime

router = APIRouter()


class SkillUserRequest(BaseModel):
    skill: str
    has_skill: bool


class SkillLearnRequest(BaseModel):
    skill: str
    learn: bool


class SkillUnwantedRequest(BaseModel):
    skill: str
    unwanted: bool


class SkillBlockRequest(BaseModel):
    skill: str


class SkillDeleteRequest(BaseModel):
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


@router.post("/api/skill/user")
def api_skill_user(req: SkillUserRequest, runtime: ServerRuntime = Depends(get_runtime)):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    changed, _dropped = _toggle_exclusive_profile_skill(
        runtime.runtime_profile,
        "user_skills",
        skill,
        req.has_skill,
        drop_from=("unwanted_skills",) if req.has_skill else (),
    )
    if changed:
        runtime.persist_runtime_profile()
        runtime.reload_runtime_profile()
        rescored = rescore_active_jobs(runtime.db_path, runtime.runtime_profile)
        print(f"API: rescore_active_jobs after user skill toggle (rescored={rescored})")
        runtime.queue_dashboard_rebuild(
            reason=f"skill {'checked' if req.has_skill else 'unchecked'} {skill}"
        )
    return {"ok": True, "skill": skill, "has_skill": req.has_skill, "changed": bool(changed)}


@router.post("/api/skill/learn")
def api_skill_learn(req: SkillLearnRequest, runtime: ServerRuntime = Depends(get_runtime)):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    changed, dropped_unwanted = _toggle_exclusive_profile_skill(
        runtime.runtime_profile,
        "missing_skills_suggestions",
        skill,
        req.learn,
        drop_from=("unwanted_skills",) if req.learn else (),
    )
    if changed:
        runtime.persist_runtime_profile()
        runtime.reload_runtime_profile()
        if dropped_unwanted:
            rescored = rescore_active_jobs(runtime.db_path, runtime.runtime_profile)
            print(
                f"API: rescore_active_jobs after learn cleared unwanted (rescored={rescored})"
            )
        runtime.queue_dashboard_rebuild(
            reason=f"skill learn {'on' if req.learn else 'off'} {skill}"
        )
    return {"ok": True, "skill": skill, "learn": req.learn, "changed": bool(changed)}


@router.post("/api/skill/unwanted")
def api_skill_unwanted(
    req: SkillUnwantedRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    skill = _normalize_skill_name(req.skill)
    if not skill:
        return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

    changed, _dropped = _toggle_exclusive_profile_skill(
        runtime.runtime_profile,
        "unwanted_skills",
        skill,
        req.unwanted,
        drop_from=("user_skills", "missing_skills_suggestions") if req.unwanted else (),
    )
    if changed:
        runtime.persist_runtime_profile()
        runtime.reload_runtime_profile()
        rescored = rescore_active_jobs(runtime.db_path, runtime.runtime_profile)
        print(f"API: rescore_active_jobs after unwanted skill toggle (rescored={rescored})")
        runtime.queue_dashboard_rebuild(
            reason=f"skill unwanted {'on' if req.unwanted else 'off'} {skill}"
        )
    return {"ok": True, "skill": skill, "unwanted": req.unwanted, "changed": bool(changed)}


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
