"""Skill list API routes (user/learn/unwanted flag toggles)."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.jobs import rescore_active_jobs
from spejder.managers.profile_manager import _toggle_exclusive_profile_skill
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
