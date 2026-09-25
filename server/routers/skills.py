"""Skill list API routes (user/learn/unwanted flag toggles + CV sync)."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.core import resolve_user_path
from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.extractors.skill_extractor.user_sync import sync_user_skills
from spejder.jobs import rescore_active_jobs
from spejder.llm import LocalLLM
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


@router.post("/api/skill/sync-from-cv")
def api_skill_sync_from_cv(runtime: ServerRuntime = Depends(get_runtime)):
    """Merge CV-extracted skills into profile ``user_skills`` (same as CLI sync-user-skills)."""
    if not runtime.cv_skills_sync_lock.acquire(blocking=False):
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": "CV skills sync already in progress"},
        )
    try:
        if not runtime.model_path:
            return JSONResponse(
                status_code=503,
                content={"ok": False, "error": "default_model is not configured in profile"},
            )
        cv_path = resolve_user_path(
            str(getattr(runtime.runtime_profile, "default_cv_path", None) or "./CV")
        )
        llm = LocalLLM(
            model_path=runtime.model_path,
            n_ctx=int(runtime.runtime_profile.n_ctx),
            verbose=runtime.cli_verbose,
        )
        try:
            result = sync_user_skills(
                profile=runtime.profile_path,
                db=runtime.db_path,
                model=runtime.model_path,
                cv=cv_path,
                quiet_model=not runtime.cli_verbose,
                llm=llm,
            )
        except ValueError as exc:
            return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})
        except Exception as exc:
            print(f"API: skill sync-from-cv failed: {exc}")
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "CV skills sync failed"},
            )
        if not result.get("ok"):
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": result.get("error") or "CV skills sync failed",
                    "extracted": int(result.get("extracted", 0) or 0),
                    "total_user_skills": int(result.get("total_user_skills", 0) or 0),
                },
            )
        runtime.reload_runtime_profile()
        rescored = rescore_active_jobs(runtime.db_path, runtime.runtime_profile)
        print(
            f"API: sync-from-cv extracted={result.get('extracted')} "
            f"total={result.get('total_user_skills')} rescored={rescored}"
        )
        runtime.queue_dashboard_rebuild(reason="skills synced from CV")
        return {
            "ok": True,
            "extracted": int(result.get("extracted", 0) or 0),
            "total_user_skills": int(result.get("total_user_skills", 0) or 0),
            "top_extracted": list(result.get("top_extracted") or []),
            "rescored": int(rescored),
        }
    finally:
        runtime.cv_skills_sync_lock.release()
