"""API Server for Spejder"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from spejder.db import (
    append_applied_job_raw_text,
    clear_job_skills_for_job,
    delete_skill_from_db,
    get_all_applied_jobs,
    get_jobs_by_company,
    set_job_applied,
    set_job_company_feedback,
    set_job_cover_letter,
    set_job_cover_letter_requested,
    set_job_feedback,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
)
from .extractors.skill_extractor import _normalize_skill_name
from .llm import LocalLLM
from .managers.dashboard_manager import _render_company_dashboard_html
from .managers.profile_manager import (
    _block_skill_in_profile,
    _remove_skill_from_profile,
    _toggle_profile_skill,
)
from .workflows.job_enrichment import (
    _translate_text_to_english_if_needed,
    materialize_job_skills,
)


def create_app(
    db_path: str,
    profile_path: str,
    runtime_profile: dict,
    model_path: str,
    report_dir: str,
    get_title_translation_llm,
    persist_runtime_profile,
    reload_runtime_profile,
    queue_dashboard_rebuild,
    cli_verbose: bool
) -> FastAPI:
    app = FastAPI(title="Spejder GUI Server")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    class FeedbackRequest(BaseModel):
        job_id: int = 0
        signal: str

    @app.post("/api/feedback")
    def api_feedback(req: FeedbackRequest):
        signal = req.signal.strip().lower()
        if signal not in {"relevant", "not relevant"}:
            return JSONResponse(status_code=400, content={"ok": False, "error": "signal must be 'relevant' or 'not relevant'"})

        set_job_feedback(db_path, req.job_id, signal)
        print(f"API: Set feedback signal={signal} for job_id={req.job_id}")
        queue_dashboard_rebuild(reason=f"feedback {signal} on job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "signal": signal, "profile_learning": {"queued": True}}

    class AppliedRequest(BaseModel):
        job_id: int = 0
        applied: bool

    @app.post("/api/applied")
    def api_applied(req: AppliedRequest):
        set_job_applied(db_path, req.job_id, req.applied)
        print(f"API: Set applied={req.applied} for job_id={req.job_id}")
        learning_info = {"queued": req.applied} if req.applied else None
        if req.applied:
            queue_dashboard_rebuild(reason=f"applied to job {req.job_id}")
        else:
            queue_dashboard_rebuild(reason=f"unapplied job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "applied": req.applied, "profile_learning": learning_info}

    class InterviewRequest(BaseModel):
        job_id: int = 0
        on_interview: bool

    @app.post("/api/interview")
    def api_interview(req: InterviewRequest):
        saved = set_job_on_interview(db_path, req.job_id, req.on_interview)
        if not saved:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "job not found or not applied"},
            )
        print(f"API: Set on_interview={req.on_interview} for job_id={req.job_id}")
        queue_dashboard_rebuild(reason=f"interview {'on' if req.on_interview else 'off'} job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "on_interview": req.on_interview}

    class InterviewStoppedRequest(BaseModel):
        job_id: int = 0
        stopped: bool

    @app.post("/api/interview/stopped")
    def api_interview_stopped(req: InterviewStoppedRequest):
        saved = set_job_interview_stopped(db_path, req.job_id, req.stopped)
        if not saved:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "job not found or not applied"},
            )
        print(f"API: Set interview_stopped={req.stopped} for job_id={req.job_id}")
        queue_dashboard_rebuild(reason=f"interview stopped {'on' if req.stopped else 'off'} job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "stopped": req.stopped}

    class InterviewFeedbackRequest(BaseModel):
        job_id: int = 0
        feedback: str

    @app.post("/api/interview/feedback")
    def api_interview_feedback(req: InterviewFeedbackRequest):
        saved = set_job_company_feedback(db_path, req.job_id, req.feedback.strip())
        if not saved:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "job not found, not applied, or not stopped"},
            )
        print(f"API: Set company_feedback for job_id={req.job_id}")
        queue_dashboard_rebuild(reason=f"company feedback job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "feedback": req.feedback.strip()}

    class ViewedRequest(BaseModel):
        job_id: int = 0
        viewed: bool

    @app.post("/api/viewed")
    def api_viewed(req: ViewedRequest):
        print(f"API: Marked job_id={req.job_id} as viewed={req.viewed}")
        set_job_viewed(db_path, req.job_id, req.viewed)
        queue_dashboard_rebuild(reason=f"job {req.job_id} marked viewed")
        return {"ok": True, "job_id": req.job_id, "viewed": req.viewed}

    class AppliedRawTextRequest(BaseModel):
        job_id: int = 0
        text: str

    @app.post("/api/applied/raw-text")
    def api_applied_raw_text(req: AppliedRawTextRequest):
        text = req.text.strip()
        if not text:
            return JSONResponse(status_code=400, content={"ok": False, "error": "text is required"})

        text = _translate_text_to_english_if_needed(text, runtime_profile=runtime_profile)

        saved = append_applied_job_raw_text(db_path, req.job_id, text)
        print(f"API: append_applied_job_raw_text job_id={req.job_id} saved={saved}")
        if not saved:
            return JSONResponse(status_code=400, content={"ok": False, "error": "job not found or not applied"})

        clear_job_skills_for_job(db_path, req.job_id)
        llm_for_manual = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=cli_verbose) if model_path else None

        applied_rows = get_all_applied_jobs(db_path, limit=0)
        target_row = next((r for r in applied_rows if int(r.get("id", 0) or 0) == req.job_id), None)

        if target_row is not None:
            materialize_job_skills(
                db_path,
                target_row,
                llm=llm_for_manual,
                runtime_profile=runtime_profile,
                limit=10,
                rescore=True,
            )
            queue_dashboard_rebuild(reason=f"manual raw text job {req.job_id}")

        return {"ok": True, "job_id": req.job_id, "signal": "applied_raw_text_added", "profile_learning": None}

    class CoverLetterRequestToggle(BaseModel):
        job_id: int = 0
        requested: bool

    @app.post("/api/applied/cover-letter/request")
    def api_applied_cover_letter_request(req: CoverLetterRequestToggle):
        saved = set_job_cover_letter_requested(db_path, req.job_id, req.requested)
        print(
            f"API: set_job_cover_letter_requested job_id={req.job_id} "
            f"requested={req.requested} saved={saved}"
        )
        if not saved:
            return JSONResponse(
                status_code=400,
                content={"ok": False, "error": "job not found, not applied, or cover letter already saved"},
            )
        queue_dashboard_rebuild(
            reason=f"cover letter requested={'on' if req.requested else 'off'} job {req.job_id}"
        )
        return {"ok": True, "job_id": req.job_id, "requested": req.requested}

    class CoverLetterSaveRequest(BaseModel):
        job_id: int = 0
        text: str

    @app.post("/api/applied/cover-letter")
    def api_applied_cover_letter(req: CoverLetterSaveRequest):
        text = req.text.strip()
        if not text:
            return JSONResponse(status_code=400, content={"ok": False, "error": "text is required"})

        saved = set_job_cover_letter(db_path, req.job_id, text)
        print(f"API: set_job_cover_letter job_id={req.job_id} saved={saved}")
        if not saved:
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "job not found, not applied, cover letter not requested, or already saved",
                },
            )
        queue_dashboard_rebuild(reason=f"cover letter job {req.job_id}")
        return {"ok": True, "job_id": req.job_id, "signal": "cover_letter_saved"}

    class SkillUserRequest(BaseModel):
        skill: str
        has_skill: bool

    @app.post("/api/skill/user")
    def api_skill_user(req: SkillUserRequest):
        skill = _normalize_skill_name(req.skill)
        if not skill:
            return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

        changed = _toggle_profile_skill(runtime_profile, "user_skills", skill, req.has_skill)
        if changed:
            persist_runtime_profile()
            reload_runtime_profile()
            queue_dashboard_rebuild(reason=f"skill {'checked' if req.has_skill else 'unchecked'} {skill}")
        return {"ok": True, "skill": skill, "has_skill": req.has_skill, "changed": bool(changed)}

    class SkillLearnRequest(BaseModel):
        skill: str
        learn: bool

    @app.post("/api/skill/learn")
    def api_skill_learn(req: SkillLearnRequest):
        skill = _normalize_skill_name(req.skill)
        if not skill:
            return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

        changed = _toggle_profile_skill(runtime_profile, "missing_skills_suggestions", skill, req.learn)
        if changed:
            persist_runtime_profile()
            reload_runtime_profile()
            queue_dashboard_rebuild(reason=f"skill learn {'on' if req.learn else 'off'} {skill}")
        return {"ok": True, "skill": skill, "learn": req.learn, "changed": bool(changed)}

    class SkillBlockRequest(BaseModel):
        skill: str

    @app.post("/api/skill/block")
    def api_skill_block(req: SkillBlockRequest):
        skill = _normalize_skill_name(req.skill)
        if not skill:
            return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

        block_info = _block_skill_in_profile(runtime_profile, skill)
        persist_runtime_profile()
        reload_runtime_profile()
        db_deleted = delete_skill_from_db(db_path, skill)
        queue_dashboard_rebuild(reason=f"skill blocked {skill}")
        return {"ok": True, "skill": skill, "block_info": block_info, "db_deleted": db_deleted}

    class SkillDeleteRequest(BaseModel):
        skill: str

    @app.post("/api/skill/delete")
    def api_skill_delete(req: SkillDeleteRequest):
        skill = _normalize_skill_name(req.skill)
        if not skill:
            return JSONResponse(status_code=400, content={"ok": False, "error": "skill is required"})

        profile_removed = _remove_skill_from_profile(runtime_profile, skill)
        persist_runtime_profile()
        reload_runtime_profile()
        db_deleted = delete_skill_from_db(db_path, skill)
        queue_dashboard_rebuild(reason=f"skill deleted cleanup {skill}")
        return {"ok": True, "skill": skill, "profile_removed": profile_removed, "db_deleted": db_deleted}

    @app.get("/company.html")
    def company_page(company: str = ""):
        company_name = company.strip()
        if not company_name:
            return HTMLResponse(status_code=400, content='<!doctype html><html lang="en"><head><meta charset="utf-8" /><title>Company Positions</title></head><body><p>Missing company name.</p><p><a href="/report.html">Back to full report</a></p></body></html>')

        company_items = get_jobs_by_company(db_path, company_name, limit=0)
        html_content = _render_company_dashboard_html(company_name, company_items)
        return HTMLResponse(content=html_content)

    # Serve static files from report_dir at the root
    app.mount("/", StaticFiles(directory=report_dir, html=True), name="static")

    return app

def start_server(host, port, app_factory_kwargs):
    import uvicorn
    app = create_app(**app_factory_kwargs)

    max_port_attempts = 20
    selected_port = port

    for port_offset in range(max_port_attempts + 1):
        candidate_port = port + port_offset
        try:
            # We can't catch port-already-in-use exceptions smoothly with uvicorn.run directly
            # unless we try binding a socket first. Let's just bind to test.
            import socket
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((host, candidate_port))
                s.close()

            selected_port = candidate_port
            break
        except OSError as exc:
            if getattr(exc, "errno", None) == 98 and port_offset < max_port_attempts:
                continue
            raise OSError(f"Address in use for all tried ports: {port}-{port + max_port_attempts}") from exc

    if selected_port != port:
        print(f"Requested port {port} is busy; using port {selected_port} instead.")

    report_url = f"http://{host}:{selected_port}/report.html"
    print(f"Serving GUI at {report_url}")

    # Check if we should open browser (relying on no_open logic inside the caller)
    uvicorn.run(app, host=host, port=selected_port, log_level="info" if app_factory_kwargs.get("cli_verbose") else "warning")
