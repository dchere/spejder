"""Job pipeline API routes (feedback, applied, interview, viewed, hidden)."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.db import (
    append_applied_job_raw_text,
    clear_job_skills_for_job,
    get_all_applied_jobs,
    set_job_applied,
    set_job_company_feedback,
    set_job_cover_letter,
    set_job_cover_letter_requested,
    set_job_feedback,
    set_job_hidden,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
)
from spejder.llm import LocalLLM
from spejder.server.context import ServerRuntime, get_runtime
from spejder.workflows.job_enrichment import (
    _translate_text_to_english_if_needed,
    materialize_job_skills,
)

router = APIRouter()


class FeedbackRequest(BaseModel):
    job_id: int = 0
    signal: str


class AppliedRequest(BaseModel):
    job_id: int = 0
    applied: bool


class InterviewRequest(BaseModel):
    job_id: int = 0
    on_interview: bool


class InterviewStoppedRequest(BaseModel):
    job_id: int = 0
    stopped: bool


class InterviewFeedbackRequest(BaseModel):
    job_id: int = 0
    feedback: str


class ViewedRequest(BaseModel):
    job_id: int = 0
    viewed: bool


class HiddenRequest(BaseModel):
    job_id: int = 0
    hidden: bool


class AppliedRawTextRequest(BaseModel):
    job_id: int = 0
    text: str


class CoverLetterRequestToggle(BaseModel):
    job_id: int = 0
    requested: bool


class CoverLetterSaveRequest(BaseModel):
    job_id: int = 0
    text: str


@router.post("/api/feedback")
def api_feedback(req: FeedbackRequest, runtime: ServerRuntime = Depends(get_runtime)):
    signal = req.signal.strip().lower()
    if signal not in {"relevant", "not relevant"}:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "signal must be 'relevant' or 'not relevant'"},
        )

    set_job_feedback(runtime.db_path, req.job_id, signal)
    print(f"API: Set feedback signal={signal} for job_id={req.job_id}")
    runtime.queue_dashboard_rebuild(reason=f"feedback {signal} on job {req.job_id}")
    return {"ok": True, "job_id": req.job_id, "signal": signal, "profile_learning": {"queued": True}}


@router.post("/api/applied")
def api_applied(req: AppliedRequest, runtime: ServerRuntime = Depends(get_runtime)):
    set_job_applied(runtime.db_path, req.job_id, req.applied)
    print(f"API: Set applied={req.applied} for job_id={req.job_id}")
    learning_info = {"queued": req.applied} if req.applied else None
    if req.applied:
        runtime.queue_dashboard_rebuild(reason=f"applied to job {req.job_id}")
    else:
        runtime.queue_dashboard_rebuild(reason=f"unapplied job {req.job_id}")
    return {"ok": True, "job_id": req.job_id, "applied": req.applied, "profile_learning": learning_info}


@router.post("/api/interview")
def api_interview(req: InterviewRequest, runtime: ServerRuntime = Depends(get_runtime)):
    saved = set_job_on_interview(runtime.db_path, req.job_id, req.on_interview)
    if not saved:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "job not found or not applied"},
        )
    print(f"API: Set on_interview={req.on_interview} for job_id={req.job_id}")
    runtime.queue_dashboard_rebuild(
        reason=f"interview {'on' if req.on_interview else 'off'} job {req.job_id}"
    )
    return {"ok": True, "job_id": req.job_id, "on_interview": req.on_interview}


@router.post("/api/interview/stopped")
def api_interview_stopped(
    req: InterviewStoppedRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    saved = set_job_interview_stopped(runtime.db_path, req.job_id, req.stopped)
    if not saved:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "job not found or not applied"},
        )
    print(f"API: Set interview_stopped={req.stopped} for job_id={req.job_id}")
    runtime.queue_dashboard_rebuild(
        reason=f"interview stopped {'on' if req.stopped else 'off'} job {req.job_id}"
    )
    return {"ok": True, "job_id": req.job_id, "stopped": req.stopped}


@router.post("/api/interview/feedback")
def api_interview_feedback(
    req: InterviewFeedbackRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    saved = set_job_company_feedback(runtime.db_path, req.job_id, req.feedback.strip())
    if not saved:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "job not found, not applied, or not stopped"},
        )
    print(f"API: Set company_feedback for job_id={req.job_id}")
    runtime.queue_dashboard_rebuild(reason=f"company feedback job {req.job_id}")
    return {"ok": True, "job_id": req.job_id, "feedback": req.feedback.strip()}


@router.post("/api/viewed")
def api_viewed(req: ViewedRequest, runtime: ServerRuntime = Depends(get_runtime)):
    print(f"API: Marked job_id={req.job_id} as viewed={req.viewed}")
    set_job_viewed(runtime.db_path, req.job_id, req.viewed)
    runtime.queue_dashboard_rebuild(reason=f"job {req.job_id} marked viewed")
    return {"ok": True, "job_id": req.job_id, "viewed": req.viewed}


@router.post("/api/hidden")
def api_hidden(req: HiddenRequest, runtime: ServerRuntime = Depends(get_runtime)):
    print(f"API: Marked job_id={req.job_id} as hidden={req.hidden}")
    set_job_hidden(runtime.db_path, req.job_id, req.hidden)
    reason = (
        f"job {req.job_id} marked hidden"
        if req.hidden
        else f"job {req.job_id} unhidden"
    )
    runtime.queue_dashboard_rebuild(reason=reason)
    return {"ok": True, "job_id": req.job_id, "hidden": req.hidden}


@router.post("/api/applied/raw-text")
def api_applied_raw_text(
    req: AppliedRawTextRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    text = req.text.strip()
    if not text:
        return JSONResponse(status_code=400, content={"ok": False, "error": "text is required"})

    text = _translate_text_to_english_if_needed(
        text, runtime_profile=runtime.runtime_profile
    )

    saved = append_applied_job_raw_text(runtime.db_path, req.job_id, text)
    print(f"API: append_applied_job_raw_text job_id={req.job_id} saved={saved}")
    if not saved:
        return JSONResponse(
            status_code=400, content={"ok": False, "error": "job not found or not applied"}
        )

    clear_job_skills_for_job(runtime.db_path, req.job_id)
    llm_for_manual = (
        LocalLLM(
            model_path=runtime.model_path,
            n_ctx=int(runtime.runtime_profile.n_ctx),
            verbose=runtime.cli_verbose,
        )
        if runtime.model_path
        else None
    )

    applied_rows = get_all_applied_jobs(runtime.db_path, limit=0)
    target_row = next(
        (r for r in applied_rows if int(r.get("id", 0) or 0) == req.job_id), None
    )

    if target_row is not None:
        materialize_job_skills(
            runtime.db_path,
            target_row,
            llm=llm_for_manual,
            runtime_profile=runtime.runtime_profile,
            rescore=True,
            first_materialize=True,
        )
        runtime.queue_dashboard_rebuild(reason=f"manual raw text job {req.job_id}")
        return {
            "ok": True,
            "job_id": req.job_id,
            "signal": "applied_raw_text_added",
            "profile_learning": None,
        }

    print(
        f"API: applied raw-text job_id={req.job_id} saved but row missing for materialize"
    )
    return JSONResponse(
        status_code=500,
        content={
            "ok": False,
            "error": "job saved but enrichment row missing; skills cache was cleared",
        },
    )


@router.post("/api/applied/cover-letter/request")
def api_applied_cover_letter_request(
    req: CoverLetterRequestToggle, runtime: ServerRuntime = Depends(get_runtime)
):
    saved = set_job_cover_letter_requested(runtime.db_path, req.job_id, req.requested)
    print(
        f"API: set_job_cover_letter_requested job_id={req.job_id} "
        f"requested={req.requested} saved={saved}"
    )
    if not saved:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "job not found, not applied, or cover letter already saved",
            },
        )
    runtime.queue_dashboard_rebuild(
        reason=f"cover letter requested={'on' if req.requested else 'off'} job {req.job_id}"
    )
    return {"ok": True, "job_id": req.job_id, "requested": req.requested}


@router.post("/api/applied/cover-letter")
def api_applied_cover_letter(
    req: CoverLetterSaveRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    text = req.text.strip()
    if not text:
        return JSONResponse(status_code=400, content={"ok": False, "error": "text is required"})

    saved = set_job_cover_letter(runtime.db_path, req.job_id, text)
    print(f"API: set_job_cover_letter job_id={req.job_id} saved={saved}")
    if not saved:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "job not found, not applied, cover letter not requested, or already saved",
            },
        )
    runtime.queue_dashboard_rebuild(reason=f"cover letter job {req.job_id}")
    return {"ok": True, "job_id": req.job_id, "signal": "cover_letter_saved"}
