"""Portrait API routes."""

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spejder.llm import LocalLLM
from spejder.server.context import ServerRuntime, get_runtime
from spejder.workflows.user_portrait import (
    generate_portrait_draft,
    load_portrait,
    portrait_file_path,
    portrait_has_context,
    render_portrait_diff_html,
    save_portrait,
)

router = APIRouter()


class PortraitSaveRequest(BaseModel):
    text: str


@router.get("/api/portrait")
def api_portrait_get(runtime: ServerRuntime = Depends(get_runtime)):
    text = load_portrait(portrait_file_path(runtime.runtime_profile))
    return {"ok": True, "text": text}


@router.post("/api/portrait/save")
def api_portrait_save(
    req: PortraitSaveRequest, runtime: ServerRuntime = Depends(get_runtime)
):
    path = portrait_file_path(runtime.runtime_profile)
    save_portrait(path, req.text)
    print(f"API: saved portrait to {path} (chars={len(req.text)})")
    return {"ok": True, "text": req.text}


@router.post("/api/portrait/generate")
def api_portrait_generate(runtime: ServerRuntime = Depends(get_runtime)):
    if not runtime.portrait_generate_lock.acquire(blocking=False):
        return JSONResponse(
            status_code=409,
            content={"ok": False, "error": "portrait generation already in progress"},
        )
    try:
        if not runtime.model_path:
            return JSONResponse(
                status_code=503,
                content={"ok": False, "error": "default_model is not configured in profile"},
            )
        if not portrait_has_context(runtime.db_path, runtime.runtime_profile):
            return JSONResponse(
                status_code=400,
                content={
                    "ok": False,
                    "error": "no portrait data (CV, skills, or applied jobs required)",
                },
            )
        path = portrait_file_path(runtime.runtime_profile)
        committed = load_portrait(path)
        llm = LocalLLM(
            model_path=runtime.model_path,
            n_ctx=int(runtime.runtime_profile.n_ctx),
            verbose=runtime.cli_verbose,
        )
        try:
            draft = generate_portrait_draft(
                llm,
                runtime.db_path,
                runtime.runtime_profile,
                current_portrait=committed,
            )
        except ValueError as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
        except Exception as exc:
            print(f"API: portrait generate failed: {exc}")
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": "portrait generation failed"},
            )
        diff_html = render_portrait_diff_html(committed, draft)
        print(f"API: portrait draft generated (chars={len(draft)})")
        return {
            "ok": True,
            "draft": draft,
            "committed": committed,
            "diff_html": diff_html,
        }
    finally:
        runtime.portrait_generate_lock.release()
