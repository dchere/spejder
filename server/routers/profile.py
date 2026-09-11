"""Profile GET/save API routes."""

from typing import Any

from fastapi import APIRouter, Body, Depends
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from spejder.managers.profile_editor import (
    build_profile_get_response,
    save_profile_updates,
    validation_errors_by_field,
)
from spejder.server.context import ServerRuntime, get_runtime

router = APIRouter()


@router.get("/api/profile")
def api_profile_get(runtime: ServerRuntime = Depends(get_runtime)):
    return build_profile_get_response(runtime.runtime_profile)


@router.post("/api/profile/save")
def api_profile_save(
    payload: Any = Body(...), runtime: ServerRuntime = Depends(get_runtime)
):
    if not isinstance(payload, dict):
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": "body must be a JSON object"},
        )
    try:
        save_profile_updates(
            runtime.runtime_profile,
            payload,
            runtime.profile_path,
            runtime.reload_runtime_profile,
        )
    except ValidationError as exc:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "error": "validation failed",
                "errors": validation_errors_by_field(exc),
            },
        )
    except (ValueError, TypeError) as exc:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "error": str(exc)},
        )
    except OSError as exc:
        print(f"API: profile save disk error: {exc}")
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": "failed to write profile"},
        )
    print(f"API: saved profile to {runtime.profile_path}")
    return {
        "ok": True,
        "values": runtime.runtime_profile.model_dump(),
    }
