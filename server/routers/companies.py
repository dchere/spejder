"""Company catalog API routes (merge / rename / delete / promote / avoid)."""

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from spejder.db.companies import (
    company_exists,
    count_company_jobs,
    promote_status_for_job,
    set_company_avoid,
    set_company_promote,
)
from spejder.jobs import rescore_active_jobs
from spejder.server.context import ServerRuntime, get_runtime
from spejder.workflows.company_catalog import (
    delete_companies,
    delete_company,
    merge_companies,
    rename_company,
)

router = APIRouter()


class CompanyMergeRequest(BaseModel):
    company_ids: list[int] = Field(default_factory=list)
    display_name: str = ""


class CompanyRenameRequest(BaseModel):
    company_id: int
    display_name: str = ""


class CompanyDeleteRequest(BaseModel):
    company_id: int


class CompanyDeleteBatchRequest(BaseModel):
    company_ids: list[int] = Field(default_factory=list)


class CompanyPromoteRequest(BaseModel):
    company_id: int
    promote: bool


class CompanyAvoidRequest(BaseModel):
    company_id: int
    avoid: bool
    bulk: bool = False


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"ok": False, "error": message})


@router.post("/api/company/merge")
def api_company_merge(
    body: CompanyMergeRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    display_name = (body.display_name or "").strip()
    ids = [int(cid) for cid in (body.company_ids or []) if int(cid) > 0]
    # Dedup while preserving order
    seen: set[int] = set()
    unique_ids: list[int] = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        unique_ids.append(cid)
    if len(unique_ids) < 2:
        return _error(400, "Select at least two companies to merge")
    if not display_name:
        return _error(400, "display_name is required")
    for cid in unique_ids:
        if not company_exists(runtime.db_path, cid):
            return _error(400, f"company_id not found: {cid}")
    try:
        result = merge_companies(runtime.db_path, unique_ids, display_name)
    except ValueError as exc:
        return _error(400, str(exc))
    runtime.queue_dashboard_rebuild(reason="company merge")
    return {"ok": True, **result}


@router.post("/api/company/rename")
def api_company_rename(
    body: CompanyRenameRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    display_name = (body.display_name or "").strip()
    company_id = int(body.company_id)
    if company_id <= 0:
        return _error(400, "company_id is required")
    if not display_name:
        return _error(400, "display_name is required")
    if not company_exists(runtime.db_path, company_id):
        return _error(400, f"company_id not found: {company_id}")
    try:
        result = rename_company(runtime.db_path, company_id, display_name)
    except ValueError as exc:
        return _error(400, str(exc))
    runtime.queue_dashboard_rebuild(reason="company rename")
    return {"ok": True, **result}


@router.post("/api/company/delete")
def api_company_delete(
    body: CompanyDeleteRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    company_id = int(body.company_id)
    if company_id <= 0:
        return _error(400, "company_id is required")
    # Pre-count for response when still present (UI confirms with job count).
    jobs_before = 0
    if company_exists(runtime.db_path, company_id):
        jobs_before = count_company_jobs(runtime.db_path, company_id)
    result = delete_company(runtime.db_path, company_id)
    if not result.get("already_gone"):
        runtime.queue_dashboard_rebuild(reason="company delete")
    return {
        "ok": True,
        **result,
        "jobs_before": jobs_before,
    }


@router.post("/api/company/delete-batch")
def api_company_delete_batch(
    body: CompanyDeleteBatchRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    ids = [int(cid) for cid in (body.company_ids or []) if int(cid) > 0]
    seen: set[int] = set()
    unique_ids: list[int] = []
    for cid in ids:
        if cid in seen:
            continue
        seen.add(cid)
        unique_ids.append(cid)
    if not unique_ids:
        return _error(400, "Select at least one company to delete")
    result = delete_companies(runtime.db_path, unique_ids)
    if result.get("deleted_ids"):
        runtime.queue_dashboard_rebuild(reason="company delete batch")
    return {"ok": True, **result}


@router.get("/api/company/promote-status")
def api_company_promote_status(
    job_id: int = Query(...),
    runtime: ServerRuntime = Depends(get_runtime),
):
    jid = int(job_id)
    if jid <= 0:
        return _error(400, "job_id is required")
    status = promote_status_for_job(runtime.db_path, jid)
    return {"ok": True, **status}


@router.post("/api/company/promote")
def api_company_promote(
    body: CompanyPromoteRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    company_id = int(body.company_id)
    if company_id <= 0:
        return _error(400, "company_id is required")
    if not company_exists(runtime.db_path, company_id):
        return _error(400, f"company_id not found: {company_id}")
    try:
        result = set_company_promote(
            runtime.db_path, company_id, bool(body.promote), touch=True
        )
    except ValueError as exc:
        return _error(400, str(exc))
    if result.get("changed"):
        rescored = rescore_active_jobs(runtime.db_path, runtime.runtime_profile)
        print(
            f"API: rescore_active_jobs after company promote "
            f"(company_id={company_id}, rescored={rescored})"
        )
        runtime.queue_dashboard_rebuild(
            reason=(
                f"company {company_id} "
                f"{'promoted' if body.promote else 'unpromoted'}"
            )
        )
    return {"ok": True, **result}


@router.post("/api/company/avoid")
def api_company_avoid(
    body: CompanyAvoidRequest,
    runtime: ServerRuntime = Depends(get_runtime),
):
    company_id = int(body.company_id)
    if company_id <= 0:
        return _error(400, "company_id is required")
    if not company_exists(runtime.db_path, company_id):
        return _error(400, f"company_id not found: {company_id}")
    try:
        result = set_company_avoid(
            runtime.db_path,
            company_id,
            bool(body.avoid),
            bulk=bool(body.bulk),
        )
    except ValueError as exc:
        return _error(400, str(exc))
    if result.get("changed"):
        # Rebuild only — no rescore (Avoid does not change relevance).
        runtime.queue_dashboard_rebuild(
            reason=(
                f"company {company_id} "
                f"{'avoided' if body.avoid else 'unavoided'}"
            )
        )
    return {"ok": True, **result}
