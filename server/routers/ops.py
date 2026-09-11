"""Ops API routes (report rebuild, inbox sync) and company dashboard page."""

import os
from email.utils import formatdate

from fastapi import APIRouter, Depends
from fastapi.responses import HTMLResponse, JSONResponse

from spejder.db import get_jobs_by_company, get_viewed_today_jobs, local_day_start_utc_iso
from spejder.managers.dashboard_manager import _render_company_dashboard_html
from spejder.server.context import ServerRuntime, get_runtime

router = APIRouter()


def _report_html_mtime_http(report_dir: str) -> str:
    report_path = os.path.join(report_dir, "report.html")
    if not os.path.isfile(report_path):
        return ""
    return formatdate(os.path.getmtime(report_path), usegmt=True)


@router.post("/api/report/rebuild")
def api_report_rebuild(runtime: ServerRuntime = Depends(get_runtime)):
    runtime.queue_dashboard_rebuild(reason="manual rebuild")
    return {"ok": True, "queued": True}


@router.get("/api/report/status")
def api_report_status(runtime: ServerRuntime = Depends(get_runtime)):
    return {
        "ok": True,
        "idle": runtime.get_report_rebuild_idle(),
        "last_modified": _report_html_mtime_http(runtime.report_dir),
    }


@router.post("/api/inbox/sync")
def api_inbox_sync(runtime: ServerRuntime = Depends(get_runtime)):
    if runtime.trigger_inbox_sync is None:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "inbox sync not available"},
        )
    result = runtime.trigger_inbox_sync()
    if not result.get("ok"):
        return JSONResponse(status_code=409, content=result)
    return {"ok": True, "started": True}


@router.get("/api/inbox/sync/status")
def api_inbox_sync_status(runtime: ServerRuntime = Depends(get_runtime)):
    if runtime.get_inbox_sync_status is None:
        return {
            "running": False,
            "stage_id": "",
            "stage_message": "",
            "status": "idle",
            "message": "",
        }
    return runtime.get_inbox_sync_status()


@router.get("/company.html")
def company_page(company: str = "", runtime: ServerRuntime = Depends(get_runtime)):
    company_name = company.strip()
    if not company_name:
        return HTMLResponse(
            status_code=400,
            content=(
                '<!doctype html><html lang="en"><head><meta charset="utf-8" />'
                "<title>Company Positions</title></head><body><p>Missing company name.</p>"
                '<p><a href="/report.html">Back to full report</a></p></body></html>'
            ),
        )

    company_items = get_jobs_by_company(runtime.db_path, company_name, limit=0)
    since_iso = local_day_start_utc_iso()
    viewed_today_order = {
        int(row["id"]): idx
        for idx, row in enumerate(
            get_viewed_today_jobs(runtime.db_path, since_iso, limit=0)
        )
    }
    html_content = _render_company_dashboard_html(
        company_name, company_items, viewed_today_order=viewed_today_order
    )
    return HTMLResponse(content=html_content)
