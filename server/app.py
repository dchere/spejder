"""FastAPI app factory and uvicorn entry for the GUI server."""

import errno
import socket
import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from spejder.server.context import ServerRuntime
from spejder.server.routers.jobs import router as jobs_router
from spejder.server.routers.jobs_applied_extras import router as jobs_applied_extras_router
from spejder.server.routers.companies import router as companies_router
from spejder.server.routers.ops import router as ops_router
from spejder.server.routers.portrait import router as portrait_router
from spejder.server.routers.profile import router as profile_router
from spejder.server.routers.skills import router as skills_router
from spejder.server.routers.skills_block_delete import router as skills_block_delete_router


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
    cli_verbose: bool,
    get_report_rebuild_idle=lambda: True,
    trigger_inbox_sync=None,
    get_inbox_sync_status=None,
    skills_rematerialize=None,
    request_shutdown=None,
) -> FastAPI:
    from spejder.workflows.skills_rematerialize import SkillsRematerializeCoordinator

    if skills_rematerialize is None:
        skills_rematerialize = SkillsRematerializeCoordinator(
            db_path=db_path,
            runtime_profile=runtime_profile,
            model_path=model_path,
            cli_verbose=cli_verbose,
            queue_dashboard_rebuild=queue_dashboard_rebuild,
        )

    app = FastAPI(title="Spejder GUI Server")
    app.state.runtime = ServerRuntime(
        db_path=db_path,
        profile_path=profile_path,
        runtime_profile=runtime_profile,
        model_path=model_path,
        report_dir=report_dir,
        persist_runtime_profile=persist_runtime_profile,
        reload_runtime_profile=reload_runtime_profile,
        queue_dashboard_rebuild=queue_dashboard_rebuild,
        cli_verbose=cli_verbose,
        get_report_rebuild_idle=get_report_rebuild_idle,
        trigger_inbox_sync=trigger_inbox_sync,
        get_inbox_sync_status=get_inbox_sync_status,
        get_title_translation_llm=get_title_translation_llm,
        portrait_generate_lock=threading.Lock(),
        cv_skills_sync_lock=threading.Lock(),
        skills_rematerialize=skills_rematerialize,
        request_shutdown=request_shutdown,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(jobs_router)
    app.include_router(jobs_applied_extras_router)
    app.include_router(skills_router)
    app.include_router(skills_block_delete_router)
    app.include_router(companies_router)
    app.include_router(ops_router)
    app.include_router(portrait_router)
    app.include_router(profile_router)
    app.mount("/", StaticFiles(directory=report_dir, html=True), name="static")
    return app


def _select_listen_port(host: str, port: int, max_port_attempts: int = 20) -> int:
    """Probe ``port`` … ``port + max_port_attempts``; retry only on EADDRINUSE."""
    for port_offset in range(max_port_attempts + 1):
        candidate_port = port + port_offset
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.bind((host, candidate_port))
            return candidate_port
        except OSError as exc:
            if getattr(exc, "errno", None) == errno.EADDRINUSE:
                if port_offset < max_port_attempts:
                    continue
                raise OSError(
                    f"Address in use for all tried ports: {port}-{port + max_port_attempts}"
                ) from exc
            raise


def start_server(host, port, app_factory_kwargs):
    import uvicorn

    app = create_app(**app_factory_kwargs)
    selected_port = _select_listen_port(host, port)

    if selected_port != port:
        print(f"Requested port {port} is busy; using port {selected_port} instead.")

    report_url = f"http://{host}:{selected_port}/report.html"
    print(f"Serving GUI at {report_url}")

    config = uvicorn.Config(
        app,
        host=host,
        port=selected_port,
        log_level="info" if app_factory_kwargs.get("cli_verbose") else "warning",
    )
    server = uvicorn.Server(config)

    def request_shutdown():
        def _exit():
            print("Shutting down Spejder GUI server.")
            server.should_exit = True

        timer = threading.Timer(0.25, _exit)
        timer.daemon = True
        timer.start()

    app.state.runtime.request_shutdown = request_shutdown
    server.run()
