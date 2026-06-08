import os
import threading
import time
import webbrowser
from typing import Optional

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile, save_profile
from spejder.db import ensure_db
from spejder.extractors.skill_extractor import _ensure_skill_pattern_seed_migration
from spejder.llm import LocalLLM
from spejder.workflows.dashboard import (
    DashboardRebuildQueue,
    populate_missing_dashboard_skills,
)
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync


def serve_gui(
    profile: str = None,
    report_dir: str = None,
    db: str = None,
    host: str = None,
    port: int = None,
    no_open: bool = False,
    verbose: bool = False,
):
    cli_verbose = bool(verbose)
    profile_path = os.path.abspath(profile or DEFAULT_PROFILE_PATH)
    runtime_profile = load_runtime_profile(profile_path)
    report_dir = os.path.abspath(
        report_dir or runtime_profile.default_report_dir or "./outbox"
    )
    db_path = os.path.abspath(db or runtime_profile.default_db or "./jobs.db")
    inbox_path = os.path.abspath(runtime_profile.default_inbox or "./inbox")
    model_path = runtime_profile.default_model or ""
    host = host or runtime_profile.server_host or "127.0.0.1"
    port = port if port is not None else int(runtime_profile.server_port or 8765)

    print(f"Serve GUI: initializing (profile={profile_path})")
    print(f"Serve GUI: db={db_path}, report_dir={report_dir}, inbox={inbox_path}")
    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)
    print("Serve GUI: database ready")
    os.makedirs(report_dir, exist_ok=True)
    dashboard_path = os.path.join(report_dir, "report.html")
    rebuild_queue = DashboardRebuildQueue(db_path, dashboard_path, runtime_profile)
    title_translation_llm: Optional[LocalLLM] = None

    def _get_title_translation_llm() -> Optional[LocalLLM]:
        nonlocal title_translation_llm
        if title_translation_llm is not None:
            return title_translation_llm
        if not model_path:
            return None
        title_translation_llm = LocalLLM(model_path=model_path, n_ctx=int(runtime_profile.n_ctx), verbose=False)
        return title_translation_llm

    def _persist_runtime_profile() -> None:
        save_profile(runtime_profile, profile_path)

    def _reload_runtime_profile() -> None:
        fresh = load_runtime_profile(profile_path)
        for key, value in fresh.model_dump().items():
            setattr(runtime_profile, key, value)

    def _populate_missing_dashboard_skills(
        rows: list[dict],
        *,
        llm: Optional[LocalLLM] = None,
        progress_label: str = "",
    ) -> int:
        return populate_missing_dashboard_skills(
            db_path,
            runtime_profile,
            rows,
            llm=llm,
            progress_label=progress_label,
        )

    sync_context = GuiSyncContext(
        db_path=db_path,
        inbox_path=inbox_path,
        model_path=model_path,
        profile_path=profile_path,
        runtime_profile=runtime_profile,
        cli_verbose=cli_verbose,
        queue_dashboard_rebuild=rebuild_queue.queue,
        reload_runtime_profile=_reload_runtime_profile,
        populate_missing_dashboard_skills=_populate_missing_dashboard_skills,
    )
    app_factory_kwargs = {
        "db_path": db_path,
        "profile_path": profile_path,
        "runtime_profile": runtime_profile,
        "model_path": model_path,
        "report_dir": report_dir,
        "get_title_translation_llm": _get_title_translation_llm,
        "persist_runtime_profile": _persist_runtime_profile,
        "reload_runtime_profile": _reload_runtime_profile,
        "queue_dashboard_rebuild": rebuild_queue.queue,
        "cli_verbose": cli_verbose,
    }

    if not no_open:
        def open_browser():
            time.sleep(1)
            report_url = f"http://{host}:{port}/report.html"
            opened = webbrowser.open(report_url, new=2)
            if opened:
                print(f"Opened in default browser: {report_url}")
        threading.Thread(target=open_browser, daemon=True).start()

    print("Serve GUI: starting startup tasks in background")
    rebuild_queue.start_worker()
    threading.Thread(
        target=lambda: rebuild_queue.queue(reason="startup snapshot"),
        name="spejder-startup-dashboard",
        daemon=True,
    ).start()
    threading.Thread(
        target=run_inbox_sync,
        args=(sync_context,),
        name="spejder-inbox-sync",
        daemon=True,
    ).start()

    try:
        from spejder.server import start_server
        start_server(host, port, app_factory_kwargs)
    except KeyboardInterrupt:
        print("Stopping server...")

