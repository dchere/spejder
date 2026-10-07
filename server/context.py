"""Shared FastAPI runtime state for the GUI server."""

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable, Optional

from fastapi import Request

if TYPE_CHECKING:
    from spejder.workflows.skills_rematerialize import SkillsRematerializeCoordinator


@dataclass
class ServerRuntime:
    db_path: str
    profile_path: str
    runtime_profile: Any
    model_path: str
    report_dir: str
    persist_runtime_profile: Callable[[], None]
    reload_runtime_profile: Callable[[], None]
    queue_dashboard_rebuild: Callable[..., None]
    cli_verbose: bool
    get_report_rebuild_idle: Callable[[], bool]
    trigger_inbox_sync: Optional[Callable]
    get_inbox_sync_status: Optional[Callable]
    get_title_translation_llm: Any
    portrait_generate_lock: Lock
    cv_skills_sync_lock: Lock
    skills_rematerialize: Optional["SkillsRematerializeCoordinator"] = None


def get_runtime(request: Request) -> ServerRuntime:
    return request.app.state.runtime
