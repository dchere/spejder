# pylint: disable=too-many-locals,missing-class-docstring,missing-function-docstring,unused-import,no-name-in-module,trailing-whitespace,unused-variable,broad-exception-caught,line-too-long,wrong-import-position,undefined-variable,pointless-string-statement
"""Core methods."""
# pylint: disable=too-many-locals,missing-class-docstring,missing-function-docstring,unused-import,no-name-in-module,trailing-whitespace,unused-variable,broad-exception-caught,line-too-long,wrong-import-position,undefined-variable,pointless-string-statement
"""
core.py: Core utilities and profile management for spejder
"""

import os
import json
from .config import AppConfig, load_profile as config_load_profile, save_profile as config_save_profile, DEFAULT_PROFILE_FILE

DEFAULT_PROFILE_PATH = os.path.join(os.path.dirname(__file__), "default_profile.json")
USER_PROFILE_PATH = "./profile.json"
WORKSPACE_ROOT_ENV = "SPEJDER_WORKSPACE"
MANUAL_APPLIED_RAW_MARKER = "[MANUAL_APPLIED_DESCRIPTION]"


def workspace_root() -> str:
    override = os.environ.get(WORKSPACE_ROOT_ENV, "").strip()
    if override:
        return os.path.abspath(os.path.expanduser(override))
    return os.path.abspath(os.getcwd())


def resolve_user_path(path: str) -> str:
    if not path:
        return path
    expanded = os.path.expanduser(path)
    if os.path.isabs(expanded):
        return expanded
    return os.path.normpath(os.path.join(workspace_root(), expanded))


def load_profile(profile_path=None) -> AppConfig:
    if profile_path == DEFAULT_PROFILE_PATH:
        profile_path = None
    return config_load_profile(profile_path)

def save_profile(profile: AppConfig, profile_path=None):
    if profile_path == DEFAULT_PROFILE_PATH:
        profile_path = None
    config_save_profile(profile, profile_path)


def print_step(message: str) -> None:
    print(f"[spejder] {message}")


def fail_init(message: str) -> None:
    print_step(message)
    raise SystemExit(1)


def load_runtime_profile(profile_path: str):
    path = profile_path or DEFAULT_PROFILE_PATH
    if path and os.path.exists(path):
        try:
            return load_profile(path)
        except Exception:
            pass
    return load_profile(None)
