# pylint: disable=all
import os
import sys
import time
import json
import codecs
import html as html_lib
import re
import traceback
import subprocess
from typing import Optional
from contextlib import contextmanager
from jinja2 import Environment, FileSystemLoader
import spejder
from spejder.llm import LocalLLM
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.db import *
from spejder.jobs import *
from spejder.managers.dashboard_manager import _render_html_from_items
from spejder.config import AppConfig
# from spejder.server import run_server

LLM_SELF_TEST_PROMPT = "Reply with one word: OK"
def _llm_model_path(
    runtime_profile: Optional[AppConfig], override_model_path: str = ""
) -> str:
    candidate = str(override_model_path or "").strip()
    if not candidate:
        profile = runtime_profile or {}
        candidate = str(profile.default_model or "").strip()
    return os.path.abspath(os.path.expanduser(candidate)) if candidate else ""



def _llm_model_looks_valid(model_path: str) -> bool:
    if not model_path or not os.path.isfile(model_path):
        return False
    if not model_path.lower().endswith(".gguf"):
        return False
    try:
        return os.path.getsize(model_path) >= 200_000_000
    except OSError:
        return False



def _print_llm_step(message: str) -> None:
    print(f"Model init: {message}")



def _fail_llm_init(message: str) -> None:
    _print_llm_step(message)
    raise SystemExit(1)



def _initialize_llm_or_exit(
    profile_path: str, override_model_path: str = "", verbose: bool = False
) -> LocalLLM:
    _print_llm_step(f"loading profile from {profile_path}")
    if not profile_path or not os.path.isfile(profile_path):
        _fail_llm_init("profile file is missing")

    runtime_profile = load_runtime_profile(profile_path)
    model_path = _llm_model_path(runtime_profile, override_model_path=override_model_path)
    if not model_path:
        _fail_llm_init("default_model is not configured")
    _print_llm_step(f"model path configured: {model_path}")

    if not os.path.exists(model_path):
        _fail_llm_init("configured model path does not exist")
    if not os.path.isfile(model_path):
        _fail_llm_init("configured model path is not a file")
    _print_llm_step("model file found")

    if not _llm_model_looks_valid(model_path):
        _fail_llm_init("model file failed basic validation")
    _print_llm_step(f"model file looks valid (size={os.path.getsize(model_path)} bytes)")

    try:
        llm = LocalLLM(
            model_path=model_path,
            n_ctx=int(runtime_profile.n_ctx),
            verbose=verbose,
        )
        llm.load()
    except Exception as exc:
        _fail_llm_init(f"model initialization failed: {exc}")
    _print_llm_step("model initialized")

    try:
        output = llm.generate(LLM_SELF_TEST_PROMPT, max_tokens=8)
    except Exception as exc:
        _fail_llm_init(f"self-test generation failed: {exc}")

    cleaned = " ".join((output or "").split()).strip()
    if not cleaned:
        _fail_llm_init("self-test returned empty output")
    _print_llm_step(f"self-test output: {cleaned}")
    _print_llm_step("self-test passed")
    return llm


initialize_llm_or_exit = _initialize_llm_or_exit



