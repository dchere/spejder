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
def _report_limit_value(raw, default: int) -> int:
    try:
        value = int(raw)
    except Exception:
        return int(default)
    return value if value > 0 else int(default)



def _report_max_relevant_positions(runtime_profile: Optional[AppConfig]) -> int:
    profile = runtime_profile or {}
    legacy = _report_limit_value(profile.report_max_relevant_positions, 7)
    return _report_limit_value(profile.report_max_relevant_positions, legacy)



def _report_max_not_relevant_positions(runtime_profile: Optional[AppConfig]) -> int:
    profile = runtime_profile or {}
    legacy = _report_limit_value(profile.report_max_relevant_positions, 7)
    return _report_limit_value(profile.report_max_not_relevant_positions, legacy)



def report_links(folder: str):
    docs = email_parser.load_files(folder)
    link_counts = {}
    for d in docs:
        for lnk in d.get("links", []):
            link_counts[lnk] = link_counts.get(lnk, 0) + 1
    items = sorted(link_counts.items(), key=lambda x: x[1], reverse=True)
    for url, cnt in items[:200]:
        print(f"{cnt}\t{url}")



def render_html(input_val: str = "./outbox/relevant_positions.jsonl", out: str = "./outbox/relevant_positions.html", title: str = "Relevant Positions"):
    if not os.path.exists(input_val):
        print("Input JSONL not found:", input_val)
        return

    items = []
    with open(input_val, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue

    _render_html_from_items(items, out, title)



