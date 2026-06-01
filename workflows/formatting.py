# pylint: disable=all
import os
import sys
import time
import json
import codecs
import html as html_lib
import re
import traceback
from spejder.language import _normalize_title_text
import subprocess
from typing import Optional
from contextlib import contextmanager
from jinja2 import Environment, FileSystemLoader
import spejder
from spejder.llm import LocalLLM
from spejder.language import _normalize_title_text
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.managers.language_manager import normalize_title_compare_key as _normalize_title_compare_key
from spejder.db import *
from spejder.jobs import *
from spejder.managers.dashboard_manager import _render_html_from_items
from spejder.config import AppConfig
# from spejder.server import run_server
def _render_title_english_line(item: dict) -> str:
    title = _normalize_title_text(str(item.get("title", "") or ""))
    title_english = _normalize_title_text(str(item.get("title_english", "") or ""))
    if not title or not title_english:
        return ""
    if _normalize_title_compare_key(title) == _normalize_title_compare_key(title_english):
        return ""
    return f'<p><strong>In English:</strong> {html_lib.escape(title_english)}</p>'



def _prepend_title_to_raw_text(
    title: str, raw_text: str, max_chars: int = 9000
) -> str:
    title_clean = " ".join((title or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not title_clean:
        return raw_clean

    prefixed = f"Title: {title_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]



def _prepend_summary_to_raw_text(
    summary: str, raw_text: str, max_chars: int = 9000
) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not summary_clean or _is_invalid_summary_text(summary_clean):
        return raw_clean

    prefixed = f"Summary: {summary_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]



