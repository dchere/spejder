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
def dedupe_jobs(profile: str = None, db: str = None):
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime_profile = load_runtime_profile(profile_path)
    db_path = db or runtime_profile.default_db or "./jobs.db"

    ensure_db(db_path)
    result = merge_cross_source_duplicates(db_path)
    print(
        "Job dedupe complete: "
        f"groups_merged={result.get('groups_merged', 0)}, "
        f"rows_updated={result.get('rows_updated', 0)}, "
        f"rows_deleted={result.get('rows_deleted', 0)}, "
        f"db={db_path}"
    )



