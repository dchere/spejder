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
def init_profile(path: str = None, force: bool = False):
    if os.path.exists(path) and not force:
        print("Profile already exists. Use --force to overwrite:", path)
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    config = AppConfig.load(DEFAULT_PROFILE_PATH)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config.model_dump(), f, ensure_ascii=False, indent=2)
    print("Created profile:", path)



