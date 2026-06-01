# pylint: disable=all
from spejder.db import *
from spejder.db import _provider_from_link, _normalize_position_link
import re
import json
import base64
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from collections.abc import Callable
from typing import Optional
from html import unescape
from bs4 import BeautifulSoup
from collections import Counter
from spejder.config import AppConfig

from .constants import *



def first_non_empty(lines: list[str]) -> str:
    for line in lines:
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


