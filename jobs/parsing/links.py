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



def _is_job_link(link: str) -> bool:
    low = link.lower()
    if "linkedin.com/comm/jobs/view/" in low or "linkedin.com/jobs/view/" in low:
        return True
    if "thehub.io/jobs/" in low and re.search(r"thehub\.io/jobs/[0-9a-f]{12,}", low):
        return True
    if re.search(
        r"(?:careers\.google\.com|google\.com)/.+/jobs/results/\d+",
        low,
    ):
        return True
    if "jobindex.dk" in low and (
        "jobid=" in low
        or re.search(r"/jobannonce/[hr]\d+", low)
        or re.search(r"/bruger/dine-job/[hr]\d+", low)
    ):
        return True
    if "careers.demant.com" in low and "/job/" in low:
        return True
    if "jobs.danfoss.com" in low and "/job/" in low:
        return True
    if "jobs.teradyne.com" in low and "/job/" in low:
        return True
    if "careers.nordea.com" in low and "/job/" in low:
        return True
    if "careers.novonordisk.com" in low and "/job/" in low:
        return True
    if (
        re.search(r"\.fa\.ocs\.oraclecloud\.(?:com|eu)", low)
        and "/candidateexperience/" in low
        and re.search(r"/job/\d+", low)
    ):
        return True
    if "careers.nttdata-solutions.com" in low and "/job/" in low:
        return True
    if "careers.getinge.com" in low and "/job/" in low:
        return True
    if "jobs.tetrapak.com" in low and re.search(r"/job/[^/]+/\d+", low):
        return True
    return False


