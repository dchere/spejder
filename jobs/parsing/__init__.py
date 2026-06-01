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


from .utils import first_non_empty
from .companies import extract_company_title
from .links import _is_job_link
from .linkedin import _work_type_from_html_for_link, _has_easy_apply_signal, _has_linkedin_public_easy_apply, _is_linkedin_reference_position_link, _is_linkedin_boilerplate_entry
from .html_parser import _parse_card_text_fields, _parse_anchor_fragments, _extract_html_entries_by_link
from .text_parser import _infer_work_type_from_text, _extract_entries_from_text
from .platforms import _extract_jobindex_entries_by_link, _extract_demant_entries_by_link, _extract_danfoss_entries_by_link, _extract_google_entries_by_link

from .core import extract_job_entries
