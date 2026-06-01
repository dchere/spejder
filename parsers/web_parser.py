
import os
import re
import time
from typing import Optional
from urllib.request import Request
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from bs4 import BeautifulSoup

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.parsers import email_parser
from spejder.llm import LocalLLM
from spejder.db import (
    ensure_db, set_job_summary, get_jobs_by_category, set_job_description,
    get_applied_jobs, get_relevant_jobs, get_viewed_jobs_count,
    get_jobs_for_description_refresh
)
from spejder.jobs import ingest_docs_to_db, apply_relevance, update_profile_from_db_signals
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.extractors.skill_extractor import _build_skills_tab_items
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
    translate_title_to_english as _translate_title_to_english,
    finalize_title_english as _finalize_title_english,
    normalize_title_compare_key as _normalize_title_compare_key,
    get_title_english_for_row as _get_title_english_for_row
)
MAX_INGEST_FILE_STATS_LINES = 10
# pylint: skip-file

def _extract_position_page_text(
    position_link: str,
    runtime_profile: Optional[AppConfig] = None,
    translation_cache: Optional[dict[str, str]] = None,
    max_chars: int = 3000,
    timeout_sec: int = 8,
) -> str:
    link = (position_link or "").strip()
    if not link.startswith(("http://", "https://")):
        return ""

    req = Request(
        link,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        })

    try:
        with urlopen(req, timeout=timeout_sec) as response:
            ctype = (response.headers.get("Content-Type") or "").lower()
            if ctype and "html" not in ctype and "text" not in ctype:
                return ""

            payload = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            html_text = payload.decode(charset, errors="ignore")
    except (HTTPError, URLError, TimeoutError, ValueError):
        return ""
    except Exception:
        return ""

    html_low = html_text.lower()
    linkedin_apply_marker = ""
    if "linkedin.com/" in link.lower() and "public_jobs_apply-link-onsite" in html_low:
        linkedin_apply_marker = "Easy Apply"

    soup = BeautifulSoup(html_text, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()

    text = " ".join(soup.get_text(" ", strip=True).split())
    if linkedin_apply_marker and not re.search(r'(?i)easy\s*apply|apply\s*with\s*linkedin', text):
        text = f"{linkedin_apply_marker} {text}".strip()
    if not text:
        return ""
    text = text[:max_chars]
    return _translate_text_to_english_if_needed(
        text,
        runtime_profile=runtime_profile,
        translation_cache=translation_cache,)


def _get_position_page_context(
    position_link: str,
    runtime_profile: Optional[AppConfig] = None,
    page_context_cache: Optional[dict] = None,
    translation_cache: Optional[dict[str, str]] = None,
) -> str:
    link = (position_link or "").strip()
    if not link:
        return ""
    if page_context_cache is not None and link in page_context_cache:
        return page_context_cache.get(link, "") or ""

    page_context = _extract_position_page_text(
        link,
        runtime_profile=runtime_profile,
        translation_cache=translation_cache,)
    if page_context_cache is not None:
        page_context_cache[link] = page_context
    return page_context


def _append_page_context_to_raw_text(
    raw_text: str, position_link: str, page_context: str, max_chars: int = 9000
) -> str:
    base_raw = (raw_text or "").strip()
    link = (position_link or "").strip()
    context = (page_context or "").strip()
    if not context:
        return base_raw
    if not base_raw:
        return context[:max_chars]
    if not link:
        return base_raw

    marker = f"[POSITION_PAGE_CONTEXT {link}]"
    if marker in base_raw:
        return base_raw

    merged = f"{base_raw}\n\n{marker}\n{context}".strip()
    return merged[:max_chars]

