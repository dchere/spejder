import re
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from spejder.config import AppConfig
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)

# pylint: skip-file


def _extract_place_from_page_text(position_link: str, page_text: str) -> str:
    link = (position_link or "").strip().lower()
    text = " ".join((page_text or "").split())
    if not text:
        return ""

    if "jobs.danfoss.com" in link:
        match = re.search(
            r"Job Location \(Short\):\s*(.+?)\s+Employment Type:",
            text,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()[:180]

    return ""


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

