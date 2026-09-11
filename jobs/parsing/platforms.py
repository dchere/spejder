import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link, _provider_from_link

from .platforms_jobindex import _extract_jobindex_entries_by_link

def _extract_demant_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "careers.demant.com" not in host or "/job/" not in path:
            continue

        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if not compact:
            continue

        title = compact
        place = ""
        match = re.match(r"^(?P<title>.+?)\s*-\s*(?P<place>.+)$", compact)
        if match:
            title = (match.group("title") or "").strip()
            place = (match.group("place") or "").strip()

        by_link[normalized] = {
            "title": title[:180],
            "company": "Demant Group",
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "source": "Demant",
        }

    return by_link


def _extract_google_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized or _provider_from_link(normalized) != "Google Careers":
            continue

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        block = anchor.find_parent(
            "td") or anchor.find_parent("tr") or anchor.parent
        compact = " ".join(block.get_text(
            " ", strip=True).split()) if block else title
        compact = compact[:2500]

        company = "Google"
        place = ""

        suffix = compact[len(title):].strip(
        ) if compact.startswith(title) else compact
        location_match = re.match(
            r"^(?P<company>[^–-]{2,80}?)\s*[–-]\s*(?P<place>.+?)(?:\s+\d+\s+(?:minute|minutes|hour|hours|day|days|week|weeks)\s+ago\b|$)",
            suffix,
            flags=re.IGNORECASE,
        )
        if location_match:
            company = (location_match.group("company")
                       or company).strip(" -|:")[:180]
            place = (location_match.group("place") or "").strip(" -|:")[:180]

        by_link[normalized] = {
            "title": title[:180],
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": compact,
            "source": "Google Careers",
        }

    return by_link


