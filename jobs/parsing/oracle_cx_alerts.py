import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link, _provider_from_link
from spejder.db.utils import EMERSON_ORACLE_FA_HOST


def _extract_oracle_cx_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        parsed = urlparse(href)
        host = re.sub(r":(443|80)$", "", (parsed.netloc or "")).lower()
        path = (parsed.path or "").lower()
        provider = _provider_from_link(normalized)
        is_emerson = (
            host == EMERSON_ORACLE_FA_HOST and "/candidateexperience/" in path
        )
        if not (is_emerson or provider == "Oracle CX" or provider == "Emerson Career Site"):
            continue

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        if is_emerson:
            company = "Emerson"
            source = "Emerson Career Site"
        else:
            company = ""
            source = "Oracle CX"

        by_link[normalized] = {
            "title": title[:180],
            "company": company,
            "place": "",
            "work_type": "Unknown",
            "raw_text": title[:2500],
            "source": source,
        }

    return by_link
