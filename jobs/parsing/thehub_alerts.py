import re
from typing import Optional

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link

_THEHUB_WORK_TYPES = frozenset(
    {"Full-time", "Part-time", "Internship", "Contract", "Freelance"}
)


def _thehub_card_block(anchor) -> Optional[object]:
    block = anchor.find_parent("table")
    while block is not None and block.name == "table":
        style = (block.get("style") or "").replace(" ", "").lower()
        if "background-color:white" in style or "padding:16px" in style:
            return block
        block = block.find_parent("table")
    return anchor.find_parent("td") or anchor.parent


def _extract_thehub_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized or not re.search(
            r"thehub\.io/jobs/[0-9a-f]{12,}", normalized.lower()
        ):
            continue
        if normalized in by_link:
            continue

        block = _thehub_card_block(anchor)
        search_root = block if block is not None else anchor

        title = ""
        for link_node in search_root.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            if _normalize_position_link(href2) != normalized:
                continue
            span = link_node.find(
                "span",
                style=lambda s: s and "#1f2430" in (s or "").replace(" ", "").lower(),
            )
            txt = " ".join((span or link_node).get_text(" ", strip=True).split())
            if txt and len(txt) > len(title):
                title = txt

        company = ""
        place = ""
        meta_table = search_root.find(
            "table",
            style=lambda s: s and "font-size:13px" in (s or ""),
        )
        if meta_table:
            texts = [
                " ".join(span.get_text(" ", strip=True).split())
                for span in meta_table.find_all("span")
                if span.get_text(strip=True)
            ]
            texts = [t for t in texts if t not in _THEHUB_WORK_TYPES]
            if texts:
                company = texts[0]
            if len(texts) > 1:
                place = texts[1]

        compact = " ".join(search_root.get_text(" ", strip=True).split())

        by_link[normalized] = {
            "title": title[:180],
            "company": company[:180],
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "source": "The Hub",
        }

    return by_link
