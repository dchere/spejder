import re
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link, _provider_from_link
from spejder.db.utils import EMERSON_ORACLE_FA_HOST
from spejder.jobs.parsing.links import _is_job_link

_THEHUB_WORK_TYPES = frozenset(
    {"Full-time", "Part-time", "Internship", "Contract", "Freelance"}
)

_DJINNI_SKIP_ANCHOR_TEXT = frozenset({"Детальніше", "Дивитися нові вакансії"})


def _djinni_work_type_from_card(compact: str) -> str:
    low = compact.lower()
    if "part-time" in low or "part time" in low:
        return "Part-time"
    if "full-time" in low or "full time" in low:
        return "Full-time"
    if "Тільки віддалено" in compact:
        return "Remote"
    return "Unknown"


def _djinni_place_from_strings(strings: list[str]) -> str:
    for value in strings:
        if "Україн" in value or "Європ" in value or "віддалено" in value.lower():
            return value[:180]
    return ""


def _djinni_is_metadata_value(value: str) -> bool:
    if not value or value == "·":
        return True
    if re.fullmatch(r"\$+", value):
        return True
    if re.search(r"\d+\s+рок", value):
        return True
    if "досвіду" in value:
        return True
    if re.match(r"B[12]", value):
        return True
    if "середн" in value.lower() or "вище" in value.lower():
        return True
    if "віддалено" in value.lower() or "гібрид" in value.lower():
        return True
    if "україн" in value.lower() or "європ" in value.lower():
        return True
    return False


def _extract_djinni_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for card in soup.select("div.card"):
        h3 = card.find("h3")
        title_anchor = h3.find("a", href=True) if h3 else None
        if title_anchor is None:
            continue

        href = title_anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized or not _is_job_link(normalized):
            continue

        title = " ".join(title_anchor.get_text(" ", strip=True).split())
        if not title or title in _DJINNI_SKIP_ANCHOR_TEXT:
            continue

        strings = [s.strip() for s in card.stripped_strings if s.strip()]
        company = ""
        details_text = card.select_one("span.details-text")
        if details_text:
            candidate = " ".join(details_text.get_text(" ", strip=True).split())
            if candidate and not re.fullmatch(r"\d+", candidate):
                company = candidate
        if not company and strings:
            title_idx = strings.index(title) if title in strings else len(strings)
            for value in strings[:title_idx]:
                if (
                    value == title
                    or _djinni_is_metadata_value(value)
                    or re.fullmatch(r"\d+", value)
                ):
                    continue
                company = value
                break

        compact = " ".join(card.get_text(" ", strip=True).split())
        by_link[normalized] = {
            "title": title[:180],
            "company": company[:180],
            "place": _djinni_place_from_strings(strings),
            "work_type": _djinni_work_type_from_card(compact),
            "raw_text": compact[:2500],
            "source": "Djinni",
        }

    return by_link


def _extract_vestas_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        is_vestas_job = (
            "careers.vestas.com" in host
            and "/job/" in path
            and re.search(r"/job/.+/\d+", path)
        )
        if not is_vestas_job:
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
            "company": "Vestas",
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "source": "Vestas",
        }

    return by_link


def _extract_danfoss_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "jobs.danfoss.com" not in host or "/job/" not in path:
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
            "company": "Danfoss",
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "source": "Danfoss",
        }

    return by_link


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
