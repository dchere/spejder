import re

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link
from spejder.jobs.parsing.links import _is_job_link

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
