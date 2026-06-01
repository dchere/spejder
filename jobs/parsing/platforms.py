# pylint: disable=all
from spejder.db import *
from spejder.db import _provider_from_link, _normalize_position_link, get_job_link, _extract_jobindex_id
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



def _extract_jobindex_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        if "jobindex.dk" not in (parsed.netloc or "").lower():
            continue

        job_id = _extract_jobindex_id(href)
        if not job_id:
            continue

        normalized = f"https://www.jobindex.dk/jobannonce/{job_id}"
        block = anchor.find_parent("table") or anchor.find_parent("tr")
        if not block:
            continue

        compact = " ".join(block.get_text(" ", strip=True).split())
        if len(compact) < 30:
            continue

        title = ""
        title_candidates = []
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            t2 = _extract_jobindex_id(href2)
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if (
                t2 == job_id
                and txt2
                and txt2.lower()
                not in {
                    "view job",
                    "apply",
                    "about the company",
                    "save job",
                    "settings",
                }
            ):
                title_candidates.append(txt2)
        if title_candidates:
            title = max(title_candidates, key=len)[:180]

        company = ""
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if txt2 and "jobindex.dk" not in href2.lower():
                company = txt2[:180]
                break
        if not company:
            fragments = [s.strip()
                         for s in block.stripped_strings if s and s.strip()]
            if fragments:
                company = fragments[0][:180]

        place = ""
        if title:
            m_place = re.search(
                re.escape(title) + r"\s+(.{2,80}?)\s+\d+\s+min\b",
                compact,
                flags=re.IGNORECASE,
            )
            if m_place:
                place = m_place.group(1).strip(" -|:")[:180]

        m_desc = re.search(
            r"settings\s*\)\s*(.*?)\s*PUBLISHED\s*:", compact, flags=re.IGNORECASE
        )
        extracted = ""
        if m_desc:
            extracted = m_desc.group(1).strip()
        else:
            m_desc2 = re.search(
                r"\d+\s+min\s*\(.*?\)\s*(.*?)\s*PUBLISHED\s*:",
                compact,
                flags=re.IGNORECASE,
            )
            if m_desc2:
                extracted = m_desc2.group(1).strip()

        raw_text = compact[:2500]
        if extracted:
            merged = f"{extracted}\n\n{raw_text}".strip()
            raw_text = merged[:2500]

        by_link[normalized] = {
            "title": title,
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": raw_text,
            "source": "Jobindex",
        }

    return by_link


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

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        by_link[normalized] = {
            "title": title[:180],
            "company": "Danfoss",
            "place": "",
            "work_type": "Unknown",
            "raw_text": title[:2500],
            "source": "Danfoss",
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


