from .links import _is_job_link
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



def _infer_work_type_from_text(text: str) -> str:
    low = (text or "").lower()
    if "hybrid" in low:
        return "Hybrid"
    if "remote" in low:
        return "Remote"
    if "on-site" in low or "onsite" in low:
        return "On-site"
    return ""


def _extract_entries_from_text(text: str) -> list[dict]:
    lines = [ln.strip() for ln in text.splitlines()]
    entries = []

    def score_title(line: str) -> int:
        low = line.lower()
        keys = [
            "developer",
            "engineer",
            "specialist",
            "manager",
            "scientist",
            "lead",
            "architect",
            "analyst",
            "consultant",
            ".net",
            "software",
        ]
        return sum(1 for k in keys if k in low)

    def score_company(line: str) -> int:
        low = line.lower()
        keys = ["group", "inc", "aps", "a/s",
                "ltd", "gmbh", "company", "danmark"]
        return sum(1 for k in keys if k in low)

    def score_place(line: str) -> int:
        low = line.lower()
        keys = [
            "aarhus",
            "copenhagen",
            "odense",
            "lystrup",
            "humleb",
            "denmark",
            "municipality",
        ]
        score = sum(1 for k in keys if k in low)
        if "," in line:
            score += 1
        return score

    for idx, line in enumerate(lines):
        if "view job:" not in line.lower():
            continue
        link_match = re.search(r"https?://\S+", line)
        if not link_match:
            continue

        link = _normalize_position_link(link_match.group(0))
        if not _is_job_link(link):
            continue

        candidates = []
        stop_phrases = [
            "this company is actively hiring",
            "apply with resume",
            "view job",
            "new jobs match your preferences",
            "job alert",
        ]
        start = max(0, idx - 10)
        for j in range(start, idx):
            candidate = lines[j].strip()
            clow = candidate.lower()
            if (
                candidate
                and "http" not in clow
                and not any(sp in clow for sp in stop_phrases)
                and "----" not in candidate
            ):
                candidates.append(candidate)

        title = ""
        company = ""
        place = ""
        if candidates:
            by_title = sorted(
                candidates, key=lambda s: (score_title(s), len(s)), reverse=True
            )
            by_company = sorted(
                candidates, key=lambda s: (score_company(s), len(s)), reverse=True
            )
            by_place = sorted(
                candidates, key=lambda s: (score_place(s), -len(s)), reverse=True
            )

            title = (
                by_title[0]
                if score_title(by_title[0]) > 0
                else (candidates[-3] if len(candidates) >= 3 else candidates[0])
            )
            company = (
                by_company[0]
                if score_company(by_company[0]) > 0
                else (candidates[-2] if len(candidates) >= 2 else "")
            )
            place = (
                by_place[0]
                if score_place(by_place[0]) > 0
                else (candidates[-1] if len(candidates) >= 3 else "")
            )

            # prevent duplicates between fields
            used = {title}
            if company in used and len(candidates) > 1:
                for c in candidates:
                    if c not in used:
                        company = c
                        break
            used.add(company)
            if place in used:
                for c in candidates:
                    if c not in used:
                        place = c
                        break

        local_chunk = " ".join(
            lines[max(0, idx - 20): min(len(lines), idx + 5)]
        ).lower()
        if "remote" in local_chunk:
            work_type = "Remote"
        elif "hybrid" in local_chunk:
            work_type = "Hybrid"
        elif "on-site" in local_chunk or "onsite" in local_chunk or place:
            work_type = "On-site"
        else:
            work_type = "Unknown"

        snippet_start = max(0, idx - 4)
        snippet_end = min(len(lines), idx + 2)
        raw_text = "\n".join(
            [s for s in lines[snippet_start:snippet_end] if s])

        entries.append(
            {
                "company": company[:180],
                "title": title[:180],
                "place": place[:180],
                "work_type": work_type,
                "position_link": link,
                "raw_text": raw_text,
            }
        )

    return entries


