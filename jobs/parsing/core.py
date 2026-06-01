from .companies import extract_company_title
from .links import _is_job_link
from .linkedin import _is_linkedin_reference_position_link, _is_linkedin_boilerplate_entry, _work_type_from_html_for_link, _has_linkedin_public_easy_apply, _has_easy_apply_signal
from .html_parser import _extract_html_entries_by_link
from .text_parser import _extract_entries_from_text
from .platforms import _extract_jobindex_entries_by_link, _extract_demant_entries_by_link, _extract_danfoss_entries_by_link, _extract_google_entries_by_link
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



def extract_job_entries(doc: dict) -> list[dict]:
    text = doc.get("text", "") or ""
    html_text = doc.get("html", "") or ""
    title_hint = doc.get("title", "") or ""
    links = doc.get("links", []) or []
    html_by_link = _extract_html_entries_by_link(html_text)
    jobindex_by_link = _extract_jobindex_entries_by_link(html_text)
    demant_by_link = _extract_demant_entries_by_link(html_text)
    danfoss_by_link = _extract_danfoss_entries_by_link(html_text)
    google_by_link = _extract_google_entries_by_link(html_text)

    by_text = _extract_entries_from_text(text)
    by_link = {}
    for entry in by_text:
        by_link[entry["position_link"]] = entry

    for lnk, entry in by_link.items():
        html_fields = html_by_link.get(lnk, {})
        ji_fields = jobindex_by_link.get(lnk, {})
        demant_fields = demant_by_link.get(lnk, {})
        danfoss_fields = danfoss_by_link.get(lnk, {})
        google_fields = google_by_link.get(lnk, {})

        if google_fields.get("title"):
            entry["title"] = google_fields["title"]
        if google_fields.get("company"):
            entry["company"] = google_fields["company"]
        if google_fields.get("place"):
            entry["place"] = google_fields["place"]
        if google_fields.get("work_type"):
            entry["work_type"] = google_fields["work_type"]
        if google_fields.get("raw_text"):
            entry["raw_text"] = google_fields["raw_text"]
        if google_fields.get("source"):
            entry["source"] = google_fields["source"]

        if danfoss_fields.get("title"):
            entry["title"] = danfoss_fields["title"]
        if danfoss_fields.get("company"):
            entry["company"] = danfoss_fields["company"]
        if danfoss_fields.get("place"):
            entry["place"] = danfoss_fields["place"]
        if danfoss_fields.get("work_type"):
            entry["work_type"] = danfoss_fields["work_type"]
        if danfoss_fields.get("raw_text"):
            entry["raw_text"] = danfoss_fields["raw_text"]
        if danfoss_fields.get("source"):
            entry["source"] = danfoss_fields["source"]

        if demant_fields.get("title"):
            entry["title"] = demant_fields["title"]
        if demant_fields.get("company"):
            entry["company"] = demant_fields["company"]
        if demant_fields.get("place"):
            entry["place"] = demant_fields["place"]
        if demant_fields.get("work_type"):
            entry["work_type"] = demant_fields["work_type"]
        if demant_fields.get("raw_text"):
            entry["raw_text"] = demant_fields["raw_text"]
        if demant_fields.get("source"):
            entry["source"] = demant_fields["source"]

        if ji_fields.get("title"):
            entry["title"] = ji_fields["title"]
        if ji_fields.get("company"):
            entry["company"] = ji_fields["company"]
        if ji_fields.get("place"):
            entry["place"] = ji_fields["place"]
        if ji_fields.get("raw_text"):
            entry["raw_text"] = ji_fields["raw_text"]

        if html_fields.get("title"):
            entry["title"] = html_fields["title"]
        if html_fields.get("company"):
            entry["company"] = html_fields["company"]
        if html_fields.get("place"):
            entry["place"] = html_fields["place"]

        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, lnk
        )
        if wt:
            entry["work_type"] = wt

        if html_fields.get("raw_text"):
            entry["raw_text"] = html_fields["raw_text"]

        entry["source"] = _provider_from_link(lnk)

    for raw_link in links:
        if not raw_link:
            continue
        normalized = _normalize_position_link(raw_link)
        if not _is_job_link(normalized):
            continue
        if _is_linkedin_reference_position_link(raw_link, normalized):
            continue
        if normalized in by_link:
            continue
        html_fields = html_by_link.get(normalized, {})
        ji_fields = jobindex_by_link.get(normalized, {})
        demant_fields = demant_by_link.get(normalized, {})
        danfoss_fields = danfoss_by_link.get(normalized, {})
        google_fields = google_by_link.get(normalized, {})
        company, title = extract_company_title(text, title_hint)
        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, normalized
        )
        by_link[normalized] = {
            "company": google_fields.get("company")
            or danfoss_fields.get("company")
            or demant_fields.get("company")
            or ji_fields.get("company")
            or html_fields.get("company")
            or company,
            "title": google_fields.get("title")
            or danfoss_fields.get("title")
            or demant_fields.get("title")
            or ji_fields.get("title")
            or html_fields.get("title")
            or title,
            "place": google_fields.get("place")
            or danfoss_fields.get("place")
            or demant_fields.get("place")
            or ji_fields.get("place")
            or html_fields.get("place")
            or "",
            "work_type": google_fields.get("work_type")
            or danfoss_fields.get("work_type")
            or demant_fields.get("work_type")
            or (wt if wt else "Unknown"),
            "position_link": normalized,
            "raw_text": google_fields.get("raw_text")
            or danfoss_fields.get("raw_text")
            or demant_fields.get("raw_text")
            or ji_fields.get("raw_text")
            or html_fields.get("raw_text")
            or text[:2500],
            "source": google_fields.get("source")
            or danfoss_fields.get("source")
            or demant_fields.get("source")
            or _provider_from_link(normalized),
        }

    filtered_entries: list[dict] = []
    for entry in by_link.values():
        if "source" not in entry:
            entry["source"] = _provider_from_link(
                entry.get("position_link", ""))
        if entry.get("source") == "Getinge":
            entry["company"] = "Getinge"
        if _is_linkedin_boilerplate_entry(entry):
            continue
        filtered_entries.append(entry)

    return filtered_entries


