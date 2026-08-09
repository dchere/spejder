"""Fixed interpreter for career-alert artifacts (no LLM, no exec)."""

from __future__ import annotations

import re
from functools import lru_cache
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

from spejder.db import _normalize_position_link
from spejder.jobs.parsing.artifact_schema import (
    CareerAlertArtifact,
    compile_safe_path_regex,
)
from spejder.jobs.parsing.jobs2web import _parse_jobs2web_anchor_text

_CTA_LABELS = frozenset(
    {
        "apply here",
        "apply now",
        "apply",
        "view job",
        "view role",
        "see job",
        "read more",
        "learn more",
    }
)


@lru_cache(maxsize=64)
def _compiled_path_regex(pattern: str) -> re.Pattern[str] | None:
    return compile_safe_path_regex(pattern)


def href_matches_artifact(href: str, artifact: CareerAlertArtifact) -> bool:
    if not href:
        return False
    parsed = urlparse(href)
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    match = artifact.match
    # Fail closed: blank substrings ("") are true for every host/path via `in`.
    hosts = [sub.lower() for sub in (match.host_substrings or []) if str(sub).strip()]
    paths = [part.lower() for part in (match.path_includes or []) if str(part).strip()]
    if not hosts or not paths:
        return False
    if not any(sub in host for sub in hosts):
        return False
    if not any(part in path for part in paths):
        return False
    if match.require_path_regex:
        compiled = _compiled_path_regex(match.require_path_regex)
        if compiled is None or not compiled.search(path):
            return False
    return True


def anchor_text_matches_artifact(compact: str, artifact: CareerAlertArtifact) -> bool:
    allowed = [
        label.casefold()
        for label in (artifact.match.anchor_text_equals or [])
        if str(label).strip()
    ]
    if not allowed:
        return True
    return (compact or "").strip().casefold() in allowed


def artifact_prefilter_matches(html_text: str, links: list[str], artifact: CareerAlertArtifact) -> bool:
    for link in links or []:
        if href_matches_artifact(str(link or ""), artifact):
            return True
    if not html_text:
        return False
    soup = BeautifulSoup(html_text, "html.parser")
    for anchor in soup.find_all("a", href=True):
        if not href_matches_artifact(anchor.get("href") or "", artifact):
            continue
        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if anchor_text_matches_artifact(compact, artifact):
            return True
    return False


def _find_title_container(anchor: Tag) -> Tag | None:
    node: Tag | None = anchor
    best: Tag | None = None
    for _ in range(14):
        if node is None:
            break
        parent = node.parent
        if not isinstance(parent, Tag):
            break
        node = parent
        text = " ".join(node.get_text(" ", strip=True).split())
        if len(text) < 40:
            continue
        best = node
        if node.name in ("tr", "li", "article", "section"):
            return node
    return best


def _title_from_ancestor_block(anchor: Tag) -> tuple[str, str]:
    """Return (title, raw_text) from the nearest substantial ancestor of a CTA button."""
    container = _find_title_container(anchor)
    if container is None:
        return "", ""
    raw = " ".join(container.get_text(" ", strip=True).split())
    headings: list[str] = []
    for tag in container.find_all(["strong", "b", "h1", "h2", "h3", "h4"]):
        text = " ".join(tag.get_text(" ", strip=True).split())
        if not text:
            continue
        if text.casefold() in _CTA_LABELS:
            continue
        headings.append(text)
    if headings:
        # iCIMS often splits one title across adjacent <strong> tags.
        title = " ".join(headings)
        return title, raw

    peeled = raw
    for label in _CTA_LABELS:
        peeled = re.sub(rf"(?i)\b{re.escape(label)}\b", " ", peeled)
    peeled = " ".join(peeled.split())
    if not peeled:
        return "", raw
    # First clause / short head as title when no heading markup exists.
    for separator in (". ", " - ", " – ", " — "):
        if separator in peeled and len(peeled.split(separator, 1)[0]) >= 12:
            return peeled.split(separator, 1)[0].strip(), raw
    return peeled[:180].strip(), raw


def _fields_from_anchor(
    compact: str,
    artifact: CareerAlertArtifact,
    *,
    anchor: Tag | None = None,
) -> dict[str, str] | None:
    recipes = artifact.fields
    if recipes.from_anchor == "ancestor_strong_or_first_line":
        if anchor is None:
            return None
        title, raw = _title_from_ancestor_block(anchor)
        if not title:
            return None
        place = ""
        work_type = "Unknown"
        compact_out = raw or title
    elif recipes.from_anchor == "jobs2web_middot_or_dash":
        if not compact:
            return None
        parsed = _parse_jobs2web_anchor_text(compact)
        title = parsed.get("title") or compact
        place = parsed.get("place") or ""
        work_type = parsed.get("work_type") or "Unknown"
        compact_out = compact
    else:
        if not compact:
            return None
        title = compact
        place = ""
        work_type = "Unknown"
        compact_out = compact

    company = recipes.company or ""
    source = recipes.source or ""
    return {
        "title": title[: recipes.title_max],
        "company": company,
        "place": place[: recipes.place_max],
        "work_type": work_type,
        "raw_text": compact_out[: recipes.raw_text_max],
        "source": source,
    }


def interpret_artifact(html_text: str, artifact: CareerAlertArtifact) -> dict[str, dict[str, str]]:
    """Return normalized_link → field map for one artifact."""
    if not html_text or not artifact.enabled:
        return {}
    if artifact.extract.mode != "filtered_links":
        # css mode reserved for later migrations
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = anchor.get("href") or ""
        if not href_matches_artifact(href, artifact):
            continue
        normalized = _normalize_position_link(href)
        if not normalized:
            continue
        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if not anchor_text_matches_artifact(compact, artifact):
            continue
        fields = _fields_from_anchor(compact, artifact, anchor=anchor)
        if not fields:
            continue
        by_link[normalized] = fields
    return by_link


def interpret_artifacts(
    html_text: str,
    artifacts: list[CareerAlertArtifact],
    *,
    links: list[str] | None = None,
) -> dict[str, dict[str, str]]:
    """Run enabled artifacts by priority (higher first); first writer wins per link."""
    ordered = sorted(artifacts, key=lambda a: (-int(a.priority), a.id))
    merged: dict[str, dict[str, str]] = {}
    link_list = list(links or [])
    for artifact in ordered:
        if not artifact.enabled:
            continue
        if not artifact_prefilter_matches(html_text, link_list, artifact):
            continue
        extracted = interpret_artifact(html_text, artifact)
        for link, fields in extracted.items():
            if link not in merged:
                merged[link] = fields
    return merged
