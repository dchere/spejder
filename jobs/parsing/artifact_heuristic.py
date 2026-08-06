"""Deterministic career-alert artifact drafts for known structural patterns."""

from __future__ import annotations

from collections import Counter, defaultdict
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

from spejder.jobs.parsing.artifact_interpreter import (
    _CTA_LABELS,
    _title_from_ancestor_block,
)
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact


def _common_path_include(paths: list[str]) -> str | None:
    if not paths:
        return None
    for candidate in ("/f/a/", "/job/", "/jobs/", "/career/"):
        if all(candidate in path for path in paths):
            return candidate
    # Shared first path segments (e.g. /jobs/view/).
    split_paths = [path.strip("/").split("/") for path in paths if path.strip("/")]
    if not split_paths:
        return None
    shared: list[str] = []
    for parts in zip(*split_paths):
        if len(set(parts)) != 1:
            break
        shared.append(parts[0])
    if not shared:
        return None
    return "/" + "/".join(shared[:2]) + ("/" if len(shared) >= 1 else "")


def _company_guess(html_text: str, host: str) -> str:
    low = (html_text or "").casefold()
    for name in (
        "Schneider Electric",
        "Danfoss",
        "Vestas",
        "Novo Nordisk",
        "Capgemini",
    ):
        if name.casefold() in low:
            return name
    host_clean = (host or "").split(":")[0]
    if host_clean.startswith("www."):
        host_clean = host_clean[4:]
    label = host_clean.split(".")[0]
    return label.replace("-", " ").title() or "Unknown"


def draft_cta_ancestor_artifact(html_text: str) -> CareerAlertArtifact | None:
    """
    Draft an artifact for CTA-button digests (e.g. iCIMS "Apply here" + nearby <strong> title).

    Returns None when fewer than two qualifying job cards are found on one host.
    """
    if not html_text:
        return None
    soup = BeautifulSoup(html_text, "html.parser")
    by_host: dict[str, list[tuple[str, str]]] = defaultdict(list)
    cta_labels: Counter[str] = Counter()

    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if compact.casefold() not in _CTA_LABELS:
            continue
        href = str(anchor.get("href") or "")
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if not host or not path:
            continue
        title, _raw = _title_from_ancestor_block(anchor)
        if not title or title.casefold() in _CTA_LABELS:
            continue
        by_host[host].append((path, title))
        cta_labels[compact] += 1

    if not by_host:
        return None
    host, cards = max(by_host.items(), key=lambda item: len(item[1]))
    if len(cards) < 2:
        return None
    paths = [path for path, _title in cards]
    path_include = _common_path_include(paths)
    if not path_include:
        return None
    cta_label = cta_labels.most_common(1)[0][0]
    company = _company_guess(html_text, host)
    return CareerAlertArtifact.model_validate(
        {
            "id": "heuristic_cta_pending",
            "version": 1,
            "priority": 60,
            "enabled": True,
            "match": {
                "host_substrings": [host],
                "path_includes": [path_include],
                "anchor_text_equals": [cta_label],
            },
            "extract": {"mode": "filtered_links"},
            "fields": {
                "from_anchor": "ancestor_strong_or_first_line",
                "company": company,
                "source": company,
            },
            "source": "heuristic",
        }
    )
