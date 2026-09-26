"""Deterministic career-alert artifact drafts for known structural patterns."""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from bs4.element import Tag

from spejder.jobs.parsing.artifact_interpreter import (
    _CTA_LABELS,
    _title_from_ancestor_block,
)
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.companies import extract_company_title, sanitize_company_name
from spejder.jobs.parsing.links import unwrap_track_link

_FROM_NOISE = frozenset(
    {
        "unknown",
        "noreply",
        "no reply",
        "no-reply",
        "mailer daemon",
        "mail delivery",
        "postmaster",
    }
)
_FROM_EMAIL_NOISE_HOSTS = frozenset(
    {
        "gmail",
        "googlemail",
        "outlook",
        "hotmail",
        "yahoo",
        "icloud",
        "live",
        "msn",
        "aol",
        "noreply",
        "no-reply",
    }
)


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


def cta_labels_from_artifacts(
    artifacts: list[CareerAlertArtifact] | None,
) -> frozenset[str]:
    """Base CTA labels plus ``anchor_text_equals`` learned from overlays/shipped."""
    labels = set(_CTA_LABELS)
    for artifact in artifacts or []:
        for label in artifact.match.anchor_text_equals or []:
            cleaned = " ".join(str(label).split())
            if cleaned:
                labels.add(cleaned.casefold())
    return frozenset(labels)


def _company_from_from_header(from_hint: str) -> str:
    raw = (from_hint or "").strip()
    if not raw:
        return ""
    match = re.match(r'^"?([^"<]+)"?\s*<', raw)
    display = (match.group(1) if match else raw).strip().strip('"')
    display = re.sub(
        r"\s+(?:careers|jobs|job alerts?|recruiting|talent|hr)\s*$",
        "",
        display,
        flags=re.IGNORECASE,
    ).strip(" -|:")
    if "@" in display:
        _local, _, domain = display.partition("@")
        host = (domain.split(".")[0] or "").casefold()
        if host and host not in _FROM_EMAIL_NOISE_HOSTS:
            return host.replace("-", " ").title()
        return ""
    cleaned = sanitize_company_name(display)
    if cleaned and cleaned.casefold() not in _FROM_NOISE:
        return cleaned
    return ""


def _company_guess(
    *,
    html_text: str,
    host: str,
    title_hint: str = "",
    text: str = "",
    from_hint: str = "",
) -> str:
    # Prefer Subject / body hints via companies.py (no hard company shortlist).
    seed_text = (text or "").strip() or (html_text or "")
    company, _title = extract_company_title(seed_text, title_hint)
    company = sanitize_company_name(company)
    if company:
        return company
    from_company = _company_from_from_header(from_hint)
    if from_company:
        return from_company
    host_clean = (host or "").split(":")[0]
    if host_clean.startswith("www."):
        host_clean = host_clean[4:]
    # clicks.icims.eu → prefer second label when first is a tracker brand
    parts = [p for p in host_clean.split(".") if p]
    if len(parts) >= 2 and parts[0] in {"clicks", "track", "email", "mail", "go"}:
        label = parts[1]
    else:
        label = parts[0] if parts else ""
    return label.replace("-", " ").title() or "Unknown"


def draft_cta_ancestor_artifact(
    html_text: str,
    *,
    title_hint: str = "",
    text: str = "",
    from_hint: str = "",
    known_cta_labels: frozenset[str] | None = None,
) -> CareerAlertArtifact | None:
    """
    Draft an artifact for CTA-button digests (e.g. iCIMS "Apply here" + nearby <strong> title).

    Returns None when no qualifying job card is found on one host.
    Single-card digests are allowed (previously required ≥2).
    """
    if not html_text:
        return None
    cta_set = known_cta_labels if known_cta_labels is not None else frozenset(_CTA_LABELS)
    soup = BeautifulSoup(html_text, "html.parser")
    by_host: dict[str, list[tuple[str, str]]] = defaultdict(list)
    cta_labels: Counter[str] = Counter()

    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if compact.casefold() not in cta_set:
            continue
        href = unwrap_track_link(str(anchor.get("href") or ""))
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if not host or not path:
            continue
        title, _raw = _title_from_ancestor_block(anchor, cta_labels=cta_set)
        if not title or title.casefold() in cta_set:
            continue
        by_host[host].append((path, title))
        cta_labels[compact] += 1

    if not by_host:
        return None
    host, cards = max(by_host.items(), key=lambda item: len(item[1]))
    if len(cards) < 1:
        return None
    paths = [path for path, _title in cards]
    path_include = _common_path_include(paths)
    if not path_include:
        return None
    cta_label = cta_labels.most_common(1)[0][0]
    company = _company_guess(
        html_text=html_text,
        host=host,
        title_hint=title_hint,
        text=text,
        from_hint=from_hint,
    )
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
