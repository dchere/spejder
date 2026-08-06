"""Deterministic HTML shrinking for career-alert synthesis prompts."""

from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup
from bs4.element import Tag

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


def _href_for_prompt(href: str) -> str:
    """Drop query/fragment so Jobs2Web tracking params do not bloat the prompt."""
    raw = (href or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.split("?", 1)[0].split("#", 1)[0]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def _ancestor_heading(anchor: Tag) -> str:
    node: Tag | None = anchor
    for _ in range(14):
        parent = node.parent if node is not None else None
        if not isinstance(parent, Tag):
            return ""
        node = parent
        text = " ".join(node.get_text(" ", strip=True).split())
        if len(text) < 40:
            continue
        headings: list[str] = []
        for tag in node.find_all(["strong", "b", "h1", "h2", "h3", "h4"]):
            piece = " ".join(tag.get_text(" ", strip=True).split())
            if piece and piece.casefold() not in _CTA_LABELS:
                headings.append(piece)
        if headings:
            joined = " ".join(headings)
            return joined[:180]
        if node.name in ("tr", "li", "article", "section"):
            peeled = text
            for label in _CTA_LABELS:
                peeled = re.sub(rf"(?i)\b{re.escape(label)}\b", " ", peeled)
            return " ".join(peeled.split())[:180]
    return ""


def shrink_html_for_prompt(html_text: str, *, max_chars: int = 12000) -> str:
    """Keep anchors + nearby text; strip scripts/styles; hard-cap length."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()

    scored: list[tuple[int, str]] = []
    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue
        href = _href_for_prompt(anchor.get("href") or "")
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if len(text) > 160:
            text = text[:157] + "..."
        if not href:
            continue
        # Skip empty image/track anchors — they dominate iCIMS digests and waste budget.
        if not text:
            continue

        parent = anchor.parent
        context = ""
        if isinstance(parent, Tag):
            context = " ".join(parent.get_text(" ", strip=True).split())
            if len(context) > 240:
                context = context[:240]

        is_cta = text.casefold() in _CTA_LABELS
        heading = _ancestor_heading(anchor) if is_cta else ""
        if heading:
            line = f'<a href="{href}">{text}</a> :: {heading}'
            score = 0  # prefer CTA+heading rows
        elif context and text and text in context and context.casefold() != text.casefold():
            line = f'<a href="{href}">{text}</a> :: {context}'
            score = 1
        elif text:
            line = f'<a href="{href}">{text}</a>'
            score = 2 if is_cta else 3
        else:
            continue
        scored.append((score, line))

    scored.sort(key=lambda item: item[0])
    lines = [line for _, line in scored]
    # When CTA+heading rows exist, drop weaker anchors so opaque tracker URLs fit the budget.
    cta_heading_lines = [line for score, line in scored if score == 0]
    if cta_heading_lines:
        lines = cta_heading_lines[:8]

    if not lines:
        # Fallback: compact visible text
        text = " ".join(soup.get_text(" ", strip=True).split())
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]

    joined = "\n".join(lines)
    if len(joined) <= max_chars:
        return joined
    return joined[: max_chars - 3] + "..."
