"""Deterministic HTML shrinking for career-alert synthesis prompts."""

from __future__ import annotations

import re
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup


def _href_for_prompt(href: str) -> str:
    """Drop query/fragment so Jobs2Web tracking params do not bloat the prompt."""
    raw = (href or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw.split("?", 1)[0].split("#", 1)[0]
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def shrink_html_for_prompt(html_text: str, *, max_chars: int = 12000) -> str:
    """Keep anchors + nearby text; strip scripts/styles; hard-cap length."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript"]):
        tag.decompose()

    lines: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = _href_for_prompt(anchor.get("href") or "")
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if len(text) > 160:
            text = text[:157] + "..."
        if not href:
            continue
        parent = anchor.parent
        context = ""
        if parent is not None:
            context = " ".join(parent.get_text(" ", strip=True).split())
            if len(context) > 240:
                context = context[:240]
        if context and text and text in context:
            lines.append(f'<a href="{href}">{text}</a> :: {context}')
        elif text:
            lines.append(f'<a href="{href}">{text}</a>')
        else:
            lines.append(f'<a href="{href}"></a>')

    if not lines:
        # Fallback: compact visible text
        text = " ".join(soup.get_text(" ", strip=True).split())
        text = re.sub(r"\s+", " ", text).strip()
        return text[:max_chars]

    joined = "\n".join(lines)
    if len(joined) <= max_chars:
        return joined
    return joined[: max_chars - 3] + "..."
