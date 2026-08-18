"""Fetch and parse job listings from the IT-DAY Wix job portal."""

from __future__ import annotations

import html as html_lib
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

ITDAY_PORTAL_URL = "https://www.itday.dk/job-portal"
ITDAY_PORTAL_SOURCE = "IT-DAY Job Portal"
_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_SKIP_LINK_MARKERS = (
    "youtube.com",
    "facebook.com",
    "instagram.com",
    "itday.dk/job-portal",
    "itday.dk/privacy-policy",
)


def _portal_page_url(page: int) -> str:
    if page <= 1:
        return ITDAY_PORTAL_URL
    return f"{ITDAY_PORTAL_URL}?dynamic_page={page}"


def fetch_itday_portal_html(page: int = 1, *, timeout_sec: int = 15) -> str:
    url = _portal_page_url(page)
    req = Request(
        url,
        headers={
            "User-Agent": _DEFAULT_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    try:
        with urlopen(req, timeout=timeout_sec) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"IT-DAY portal fetch failed for {url}: {exc}") from exc


def _is_listing_link(href: str) -> bool:
    link = (href or "").strip()
    if not link.startswith(("http://", "https://")):
        return False
    lower = link.lower()
    if "linkedin.com" in lower:
        return "/jobs/" in lower
    if any(marker in lower for marker in _SKIP_LINK_MARKERS):
        return False
    if "itday.dk" in lower:
        return "/praktik" in lower
    return True


def _canonicalize_listing_link(href: str) -> str:
    if href.startswith("http://"):
        return "https://" + href[len("http://") :]
    return href


def _pick_card_link(item: BeautifulSoup) -> str:
    for anchor in item.select("a[href]"):
        href = html_lib.unescape(anchor.get("href", "").strip())
        if not href:
            continue
        href = _canonicalize_listing_link(urljoin(ITDAY_PORTAL_URL, href))
        if _is_listing_link(href):
            return href
    return ""


def _card_text_blocks(item: BeautifulSoup) -> list[str]:
    texts: list[str] = []
    for node in item.select(".wixui-collapsible-text__text"):
        text = node.get_text(strip=True)
        if text:
            texts.append(text)
    return texts


def _card_headings(item: BeautifulSoup) -> list[str]:
    headings: list[str] = []
    for node in item.select("h2"):
        text = node.get_text(strip=True)
        if text:
            headings.append(text)
    return headings


def _build_raw_text(company: str, title: str, place: str, work_type: str, link: str) -> str:
    lines = [
        ITDAY_PORTAL_SOURCE,
        f"Company: {company}",
        f"Title: {title}",
    ]
    if place:
        lines.append(f"Place: {place}")
    if work_type:
        lines.append(f"Type: {work_type}")
    lines.append(f"Link: {link}")
    return "\n".join(lines)


def parse_itday_portal_html(html_text: str) -> list[dict]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    entries: list[dict] = []
    seen_links: set[str] = set()

    for item in soup.select(".wixui-repeater__item"):
        link = _pick_card_link(item)
        if not link:
            continue
        dedupe_key = link.split("#", 1)[0]
        if dedupe_key in seen_links:
            continue
        seen_links.add(dedupe_key)

        texts = _card_text_blocks(item)
        headings = _card_headings(item)
        company = texts[0] if texts else ""
        title = texts[1] if len(texts) > 1 else ""
        place = headings[0] if headings else ""
        work_type = headings[1] if len(headings) > 1 else "Unknown"
        if not title and not company:
            continue

        entries.append(
            {
                "position_link": link,
                "company": company,
                "title": title,
                "place": place,
                "work_type": work_type or "Unknown",
                "raw_text": _build_raw_text(company, title, place, work_type, link),
                "source": ITDAY_PORTAL_SOURCE,
            }
        )
    return entries


def fetch_itday_portal_entries(
    *,
    max_pages: int = 10,
    timeout_sec: int = 15,
) -> list[dict]:
    entries: list[dict] = []
    seen_links: set[str] = set()

    for page in range(1, max_pages + 1):
        html_text = fetch_itday_portal_html(page, timeout_sec=timeout_sec)
        page_entries = parse_itday_portal_html(html_text)
        if not page_entries:
            break

        new_on_page = 0
        for entry in page_entries:
            dedupe_key = str(entry.get("position_link") or "").split("#", 1)[0]
            if not dedupe_key or dedupe_key in seen_links:
                continue
            seen_links.add(dedupe_key)
            entries.append(entry)
            new_on_page += 1

        if new_on_page == 0:
            break

    return entries
