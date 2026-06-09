import re
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from spejder.db import _normalize_position_link, _provider_from_link

from .constants import EASY_APPLY_PATTERN


def _work_type_from_html_for_link(html_text: str, normalized_link: str) -> str:
    if not html_text or not normalized_link:
        return ""
    low = html_text.lower()
    m = re.search(r"/jobs/view/(\d+)", normalized_link.lower())
    if not m:
        return ""
    job_id = m.group(1)

    candidates = [f"/jobs/view/{job_id}", f"/comm/jobs/view/{job_id}"]
    link_positions = []
    for needle in candidates:
        link_positions.extend([m.start()
                              for m in re.finditer(re.escape(needle), low)])

    if not link_positions:
        return ""

    token_patterns = {
        "Hybrid": r"\bhybrid\b",
        "Remote": r"\bremote\b",
        "On-site": r"\bon-site\b|\bonsite\b",
    }

    best_type = ""
    best_distance = None
    for work_type, pattern in token_patterns.items():
        token_positions = [m.start() for m in re.finditer(pattern, low)]
        if not token_positions:
            continue
        for link_pos in link_positions:
            nearest_distance = min(
                abs(token_pos - link_pos) for token_pos in token_positions
            )
            if best_distance is None or nearest_distance < best_distance:
                best_distance = nearest_distance
                best_type = work_type

    if best_distance is not None and best_distance <= 6000:
        return best_type
    return ""


def _has_easy_apply_signal(text: str) -> bool:
    compact = " ".join((text or "").split())
    return bool(compact and EASY_APPLY_PATTERN.search(compact))


def _has_linkedin_public_easy_apply(
    position_link: str, easy_apply_cache: Optional[dict[str, bool]] = None
) -> bool:
    link = (position_link or "").strip()
    if not link or "linkedin.com/" not in link.lower():
        return False
    if easy_apply_cache is not None and link in easy_apply_cache:
        return bool(easy_apply_cache[link])

    req = Request(
        link,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
    )

    has_easy_apply = False
    try:
        with urlopen(req, timeout=8) as response:
            ctype = (response.headers.get("Content-Type") or "").lower()
            if not ctype or "html" in ctype or "text" in ctype:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                html_text = payload.decode(charset, errors="ignore")
                has_easy_apply = "public_jobs_apply-link-onsite" in html_text.lower()
    except (HTTPError, URLError, TimeoutError, ValueError):
        has_easy_apply = False
    except Exception:
        has_easy_apply = False

    if easy_apply_cache is not None:
        easy_apply_cache[link] = has_easy_apply
    return has_easy_apply


def _is_linkedin_reference_position_link(raw_link: str, normalized_link: str) -> bool:
    low = (raw_link or "").lower()
    if "linkedin.com" not in low:
        return False

    parsed = urlparse(raw_link)
    q = parse_qs(parsed.query)
    reference_id = (
        q.get("referenceJobId", [""])[0] or q.get(
            "referencejobid", [""])[0] or ""
    ).strip()
    if not reference_id or not reference_id.isdigit():
        return False

    m = re.search(
        r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", (normalized_link or "").lower()
    )
    if not m:
        return False

    return m.group(1) == reference_id


def _is_linkedin_boilerplate_entry(entry: dict) -> bool:
    source = (
        entry.get("source") or _provider_from_link(
            entry.get("position_link", ""))
    ).lower()
    if source != "linkedin":
        return False

    boilerplate_phrases = [
        "jobs similar to",
        "new jobs match your preferences",
        "job alert",
        "viewed jobs",
    ]

    title = (entry.get("title") or "").strip().lower()
    company = (entry.get("company") or "").strip().lower()
    place = (entry.get("place") or "").strip().lower()

    return any(
        phrase in value
        for phrase in boilerplate_phrases
        for value in [title, company, place]
    )


