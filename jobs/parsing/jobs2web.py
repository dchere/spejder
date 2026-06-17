import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link


def _work_type_from_parenthetical(compact: str) -> tuple[str, str]:
    work_type = "Unknown"
    text = compact
    wt_match = re.search(
        r"\s*\((Hybrid|Remote|On-site|Onsite)\)\s*$",
        compact,
        flags=re.IGNORECASE,
    )
    if wt_match:
        wt = (wt_match.group(1) or "").lower()
        if wt in ("on-site", "onsite"):
            work_type = "On-site"
        elif wt == "hybrid":
            work_type = "Hybrid"
        elif wt == "remote":
            work_type = "Remote"
        text = compact[: wt_match.start()].strip()
    return text, work_type


def _parse_jobs2web_anchor_text(compact: str) -> dict[str, str]:
    title = compact
    place = ""
    work_type = "Unknown"

    if "·" in compact:
        middot_text, work_type = _work_type_from_parenthetical(compact)
        middot_match = re.match(
            r"^(?P<title>.+?)\s*·\s*.+?\s*·\s*(?P<place>.+?)\s*$",
            middot_text,
            flags=re.IGNORECASE,
        )
        if middot_match:
            return {
                "title": (middot_match.group("title") or "").strip(),
                "place": (middot_match.group("place") or "").strip(),
                "work_type": work_type,
            }
        return {"title": middot_text, "place": "", "work_type": work_type}

    dash_match = re.match(r"^(?P<title>.+?)\s*-\s*(?P<place>.+)$", compact)
    if dash_match:
        return {
            "title": (dash_match.group("title") or "").strip(),
            "place": (dash_match.group("place") or "").strip(),
            "work_type": work_type,
        }

    return {"title": title, "place": place, "work_type": work_type}


def _extract_jobs2web_site_entries_by_link(
    html_text: str,
    *,
    host_substring: str,
    company: str,
    source: str,
    require_job_id_in_path: bool = False,
) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if host_substring not in host or "/job/" not in path:
            continue
        if require_job_id_in_path and not re.search(r"/job/.+/\d+", path):
            continue

        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if not compact:
            continue

        fields = _parse_jobs2web_anchor_text(compact)
        by_link[normalized] = {
            "title": fields["title"][:180],
            "company": company,
            "place": fields["place"][:180],
            "work_type": fields["work_type"],
            "raw_text": compact[:2500],
            "source": source,
        }

    return by_link


def _extract_novonordisk_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    return _extract_jobs2web_site_entries_by_link(
        html_text,
        host_substring="careers.novonordisk.com",
        company="Novo Nordisk",
        source="Novo Nordisk",
    )


def _extract_vestas_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    return _extract_jobs2web_site_entries_by_link(
        html_text,
        host_substring="careers.vestas.com",
        company="Vestas",
        source="Vestas",
        require_job_id_in_path=True,
    )


def _extract_danfoss_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    return _extract_jobs2web_site_entries_by_link(
        html_text,
        host_substring="jobs.danfoss.com",
        company="Danfoss",
        source="Danfoss",
    )
