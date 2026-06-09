import re

from bs4 import BeautifulSoup

from spejder.db import _normalize_position_link

from .links import _is_job_link
from .text_parser import _infer_work_type_from_text


def _parse_card_text_fields(card_text: str) -> dict[str, str]:
    compact = " ".join((card_text or "").split())
    if not compact:
        return {"title": "", "company": "", "place": "", "work_type": ""}

    work_type = _infer_work_type_from_text(compact)
    match = re.search(
        r"^(?P<title>.+?)\s+(?P<company>[^·\(]{2,}?)\s*·\s*(?P<place>[^\(]{2,}?)\s*(?:\((?P<wt>Hybrid|Remote|On-site|Onsite)\))",
        compact,
        flags=re.IGNORECASE,
    )
    if not match:
        return {"title": "", "company": "", "place": "", "work_type": work_type}

    title = match.group("title").strip(" -|:")[:180]
    company = match.group("company").strip(" -|:")[:180]
    place = match.group("place").strip(" -|:")[:180]
    wt = match.group("wt") or ""
    if wt:
        wt_low = wt.lower()
        if wt_low == "onsite" or wt_low == "on-site":
            work_type = "On-site"
        elif wt_low == "hybrid":
            work_type = "Hybrid"
        elif wt_low == "remote":
            work_type = "Remote"

    return {
        "title": title,
        "company": company,
        "place": place,
        "work_type": work_type,
    }


def _parse_anchor_fragments(fragments: list[str]) -> dict[str, str]:
    if not fragments:
        return {"title": "", "company": "", "place": "", "work_type": ""}

    detail_index = -1
    for i, frag in enumerate(fragments):
        if "·" in frag:
            detail_index = i
            break

    if detail_index > 0:
        title = " ".join(fragments[:detail_index]).strip(" -|:")[:180]
    else:
        title = fragments[0].strip(" -|:")[:180]

    company = ""
    place = ""
    work_type = _infer_work_type_from_text(" ".join(fragments))

    detail_line = ""
    for frag in fragments:
        if "·" in frag:
            detail_line = frag
            break

    if detail_line:
        left, right = detail_line.split("·", 1)
        company = left.strip(" -|:")[:180]
        right = right.strip()

        wt_match = re.search(
            r"\((Hybrid|Remote|On-site|Onsite)\)", right, flags=re.IGNORECASE
        )
        if wt_match:
            wt = wt_match.group(1).lower()
            if wt in ("on-site", "onsite"):
                work_type = "On-site"
            elif wt == "hybrid":
                work_type = "Hybrid"
            elif wt == "remote":
                work_type = "Remote"
            right = re.sub(
                r"\((Hybrid|Remote|On-site|Onsite)\)", "", right, flags=re.IGNORECASE
            ).strip()

        place = right.strip(" -|:")[:180]

    if not place and " - " in title:
        maybe_title, maybe_place = title.rsplit(" - ", 1)
        if maybe_title and maybe_place:
            maybe_place_low = maybe_place.lower()
            if (
                "," in maybe_place
                or maybe_place_low.endswith(" dk")
                or "denmark" in maybe_place_low
            ):
                title = maybe_title.strip(" -|:")[:180]
                place = maybe_place.strip(" -|:")[:180]

    return {
        "title": title,
        "company": company,
        "place": place,
        "work_type": work_type,
    }


def _extract_html_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}
    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    def field_score(fields: dict[str, str], has_detail: bool) -> tuple[int, int, int]:
        count = sum(
            1 for key in ["title", "company", "place", "work_type"] if fields.get(key)
        )
        richness = (
            len(fields.get("title", ""))
            + len(fields.get("company", ""))
            + len(fields.get("place", ""))
        )
        return (1 if has_detail else 0, count, richness)

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not _is_job_link(normalized):
            continue

        fragments = [s.strip()
                     for s in anchor.stripped_strings if s and s.strip()]
        fields = _parse_anchor_fragments(fragments)
        has_detail = any("·" in frag for frag in fragments)

        node = anchor
        card_text = ""
        for _ in range(8):
            if not node:
                break
            txt = " ".join(node.get_text(" ", strip=True).split())
            if txt and len(txt) >= 30:
                card_text = txt
            if (
                node.name in ("tr", "table", "li", "div", "td")
                and 30 <= len(txt) <= 900
            ):
                card_text = txt
                break
            node = node.parent

        if has_detail and (not fields.get("company") or not fields.get("place")):
            card_fields = _parse_card_text_fields(card_text)
            for key in ["title", "company", "place", "work_type"]:
                if not fields.get(key) and card_fields.get(key):
                    fields[key] = card_fields[key]

        fields["raw_text"] = (" | ".join(fragments)
                              if fragments else card_text)[:800]
        fields["_has_detail"] = "1" if has_detail else "0"

        current = by_link.get(normalized)
        current_has_detail = (current or {}).get("_has_detail") == "1"
        if not current or field_score(fields, has_detail) > field_score(
            current, current_has_detail
        ):
            by_link[normalized] = fields

    for value in by_link.values():
        value.pop("_has_detail", None)

    return by_link


