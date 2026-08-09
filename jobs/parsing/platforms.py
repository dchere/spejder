import re
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from spejder.db import _extract_jobindex_id, _normalize_position_link, _provider_from_link

from .utils import (
    infer_jobindex_company_from_title,
    is_jobindex_company_echo_place,
    is_jobindex_noise_fragment,
    listing_place_hint_from_compact,
    looks_like_jobindex_company_name,
    looks_like_jobindex_job_title,
    looks_like_jobindex_place,
    merge_jobindex_place,
    peel_jobindex_dash_place,
    peel_jobindex_i_city_trailing_district,
    peel_jobindex_trailing_place,
    pick_jobindex_title,
    split_jobindex_trailing_postcode,
    split_title_trailing_i_place,
    strip_jobindex_company_prefix,
    strip_title_overlap_from_place,
)


def _extract_jobindex_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        if "jobindex.dk" not in (parsed.netloc or "").lower():
            continue

        job_id = _extract_jobindex_id(href)
        if not job_id:
            continue

        normalized = f"https://www.jobindex.dk/jobannonce/{job_id}"
        block = anchor.find_parent("table") or anchor.find_parent("tr")
        if not block:
            continue

        compact = " ".join(block.get_text(" ", strip=True).split())
        if len(compact) < 30:
            continue

        title = ""
        title_candidates = []
        jobannonce_texts: set[str] = set()
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            t2 = _extract_jobindex_id(href2)
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if (
                t2 == job_id
                and txt2
                and txt2.lower()
                not in {
                    "view job",
                    "apply",
                    "about the company",
                    "save job",
                    "settings",
                }
            ):
                title_candidates.append(txt2)
                jobannonce_texts.add(txt2.casefold())

        listing_place_hint = listing_place_hint_from_compact(compact)
        if is_jobindex_company_echo_place(listing_place_hint, title_candidates):
            listing_place_hint = ""

        def _is_place_candidate(text: str) -> bool:
            if looks_like_jobindex_place(text):
                return True
            hint = (listing_place_hint or "").strip()
            return bool(hint and text.strip().casefold() == hint.casefold())

        company = ""
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if txt2 and "jobindex.dk" not in href2.lower():
                if looks_like_jobindex_company_name(txt2) and not _is_place_candidate(
                    txt2
                ):
                    company = txt2[:180]
                    break

        if not company:
            # Prefer company-like /jobannonce/ anchors (ApS/A/S) over plain fragments.
            for candidate in title_candidates:
                if not looks_like_jobindex_company_name(candidate):
                    continue
                if _is_place_candidate(candidate):
                    continue
                if re.search(r"\b(?:aps|a/s|gmbh|group)\b\.?$", candidate, re.I):
                    company = candidate[:180]
                    break
            if not company:
                for candidate in title_candidates:
                    if not looks_like_jobindex_company_name(candidate):
                        continue
                    if _is_place_candidate(candidate):
                        continue
                    if len(candidate) <= 60 and len(candidate.split()) <= 6:
                        company = candidate[:180]
                        break
            if not company:
                for fragment in block.stripped_strings:
                    txt = " ".join(str(fragment or "").split()).strip()
                    if not txt:
                        continue
                    pieces = [txt]
                    if re.search(r"[?!]", txt):
                        pieces.extend(
                            part.strip()
                            for part in re.split(r"[?!]\s*", txt)
                            if part.strip()
                        )
                    for piece in pieces:
                        if piece.casefold() in jobannonce_texts:
                            continue
                        if is_jobindex_noise_fragment(piece):
                            continue
                        if _is_place_candidate(piece):
                            continue
                        if not looks_like_jobindex_company_name(piece):
                            continue
                        company = piece[:180]
                        break
                    if company:
                        break

        if company and (
            not looks_like_jobindex_company_name(company)
            or _is_place_candidate(company)
        ):
            company = ""

        title = pick_jobindex_title(
            title_candidates,
            company=company,
            listing_place_hint=listing_place_hint,
        )
        place = ""
        if listing_place_hint and listing_place_hint.casefold() != title.casefold():
            place = listing_place_hint[:180]
        else:
            for candidate in title_candidates:
                if (
                    _is_place_candidate(candidate)
                    and candidate.casefold() != title.casefold()
                ):
                    place = candidate.strip(" -|:")[:180]
                    break
        if place and title:
            place = strip_title_overlap_from_place(title, place)
        if not place and title:
            _, dash_place = peel_jobindex_dash_place(title)
            if dash_place and not (
                company and dash_place.casefold() == company.casefold()
            ):
                place = dash_place
        if place and company and place.casefold() == company.casefold():
            place = ""

        company_is_title = bool(
            company
            and title_candidates
            and any(
                company.strip().casefold() == candidate.strip().casefold()
                for candidate in title_candidates
            )
        )
        if (
            len(title_candidates) == 1
            and company
            and not looks_like_jobindex_job_title(company)
            and company_is_title
        ):
            m_compact_title = re.search(
                r"^(?:(?:"
                + re.escape(company)
                + r")\s+)+(?P<title>.+?)\s+\d+\s+min\b",
                compact,
                flags=re.IGNORECASE,
            )
            if not m_compact_title:
                m_compact_title = re.search(
                    re.escape(company) + r"\s+(?P<title>.+?)\s+\d+\s+min\b",
                    compact,
                    flags=re.IGNORECASE,
                )
            if m_compact_title:
                title = (m_compact_title.group("title") or "").strip(" -|:")
                title, compact_place = split_jobindex_trailing_postcode(title)
                if not compact_place:
                    peeled_title, peeled_place = peel_jobindex_trailing_place(title)
                    if peeled_place:
                        title = peeled_title
                        compact_place = peeled_place
                if not compact_place:
                    peeled_title, peeled_place = peel_jobindex_i_city_trailing_district(
                        title
                    )
                    if peeled_place:
                        title = peeled_title
                        compact_place = peeled_place
                if compact_place:
                    place = merge_jobindex_place(title, compact_place)

        if title:
            if not place:
                m_place = re.search(
                    re.escape(title) + r"\s+(.{2,200}?)\s+\d+\s+min\b",
                    compact,
                    flags=re.IGNORECASE,
                )
                if m_place:
                    place = strip_jobindex_company_prefix(
                        m_place.group(1).strip(" -|:")[:180],
                        company,
                    )
            place = merge_jobindex_place(title, place)
            if not place:
                _, split_place = split_title_trailing_i_place(title)
                if (
                    split_place
                    and company
                    and split_place.casefold() == company.casefold()
                ):
                    split_place = ""
                place = merge_jobindex_place(title, split_place)
            if place and company and place.casefold() == company.casefold():
                place = ""
            if not place:
                peeled_title, peeled_place = peel_jobindex_trailing_place(title)
                if peeled_place:
                    title = peeled_title
                    place = peeled_place
            if not place:
                peeled_title, peeled_district = peel_jobindex_i_city_trailing_district(
                    title
                )
                if peeled_district:
                    title = peeled_title
                    place = merge_jobindex_place(title, peeled_district)
            if place:
                place = strip_title_overlap_from_place(title, place)
                if company:
                    place = strip_title_overlap_from_place(company, place)
                    place = strip_jobindex_company_prefix(place, company)
            if place and company and place.casefold() == company.casefold():
                place = ""

        if not company and title:
            company = infer_jobindex_company_from_title(title)[:180]
        if not company and title:
            title_at = compact.find(title)
            if title_at > 0:
                before = compact[:title_at].rstrip()
                segments = [
                    part.strip()
                    for part in re.split(r"[.!?]\s*", before)
                    if part.strip()
                ]
                segment = segments[-1] if segments else before
                tokens = [
                    tok.strip(",;:«»\"'()[]")
                    for tok in segment.split()
                    if tok.strip(",;:«»\"'()[]")
                ]
                connectors = {"for", "og", "af", "and", "the", "de", "von"}

                def _brand_from_tokens(cand_tokens: list[str]) -> str:
                    if not cand_tokens:
                        return ""
                    if not all(
                        re.match(r"[A-ZÆØÅ]", tok)
                        or (
                            tok.casefold() in connectors
                            and 0 < idx < len(cand_tokens) - 1
                        )
                        for idx, tok in enumerate(cand_tokens)
                    ):
                        return ""
                    brand = " ".join(cand_tokens)
                    if looks_like_jobindex_company_name(
                        brand
                    ) and not _is_place_candidate(brand):
                        return brand[:180]
                    return ""

                # Prefer a leading capitalized name in the last clause (card
                # often starts with the employer), then tokens immediately
                # before the title.
                if tokens:
                    lead: list[str] = []
                    for tok in tokens:
                        if re.match(r"[A-ZÆØÅ]", tok):
                            lead.append(tok)
                            if len(
                                [t for t in lead if t.casefold() not in connectors]
                            ) >= 3:
                                break
                        elif lead and tok.casefold() in connectors:
                            lead.append(tok)
                        else:
                            break
                    brand = _brand_from_tokens(lead)
                    if brand:
                        company = brand
                if not company:
                    for n in range(min(3, len(tokens)), 0, -1):
                        brand = _brand_from_tokens(tokens[-n:])
                        if brand:
                            company = brand
                            break

        m_desc = re.search(
            r"settings\s*\)\s*(.*?)\s*PUBLISHED\s*:", compact, flags=re.IGNORECASE
        )
        extracted = ""
        if m_desc:
            extracted = m_desc.group(1).strip()
        else:
            m_desc2 = re.search(
                r"\d+\s+min\s*\(.*?\)\s*(.*?)\s*PUBLISHED\s*:",
                compact,
                flags=re.IGNORECASE,
            )
            if m_desc2:
                extracted = m_desc2.group(1).strip()

        raw_text = compact[:2500]
        if extracted:
            merged = f"{extracted}\n\n{raw_text}".strip()
            raw_text = merged[:2500]

        by_link[normalized] = {
            "title": title[:180],
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": raw_text,
            "source": "Jobindex",
        }

    return by_link


def _extract_demant_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "careers.demant.com" not in host or "/job/" not in path:
            continue

        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if not compact:
            continue

        title = compact
        place = ""
        match = re.match(r"^(?P<title>.+?)\s*-\s*(?P<place>.+)$", compact)
        if match:
            title = (match.group("title") or "").strip()
            place = (match.group("place") or "").strip()

        by_link[normalized] = {
            "title": title[:180],
            "company": "Demant Group",
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "source": "Demant",
        }

    return by_link


def _extract_google_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized or _provider_from_link(normalized) != "Google Careers":
            continue

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        block = anchor.find_parent(
            "td") or anchor.find_parent("tr") or anchor.parent
        compact = " ".join(block.get_text(
            " ", strip=True).split()) if block else title
        compact = compact[:2500]

        company = "Google"
        place = ""

        suffix = compact[len(title):].strip(
        ) if compact.startswith(title) else compact
        location_match = re.match(
            r"^(?P<company>[^–-]{2,80}?)\s*[–-]\s*(?P<place>.+?)(?:\s+\d+\s+(?:minute|minutes|hour|hours|day|days|week|weeks)\s+ago\b|$)",
            suffix,
            flags=re.IGNORECASE,
        )
        if location_match:
            company = (location_match.group("company")
                       or company).strip(" -|:")[:180]
            place = (location_match.group("place") or "").strip(" -|:")[:180]

        by_link[normalized] = {
            "title": title[:180],
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": compact,
            "source": "Google Careers",
        }

    return by_link


