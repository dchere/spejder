import re

_TITLE_TRAILING_I_PLACE_RE = re.compile(
    r"\s+i\s+([A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s*\([^)]+\))?)\s*$"
)
_JOBINDEX_COMPANY_SUFFIX_RE = re.compile(r"\b(?:aps|a/s|gmbh|group)\b\.?$", re.IGNORECASE)
_JOBINDEX_POSTCODE_PREFIX_RE = re.compile(r"^\d{3,5}\s+")
_JOBINDEX_TRAILING_POSTCODE_PLACE_RE = re.compile(
    r"(\d{3,5}\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*){0,3})$"
)
_JOBINDEX_TRAILING_MULTI_CITY_RE = re.compile(
    r"\s+(?P<place>[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*(?:\s+or\s+[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*)+)\s*$"
)


def split_title_trailing_i_place(title: str) -> tuple[str, str]:
    """Split trailing ``i City (District)`` location suffix from a job title."""
    title_clean = str(title or "").strip()
    if not title_clean:
        return "", ""

    match = _TITLE_TRAILING_I_PLACE_RE.search(title_clean)
    if not match:
        return title_clean, ""

    parsed_place = (match.group(1) or "").strip()
    parsed_title = title_clean[: match.start()].strip()
    if parsed_title and parsed_place:
        return parsed_title, parsed_place
    return title_clean, ""


def _normalize_jobindex_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).strip(" -|:").casefold()


def pick_jobindex_title(candidates: list[str], company: str = "") -> str:
    best = ""
    best_score: tuple[int, int, int] | None = None
    normalized_company = _normalize_jobindex_text(company)
    for candidate in candidates:
        cleaned = " ".join(str(candidate or "").split()).strip()
        if not cleaned:
            continue

        normalized_candidate = _normalize_jobindex_text(cleaned)
        is_company_match = int(
            bool(normalized_company and normalized_candidate == normalized_company)
        )
        has_company_suffix = int(bool(_JOBINDEX_COMPANY_SUFFIX_RE.search(cleaned)))
        score = (is_company_match, has_company_suffix, -len(cleaned))
        if best_score is None or score < best_score:
            best = cleaned
            best_score = score
    return best


def merge_jobindex_place(title: str, listing_place: str) -> str:
    _, parsed_place = split_title_trailing_i_place(title)
    parsed_place = str(parsed_place or "").strip()
    listing_clean = str(listing_place or "").strip(" -|:")
    if not parsed_place:
        return listing_clean
    if not listing_clean:
        return parsed_place

    if " or " in listing_clean or "/" in listing_clean:
        return listing_clean

    city = parsed_place
    parsed_district = ""
    district_match = re.match(r"^(?P<city>.+?)\s*\((?P<district>[^)]+)\)$", parsed_place)
    if district_match:
        city = (district_match.group("city") or "").strip()
        parsed_district = (district_match.group("district") or "").strip()

    city_start = re.compile(
        rf"^{re.escape(city)}\b",
        flags=re.IGNORECASE,
    )
    if city_start.match(listing_clean):
        return listing_clean

    listing_candidate = listing_clean
    trailing_postcode = _JOBINDEX_TRAILING_POSTCODE_PLACE_RE.search(listing_clean)
    if trailing_postcode:
        listing_candidate = trailing_postcode.group(1).strip()
    listing_district = _JOBINDEX_POSTCODE_PREFIX_RE.sub("", listing_candidate).strip()
    if not listing_district:
        return parsed_place
    if parsed_district:
        return parsed_place
    if _normalize_jobindex_text(listing_district) == _normalize_jobindex_text(city):
        return parsed_place
    return f"{city} {listing_district}".strip()


def peel_jobindex_i_city_trailing_district(title: str) -> tuple[str, str]:
    """Split ``… i City District`` when district tokens follow the city (no postcode)."""
    title_clean = str(title or "").strip()
    if not title_clean:
        return "", ""

    match = re.match(
        r"^(?P<prefix>.+?\s+i\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s*\([^)]+\))?)\s+"
        r"(?P<district>[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*(?:\s+[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*){0,2})$",
        title_clean,
    )
    if not match:
        return title_clean, ""

    prefix = (match.group("prefix") or "").strip()
    district = (match.group("district") or "").strip()
    if prefix and district:
        return prefix, district
    return title_clean, ""


def peel_jobindex_trailing_place(title: str) -> tuple[str, str]:
    """Split trailing multi-city suffix (e.g. ``Aarhus or Copenhagen``) from a title."""
    title_clean = str(title or "").strip()
    if not title_clean:
        return "", ""

    match = _JOBINDEX_TRAILING_MULTI_CITY_RE.search(title_clean)
    if not match:
        return title_clean, ""

    place = (match.group("place") or "").strip()
    peeled_title = title_clean[: match.start()].strip()
    if peeled_title and place:
        return peeled_title, place
    return title_clean, ""


def split_jobindex_trailing_postcode(title: str) -> tuple[str, str]:
    """Split trailing ``8260 Tranbjerg J`` listing suffix from a title."""
    title_clean = str(title or "").strip()
    if not title_clean:
        return "", ""

    match = _JOBINDEX_TRAILING_POSTCODE_PLACE_RE.search(title_clean)
    if not match:
        return title_clean, ""

    listing = match.group(1).strip()
    peeled_title = title_clean[: match.start()].strip()
    if peeled_title and listing:
        return peeled_title, listing
    return title_clean, ""


def strip_jobindex_company_prefix(value: str, company: str) -> str:
    cleaned = str(value or "").strip()
    company_clean = str(company or "").strip()
    if not cleaned or not company_clean:
        return cleaned
    prefix = f"{company_clean} "
    if cleaned.casefold().startswith(prefix.casefold()):
        return cleaned[len(prefix):].strip(" -|:")
    return cleaned


def first_non_empty(lines: list[str]) -> str:
    for line in lines:
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


