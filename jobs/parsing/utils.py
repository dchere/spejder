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
_JOBINDEX_PLACE_ONLY_RE = re.compile(
    r"^(?:"
    r"\d{3,5}\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*){0,3}"
    r"|[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*(?:\s+or\s+[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]+)+"
    r"|[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*(?:\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*){0,2}\s+[A-ZÆØÅ]$"
    r")$"
)
_JOBINDEX_TITLE_MARKER_RE = re.compile(
    r"(?i)(?:…|\.\.\.|\b(?:søger|seeks|looking\s+for|engineer|manager|"
    r"student|medhjælper|paralegal|assistant|assistent|rådgiver|udvikler|"
    r"consultant|elev|butiks|kasse|flaske|medarbejder)\b)"
)
_JOBINDEX_ROLE_DASH_PLACE_RE = re.compile(
    r"\s+-\s+[A-ZÆØÅa-zæøå]"
)
_JOBINDEX_COMPANY_FROM_TITLE_RE = re.compile(
    r"^([A-ZÆØÅ][\w.&-]{1,40})\s+søger\b",
    flags=re.IGNORECASE,
)
_JOBINDEX_NOISE_FRAGMENT_RE = re.compile(
    r"(?i)^(?:published\b|\d+\s+min\b|settings|view job|apply|about the company|save job)\b"
)
_JOBINDEX_TEASER_FRAGMENT_RE = re.compile(
    r"(?i)^(?:vil du|do you|would you|wanna|hvad |som )\b"
)
# Case-sensitive on purpose: IGNORECASE makes ``til …`` match ``[A-Z]…``.
# Bare places are single city or ``City X`` district letter — not ``Lystrup Lystrup``.
_JOBINDEX_LISTING_PLACE_BEFORE_MIN_RE = re.compile(
    r"(?P<place>"
    r"\d{3,5}\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s+[A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*){0,3}"
    r"|[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*(?:\s+or\s+[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]+)+"
    r"|[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*\s+[A-ZÆØÅ]"
    r"|[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*"
    r")\s+\d+\s+min\b"
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


def looks_like_jobindex_place(value: str) -> bool:
    """True for listing locations (postcode, ``City or City``, ``Aarhus N``)."""
    cleaned = " ".join(str(value or "").split()).strip(" -|:")
    if not cleaned or len(cleaned) > 60:
        return False
    if _JOBINDEX_COMPANY_SUFFIX_RE.search(cleaned):
        return False
    if _JOBINDEX_TITLE_MARKER_RE.search(cleaned):
        return False
    words = cleaned.split()
    if not 1 <= len(words) <= 4:
        return False
    if cleaned[:1].isdigit():
        return bool(_JOBINDEX_PLACE_ONLY_RE.fullmatch(cleaned))
    if " or " in cleaned.casefold():
        return bool(_JOBINDEX_PLACE_ONLY_RE.fullmatch(cleaned))
    # District letter: ``Aarhus N``, ``Tranbjerg J`` — not agency names.
    if len(words) >= 2 and re.fullmatch(r"[A-ZÆØÅ]", words[-1] or ""):
        return bool(_JOBINDEX_PLACE_ONLY_RE.fullmatch(cleaned))
    return False


def listing_place_hint_from_compact(compact: str) -> str:
    """Best-effort place token immediately before ``N min`` in a Jobindex card."""
    text = " ".join(str(compact or "").split())
    if not text:
        return ""
    matches = list(_JOBINDEX_LISTING_PLACE_BEFORE_MIN_RE.finditer(text))
    if not matches:
        return ""

    def _rank(match: re.Match[str]) -> tuple[int, int, int]:
        place = (match.group("place") or "").strip()
        has_postcode = int(not place[:1].isdigit())
        has_or = int(" or " not in place.casefold())
        return (has_postcode, has_or, len(place))

    best = min(matches, key=_rank)
    place = (best.group("place") or "").strip(" -|:")
    # Avoid absorbing the trailing ``S`` from ``A/S``.
    prefix = text[: best.start()]
    if prefix.endswith("/"):
        place = re.sub(r"^[Ss]\s+", "", place)
    words = place.split()
    if len(words) == 2 and words[0].casefold() == words[1].casefold():
        place = words[0]
    return place


def looks_like_jobindex_job_title(value: str) -> bool:
    """True when text looks like a Jobindex role line rather than a company name."""
    cleaned = " ".join(str(value or "").split()).strip()
    if not cleaned:
        return False
    if "…" in cleaned or "..." in cleaned:
        return True
    if len(cleaned) > 80:
        return True
    if _JOBINDEX_ROLE_DASH_PLACE_RE.search(cleaned):
        return True
    # ALL-CAPS multi-word shouty titles (volunteer ads, etc.).
    letters = re.sub(r"[^A-Za-zÆØÅæøå]", "", cleaned)
    if len(cleaned.split()) >= 3 and letters and letters == letters.upper():
        return True
    # Substring stems catch compounds like ``Butikselev`` / ``Kasseassistent``.
    if re.search(
        r"(?i)(?:elev|assistent|medarbejder|butiks|kasseassistent|flaske|"
        r"trainee|internship|\baudit\b|\bintern\b|frivillig|rundviser|\bbliv\b)",
        cleaned,
    ):
        return True
    # ``Audit Trainee i EY`` — role phrase ending in ``i Brand``.
    if (
        len(cleaned.split()) >= 3
        and re.search(r"\s+i\s+[A-ZÆØÅ][\w.&-]{1,40}\s*$", cleaned)
    ):
        return True
    if _JOBINDEX_TITLE_MARKER_RE.search(cleaned):
        return True
    return bool(
        re.search(r"(?i)\b(?:søger|seeks|looking\s+for)\b", cleaned)
    )


def is_jobindex_company_echo_place(
    place_hint: str, title_candidates: list[str]
) -> bool:
    """True when a listing place is really the company echoed from ``i Brand``."""
    hint = " ".join(str(place_hint or "").split()).strip()
    if not hint:
        return False
    hint_cf = hint.casefold()
    candidates = [
        " ".join(str(c or "").split()).strip()
        for c in title_candidates
        if str(c or "").strip()
    ]
    has_hint_anchor = any(c.casefold() == hint_cf for c in candidates)
    has_i_hint_title = any(
        c.casefold() != hint_cf
        and re.search(rf"\bi\s+{re.escape(hint)}\b", c, flags=re.IGNORECASE)
        for c in candidates
    )
    return bool(has_hint_anchor and has_i_hint_title)


def looks_like_jobindex_company_name(value: str) -> bool:
    """Reject punctuation, teasers, places, and role lines used as company."""
    cleaned = " ".join(str(value or "").split()).strip()
    if len(cleaned) < 2:
        return False
    if len(cleaned.split()) > 8:
        return False
    if re.fullmatch(r"[\W_]+", cleaned, flags=re.UNICODE):
        return False
    if cleaned.casefold() in {
        "settings",
        "view job",
        "apply",
        "about the company",
        "save job",
        "hvad",
        "skal",
        "som",
        "vil",
    }:
        return False
    if cleaned.endswith("?"):
        return False
    if _JOBINDEX_TEASER_FRAGMENT_RE.match(cleaned):
        return False
    if looks_like_jobindex_job_title(cleaned):
        return False
    if looks_like_jobindex_place(cleaned):
        return False
    return True


def peel_jobindex_dash_place(title: str) -> tuple[str, str]:
    """Split trailing `` - City`` from retail-style Jobindex titles."""
    title_clean = " ".join(str(title or "").split()).strip()
    if not title_clean:
        return "", ""
    match = re.search(
        r"^(?P<title>.+?)\s+-\s+(?P<place>[A-ZÆØÅ][A-Za-zÆØÅæøå.\-]*"
        r"(?:\s+[A-ZÆØÅ])?)$",
        title_clean,
    )
    if not match:
        return title_clean, ""
    peeled_title = (match.group("title") or "").strip()
    place = (match.group("place") or "").strip()
    if peeled_title and place:
        return peeled_title, place
    return title_clean, ""


def strip_title_overlap_from_place(title: str, place: str) -> str:
    """Drop a place prefix that is actually a trailing title fragment.

    Only strips lowercase-leading residue (e.g. ``til patientklager Aarhus N`` →
    ``Aarhus N``). Capitalized city tokens from ``merge_jobindex_place`` are kept.
    """
    title_clean = " ".join(str(title or "").split()).strip()
    place_clean = " ".join(str(place or "").split()).strip(" -|:")
    if not title_clean or not place_clean:
        return place_clean

    title_words = title_clean.split()
    for n in range(len(title_words), 0, -1):
        suffix = " ".join(title_words[-n:])
        if not place_clean.casefold().startswith(suffix.casefold() + " "):
            continue
        if not re.match(r"^[a-zæøå]", suffix):
            continue
        remainder = place_clean[len(suffix) :].strip(" -|:")
        if remainder:
            return remainder
    return place_clean


def is_jobindex_noise_fragment(value: str) -> bool:
    cleaned = " ".join(str(value or "").split()).strip()
    if not cleaned:
        return True
    if _JOBINDEX_NOISE_FRAGMENT_RE.search(cleaned):
        return True
    if re.search(r"(?i)\d+\s+min\b|published\s*:", cleaned):
        return True
    if cleaned.endswith("?"):
        return True
    if _JOBINDEX_TEASER_FRAGMENT_RE.match(cleaned):
        return True
    return looks_like_jobindex_place(cleaned)


def infer_jobindex_company_from_title(title: str) -> str:
    """Pull a leading company token from titles like ``YouSee søger …``."""
    title_clean = " ".join(str(title or "").split()).strip()
    if not title_clean:
        return ""
    match = _JOBINDEX_COMPANY_FROM_TITLE_RE.match(title_clean)
    if not match:
        return ""
    return (match.group(1) or "").strip()


def pick_jobindex_title(
    candidates: list[str],
    company: str = "",
    listing_place_hint: str = "",
) -> str:
    best = ""
    best_score: tuple[int, int, int, int] | None = None
    normalized_company = _normalize_jobindex_text(company)
    normalized_place_hint = _normalize_jobindex_text(listing_place_hint)
    for candidate in candidates:
        cleaned = " ".join(str(candidate or "").split()).strip()
        if not cleaned:
            continue

        normalized_candidate = _normalize_jobindex_text(cleaned)
        is_company_match = int(
            bool(normalized_company and normalized_candidate == normalized_company)
        )
        has_company_suffix = int(bool(_JOBINDEX_COMPANY_SUFFIX_RE.search(cleaned)))
        is_place_like = int(
            looks_like_jobindex_place(cleaned)
            or bool(
                normalized_place_hint and normalized_candidate == normalized_place_hint
            )
        )
        score = (is_company_match, has_company_suffix, is_place_like, -len(cleaned))
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


