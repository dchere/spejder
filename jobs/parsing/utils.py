import re

_TITLE_TRAILING_I_PLACE_RE = re.compile(
    r"\s+i\s+([A-ZÆØÅa-zæøå][A-Za-zÆØÅæøå.\-]*(?:\s*\([^)]+\))?)\s*$"
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


def first_non_empty(lines: list[str]) -> str:
    for line in lines:
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


