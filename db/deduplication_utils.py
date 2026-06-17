import re
from difflib import SequenceMatcher

from .utils import sanitize_job_title

RAW_TEXT_MAX_CHARS = 120000
DEDUPE_SNIPPET_MARKER = "[DEDUPE_SNIPPET]"
DEFAULT_RAW_TEXT_SIMILARITY = 0.85

COMPANY_NOISE_TOKENS = {
    "danmark",
    "denmark",
    "aps",
    "a",
    "s",
    "as",
    "ab",
    "oy",
    "ltd",
    "llc",
    "inc",
    "group",
    "holding",
}

_GENDER_MARKER_RE = re.compile(
    r"\(\s*"
    r"(?:"
    r"m\s*/\s*f\s*/\s*d"
    r"|m\s*/\s*f\s*/\s*x"
    r"|w\s*/\s*m\s*/\s*d"
    r"|mfd"
    r"|wmd"
    r"|all\s+genders?"
    r")"
    r"\s*\)",
    flags=re.IGNORECASE,
)

_TITLE_ABBREVIATION_REPLACEMENTS = (
    (re.compile(r"\bsw\b", re.IGNORECASE), "software"),
    (re.compile(r"\bsr\.?\b", re.IGNORECASE), "senior"),
)


def _canonicalize_title_for_dedupe(title: str) -> str:
    text = sanitize_job_title(title)
    text = _GENDER_MARKER_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip(" -–—|,;")
    for pattern, replacement in _TITLE_ABBREVIATION_REPLACEMENTS:
        text = pattern.sub(replacement, text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _normalize_company_key(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", (value or "").lower())
    kept = [token for token in tokens if token and token not in COMPANY_NOISE_TOKENS]
    if not kept:
        kept = tokens
    return "".join(kept)


def _position_dedupe_key(company: str, title: str) -> str:
    company_key = _normalize_company_key(company)
    title_key = _normalize_title_key(_canonicalize_title_for_dedupe(title))
    if not company_key or not title_key:
        return ""
    return f"{company_key}|{title_key}"


def _cross_source_dedupe_key(source: str, company: str, title: str) -> str:
    """Backward-compatible alias; source is ignored."""
    del source
    return _position_dedupe_key(company, title)


def _normalize_raw_text_for_compare(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _merge_raw_text(
    keeper_raw: str,
    duplicate_raw: str,
    *,
    similarity_threshold: float = DEFAULT_RAW_TEXT_SIMILARITY,
    max_total_chars: int = RAW_TEXT_MAX_CHARS,
) -> str:
    keeper = (keeper_raw or "").strip()
    duplicate = (duplicate_raw or "").strip()
    if not duplicate:
        return keeper[:max_total_chars]
    if not keeper:
        return duplicate[:max_total_chars]

    keeper_norm = _normalize_raw_text_for_compare(keeper)
    duplicate_norm = _normalize_raw_text_for_compare(duplicate)
    if duplicate_norm in keeper_norm or keeper_norm in duplicate_norm:
        return (keeper if len(keeper) >= len(duplicate) else duplicate)[:max_total_chars]

    ratio = SequenceMatcher(None, keeper_norm, duplicate_norm).ratio()
    if ratio >= similarity_threshold:
        return (keeper if len(keeper) >= len(duplicate) else duplicate)[:max_total_chars]

    if DEDUPE_SNIPPET_MARKER in keeper and duplicate in keeper:
        return keeper[:max_total_chars]

    merged = f"{keeper}\n\n{DEDUPE_SNIPPET_MARKER}\n{duplicate}".strip()
    return merged[:max_total_chars]


def _merge_duplicate_into_keeper(keeper: dict, duplicate: dict) -> None:
    if not keeper.get("company") and duplicate.get("company"):
        keeper["company"] = duplicate["company"]
    if not keeper.get("title") and duplicate.get("title"):
        keeper["title"] = duplicate["title"]

    keeper_place = (keeper.get("place") or "").strip()
    duplicate_place = (duplicate.get("place") or "").strip()
    if (not keeper_place or keeper_place.lower() == "unknown") and duplicate_place:
        keeper["place"] = duplicate_place

    keeper_work_type = (keeper.get("work_type") or "").strip()
    duplicate_work_type = (duplicate.get("work_type") or "").strip()
    if (
        not keeper_work_type or keeper_work_type.lower() == "unknown"
    ) and duplicate_work_type:
        keeper["work_type"] = duplicate_work_type

    keeper["viewed"] = max(int(keeper.get("viewed") or 0), int(duplicate.get("viewed") or 0))
    keeper["applied"] = max(int(keeper.get("applied") or 0), int(duplicate.get("applied") or 0))
    keeper["raw_text"] = _merge_raw_text(
        str(keeper.get("raw_text") or ""),
        str(duplicate.get("raw_text") or ""),
    )


def _keeper_sort_key(item: dict) -> tuple[str, int]:
    created_at = str(item.get("created_at") or "")
    return (created_at, int(item.get("id") or 0))
