import re

_EASY_APPLY_REGEX = re.compile(r"(?i)easy\s*apply|apply\s*with\s*linkedin")


def _is_linkedin_item(source: str, position_link: str) -> bool:
    source_low = (source or "").strip().lower()
    link_low = (position_link or "").strip().lower()
    return source_low == "linkedin" or "linkedin.com/" in link_low


def _has_easy_apply_signal(*parts: str) -> bool:
    compact = " ".join(" ".join((part or "").split()) for part in parts if part)
    return bool(compact and _EASY_APPLY_REGEX.search(compact))


def _is_easy_apply_item(item: dict) -> bool:
    source = str(item.get("source", ""))
    position_link = str(item.get("position_link", ""))
    if not _is_linkedin_item(source, position_link):
        return False
    reason = str(item.get("relevance_reason", ""))
    if re.search(r"\beasy_apply\s*=\s*true\b", reason, flags=re.IGNORECASE):
        return True
    return _has_easy_apply_signal(
        str(item.get("title", "")),
        str(item.get("summary", "")),
        str(item.get("description", "")),
        str(item.get("raw_text", "")),
    )
