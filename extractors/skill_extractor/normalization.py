"""Skill name normalization."""

import re


def _normalize_skill_name(skill: str) -> str:
    s = (skill or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"^[-*\d.)\s]+", "", s)
    s = re.sub(r"^(?:a|an|as|at|you|but)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^you\s+(?:will|can|have|are|should|must)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(?:good|great|strong|solid|excellent|proven|quality|high\s+quality)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:degree|bachelor(?:'s)?|master(?:'s)?|phd|doctorate)"
        r"\s+in\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:experience\s+with|experienced\s+with|hands-?on\s+with|"
        r"knowledge\s+of|familiarity\s+with)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(
        r"\b(?:required|required:|requirements?|qualifications?|must have|nice to have)\b",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip(" ,.;:-")
    if not s:
        return ""
    if len(s.split()) > 5:
        return ""
    if len(s) < 2:
        return ""
    return s
