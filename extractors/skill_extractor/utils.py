"""Text parsing and formatting helpers for skill extraction."""

import json
import re

from .normalization import _normalize_skill_name


def _split_skills_from_text(text: str) -> list[str]:
    compact = (text or "").replace("\n", ",")
    compact = re.sub(r"[;|/]+", ",", compact)
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    out = []
    seen = set()
    for part in parts:
        item = _normalize_skill_name(part)
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _format_skills(skills: list[str]) -> str:
    compact = []
    seen = set()
    for skill in skills:
        normalized = _normalize_skill_name(skill)
        key = normalized.lower()
        if normalized and key not in seen:
            seen.add(key)
            compact.append(normalized)
    return ", ".join(compact)


def _clean_model_output(text: str) -> str:
    out = text or ""
    out = out.replace("```", " ")
    out = re.sub(r"\bskills?\s*:\s*", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\boutput\s*:\s*", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\bplaintext\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s*[-*]\s*", ", ", out)
    out = re.sub(r"\s*\d+[.)]\s*", ", ", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _extract_json_object(text: str) -> dict:
    payload = (text or "").strip()
    if not payload:
        return {}
    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    candidate = payload[start : end + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def _to_items(value) -> list[dict]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        if isinstance(item, str):
            out.append({"name": item, "confidence": 1.0, "evidence": ""})
        elif isinstance(item, dict):
            out.append(item)
    return out


def _skill_to_regex(skill_name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(r"[A-Za-z0-9+#.]+", skill_name or "") if t]
    if not tokens:
        return ""
    return r"\b" + r"\s+".join(tokens) + r"\b"


def _profile_skill_pattern_fields(item) -> tuple[str, str]:
    """Read (name, pattern) from a profile known_skill_patterns entry (dict or Pydantic)."""
    if hasattr(item, "name"):
        name = str(getattr(item, "name", "")).strip()
        pattern = str(getattr(item, "pattern", "")).strip()
    elif isinstance(item, dict):
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
    else:
        return "", ""
    return name, pattern
