"""LLM synthesis and profile merge helpers for antipattern sync."""

from spejder.config import AppConfig
from spejder.llm import LocalLLM

from .normalization import _normalize_skill_name
from .utils import _extract_json_object, _to_items

SYNTHESIS_PATTERN_COUNT = 3
SYNTHESIS_BLOCKED_INPUT_MAX = 150
SYNTHESIS_MAX_TOKENS = 512
PROFILE_ANTIPATTERNS_MAX = 200

# Shared cap for synthesis prompts and synthetic job generation.
ANTIPATTERN_PROMPT_INPUT_MAX = SYNTHESIS_BLOCKED_INPUT_MAX


def _antipattern_keys(profile: AppConfig) -> set[str]:
    keys = set()
    for item in profile.skill_extraction_antipatterns or []:
        normalized = _normalize_skill_name(str(item))
        if normalized:
            keys.add(normalized.lower())
    return keys


def _blocked_skills_for_synthesis(profile: AppConfig) -> tuple[list[str], bool]:
    """Return deduped normalized blocked skills; truncated flag if list was capped."""
    existing = _antipattern_keys(profile)
    skills: list[str] = []
    seen = set()
    for item in profile.blocked_skills or []:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not normalized or key in seen or key in existing:
            continue
        seen.add(key)
        skills.append(normalized)
    truncated = len(skills) > SYNTHESIS_BLOCKED_INPUT_MAX
    if truncated:
        skills = skills[:SYNTHESIS_BLOCKED_INPUT_MAX]
    return skills, truncated


def _synthesize_antipatterns_via_llm(
    llm: LocalLLM,
    blocked_skills: list[str],
    pattern_count: int = SYNTHESIS_PATTERN_COUNT,
) -> list[str]:
    if not blocked_skills:
        return []

    phrases_text = "\n".join(f"- {item}" for item in blocked_skills)
    prompt = (
        "Given these rejected non-skill phrases extracted from job ads, produce exactly "
        f"{pattern_count} short generic antipattern rules that would help an extractor "
        "ignore similar phrasing in the future.\n"
        "Categories: hiring narrative, pronoun-led fragments, company fluff, malformed text.\n"
        "Do NOT include legitimate tools, languages, frameworks, or domain skills.\n"
        f"Output strict JSON with key rules as an array of exactly {pattern_count} strings.\n\n"
        f"Rejected phrases:\n{phrases_text}\n\n"
        "JSON:"
    )
    out = llm.generate(prompt, max_tokens=SYNTHESIS_MAX_TOKENS)
    parsed = _extract_json_object(out)

    merged: list[str] = []
    seen = set()
    for item in _to_items(parsed.get("rules")):
        if isinstance(item, str):
            text = _normalize_skill_name(item)
        elif isinstance(item, dict):
            text = _normalize_skill_name(str(item.get("name", item.get("text", ""))))
        else:
            text = _normalize_skill_name(str(item))
        item_key = text.lower()
        if not text or item_key in seen:
            continue
        seen.add(item_key)
        merged.append(text)
        if len(merged) >= pattern_count:
            break
    return merged


def _merge_antipatterns(profile: AppConfig, new_items: list[str]) -> list[str]:
    if not new_items:
        return []

    existing = list(profile.skill_extraction_antipatterns or [])
    seen = {
        _normalize_skill_name(str(item)).lower()
        for item in existing
        if _normalize_skill_name(str(item))
    }
    added = []
    for item in new_items:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        existing.append(normalized)
        added.append(normalized)

    if len(existing) > PROFILE_ANTIPATTERNS_MAX:
        existing = existing[-PROFILE_ANTIPATTERNS_MAX:]

    profile.skill_extraction_antipatterns = existing
    return added


def _remove_from_blocked_skills(profile: AppConfig, skill_name: str) -> bool:
    target = _normalize_skill_name(skill_name).lower()
    if not target:
        return False
    blocked = profile.blocked_skills or []
    kept = []
    removed = False
    seen = set()
    for item in blocked:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not key or key in seen:
            continue
        seen.add(key)
        if key == target:
            removed = True
            continue
        kept.append(normalized)
    profile.blocked_skills = kept
    return removed
