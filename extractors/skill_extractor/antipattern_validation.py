"""Synthetic job validation for antipattern sync."""

from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_top_skills_by_job_links
from spejder.db.utils import _normalize_skill_name_key
from spejder.llm import LocalLLM

from .antipattern_synthesis import ANTIPATTERN_PROMPT_INPUT_MAX
from .extraction_llm import _extract_job_skills_llm_path
from .normalization import _normalize_skill_name
from .utils import _extract_json_object

VALIDATION_RUNS = 3
GOOD_SKILL_LOSS_TOLERANCE = 1
SYNTHETIC_JOB_MAX_TOKENS = 1024
MATCH_MAX_TOKENS = 512


def _extracted_skill_keys(skills_text: str) -> set[str]:
    keys = set()
    for part in (skills_text or "").split(","):
        normalized = _normalize_skill_name(part.strip())
        if normalized:
            keys.add(normalized.lower())
    return keys


def _cap_prompt_list(items: list[str], limit: int) -> list[str]:
    if len(items) <= limit:
        return items
    return items[:limit]


def _chunk_list(items: list[str], chunk_size: int) -> list[list[str]]:
    if chunk_size <= 0:
        return [items] if items else []
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _top_position_skills(db_path: str, profile: AppConfig, limit: int) -> list[str]:
    """Return top DB skills by job link count, excluding blocked entries."""
    exclude_keys = {
        _normalize_skill_name_key(str(item))
        for item in (profile.blocked_skills or [])
        if _normalize_skill_name_key(str(item))
    }
    return get_top_skills_by_job_links(
        db_path,
        limit,
        exclude_keys=exclude_keys,
    )


def _matches_from_llm_output(text: str, blocked_by_key: dict[str, str]) -> list[str]:
    parsed = _extract_json_object(text)
    raw_matches = parsed.get("matches")
    if not isinstance(raw_matches, list):
        return []

    matches: list[str] = []
    seen: set[str] = set()
    for item in raw_matches:
        if isinstance(item, dict):
            raw = str(item.get("name", item.get("text", "")))
        else:
            raw = str(item)
        normalized = _normalize_skill_name(raw)
        key = normalized.lower()
        if not key or key in seen:
            continue
        if key not in blocked_by_key:
            continue
        seen.add(key)
        matches.append(blocked_by_key[key])
    return matches


def _match_blocked_skills_for_antipattern(
    llm: LocalLLM,
    rule: str,
    blocked_skills: list[str],
) -> list[str]:
    """Return blocked phrases the candidate rule should help filter."""
    normalized_rule = _normalize_skill_name(str(rule))
    if not normalized_rule or not blocked_skills:
        return []

    blocked_by_key = {
        _normalize_skill_name(item).lower(): item
        for item in blocked_skills
        if _normalize_skill_name(item)
    }
    if not blocked_by_key:
        return []

    matched: list[str] = []
    matched_keys: set[str] = set()
    for chunk in _chunk_list(blocked_skills, ANTIPATTERN_PROMPT_INPUT_MAX):
        phrases_text = "\n".join(f"- {item}" for item in chunk)
        prompt = (
            f'Given this antipattern rule: "{normalized_rule}"\n'
            "Which of the following rejected non-skill phrases from job ads would this rule "
            "help an extractor ignore?\n"
            "Only include phrases that clearly match the rule.\n"
            'Output strict JSON only: {"matches": ["phrase one", "phrase two"]}\n\n'
            f"Rejected phrases:\n{phrases_text}\n\n"
            "JSON:"
        )
        out = llm.generate(prompt, max_tokens=MATCH_MAX_TOKENS)
        chunk_matches = _matches_from_llm_output(out, blocked_by_key)
        if not chunk_matches and (out or "").strip():
            preview = (out or "").strip()
            if len(preview) > 400:
                preview = preview[:400] + "..."
            print(f"Antipattern sync: match parse empty, llm_output={preview!r}")
        for item in chunk_matches:
            key = _normalize_skill_name(item).lower()
            if not key or key in matched_keys:
                continue
            matched_keys.add(key)
            matched.append(item)
    return matched


def _generate_synthetic_job_posting(
    llm: LocalLLM,
    blocked_skills: list[str],
    good_skills: list[str],
) -> tuple[str, bool, bool]:
    blocked_truncated = len(blocked_skills) > ANTIPATTERN_PROMPT_INPUT_MAX
    good_skills_truncated = len(good_skills) > ANTIPATTERN_PROMPT_INPUT_MAX
    blocked = _cap_prompt_list(blocked_skills, ANTIPATTERN_PROMPT_INPUT_MAX)
    good = _cap_prompt_list(good_skills, ANTIPATTERN_PROMPT_INPUT_MAX)
    blocked_text = "\n".join(f"- {item}" for item in blocked) or "- (none)"
    good_text = "\n".join(f"- {item}" for item in good) or "- (none)"
    prompt = (
        "Write a realistic software/engineering job posting (300-600 words).\n"
        "Requirements:\n"
        "1) Naturally weave in ALL of the following non-skill narrative phrases "
        "(hiring fluff, sentence fragments — not as skill bullets):\n"
        f"{blocked_text}\n\n"
        "2) Clearly require these legitimate technical/professional skills:\n"
        f"{good_text}\n\n"
        "3) Use plain text only, no JSON, no markdown headers.\n\n"
        "Job posting:"
    )
    out = llm.generate(prompt, max_tokens=SYNTHETIC_JOB_MAX_TOKENS)
    text = " ".join((out or "").split()).strip()
    return text, blocked_truncated, good_skills_truncated


def _stable_extracted_keys(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    job_text: str,
    antipatterns_override: Optional[list[str]],
    runs: int = VALIDATION_RUNS,
) -> set[str]:
    if runs <= 0 or not job_text.strip():
        return set()

    run_sets: list[set[str]] = []
    for _ in range(runs):
        extracted = _extract_job_skills_llm_path(
            db_path,
            job_text,
            llm=llm,
            profile=profile,
            skip_blocked_filter=True,
            antipatterns_override=antipatterns_override,
        )
        if extracted is None:
            continue
        keys = _extracted_skill_keys(extracted)
        if keys:
            run_sets.append(keys)

    if not run_sets:
        return set()

    stable = set(run_sets[0])
    for keys in run_sets[1:]:
        stable &= keys
    return stable


def _validate_antipattern_candidate(
    db_path: str,
    profile: AppConfig,
    llm: LocalLLM,
    job_text: str,
    candidate_rule: str,
    existing_antipatterns: list[str],
    matched_blocked: list[str],
    good_skill_keys: set[str],
    validation_runs: int = VALIDATION_RUNS,
) -> dict:
    normalized_rule = _normalize_skill_name(str(candidate_rule))
    result = {
        "rule": normalized_rule,
        "accepted": False,
        "skip_reason": "",
        "matched_blocked": [],
        "baseline_blocked": [],
        "with_blocked": [],
        "pruned_blocked": [],
    }
    if not normalized_rule or not job_text.strip():
        result["skip_reason"] = "empty_rule_or_job"
        return result

    matched_keys = {
        _normalize_skill_name(str(item)).lower()
        for item in matched_blocked
        if _normalize_skill_name(str(item))
    }
    result["matched_blocked"] = sorted(matched_keys)
    if not matched_keys:
        result["skip_reason"] = "no_matched_blocked"
        return result

    baseline_keys = _stable_extracted_keys(
        db_path,
        profile,
        llm,
        job_text,
        antipatterns_override=list(existing_antipatterns),
        runs=validation_runs,
    )
    if not baseline_keys:
        result["skip_reason"] = "baseline_empty"
        return result

    missing_blocked = matched_keys - baseline_keys
    if missing_blocked:
        result["skip_reason"] = "baseline_missing_blocked"
        result["baseline_blocked"] = sorted(matched_keys & baseline_keys)
        return result

    with_keys = _stable_extracted_keys(
        db_path,
        profile,
        llm,
        job_text,
        antipatterns_override=list(existing_antipatterns) + [normalized_rule],
        runs=validation_runs,
    )
    if not with_keys and baseline_keys:
        result["skip_reason"] = "with_pattern_empty"
        return result

    baseline_blocked = sorted(matched_keys)
    with_blocked = sorted(k for k in with_keys if k in matched_keys)
    result["baseline_blocked"] = baseline_blocked
    result["with_blocked"] = with_blocked

    if len(with_blocked) >= len(baseline_blocked):
        result["skip_reason"] = "no_blocked_reduction"
        return result

    baseline_good_count = len(baseline_keys & good_skill_keys)
    with_good_count = len(with_keys & good_skill_keys)
    if with_good_count < baseline_good_count - GOOD_SKILL_LOSS_TOLERANCE:
        result["skip_reason"] = "good_skills_lost"
        return result

    pruned = sorted(set(baseline_blocked) - set(with_blocked))
    if not pruned:
        result["skip_reason"] = "no_prunable_blocked"
        return result

    result["accepted"] = True
    result["pruned_blocked"] = pruned
    return result
