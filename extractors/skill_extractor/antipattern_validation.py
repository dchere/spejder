"""Synthetic job validation for antipattern sync."""

from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_skill_patterns
from spejder.llm import LocalLLM

from .antipattern_synthesis import ANTIPATTERN_PROMPT_INPUT_MAX
from .extraction_llm import _extract_job_skills_llm_path
from .normalization import _normalize_skill_name

VALIDATION_RUNS = 3
SEEN_SKILL_LOSS_TOLERANCE = 1
SYNTHETIC_JOB_MAX_TOKENS = 1024


def _extracted_skill_keys(skills_text: str) -> set[str]:
    keys = set()
    for part in (skills_text or "").split(","):
        normalized = _normalize_skill_name(part.strip())
        if normalized:
            keys.add(normalized.lower())
    return keys


def _skills_seen_at_least_once(db_path: str) -> list[str]:
    skills: list[str] = []
    seen = set()
    for row in get_skill_patterns(db_path, enabled_only=False):
        if int(row.get("occurrences", 0) or 0) < 1:
            continue
        name = _normalize_skill_name(str(row.get("name", "")))
        key = name.lower()
        if not name or key in seen:
            continue
        seen.add(key)
        skills.append(name)
    return skills


def _cap_prompt_list(items: list[str], limit: int) -> list[str]:
    if len(items) <= limit:
        return items
    return items[:limit]


def _generate_synthetic_job_posting(
    llm: LocalLLM,
    blocked_skills: list[str],
    seen_skills: list[str],
) -> tuple[str, bool, bool]:
    blocked_truncated = len(blocked_skills) > ANTIPATTERN_PROMPT_INPUT_MAX
    seen_truncated = len(seen_skills) > ANTIPATTERN_PROMPT_INPUT_MAX
    blocked = _cap_prompt_list(blocked_skills, ANTIPATTERN_PROMPT_INPUT_MAX)
    seen = _cap_prompt_list(seen_skills, ANTIPATTERN_PROMPT_INPUT_MAX)
    blocked_text = "\n".join(f"- {item}" for item in blocked) or "- (none)"
    seen_text = "\n".join(f"- {item}" for item in seen) or "- (none)"
    prompt = (
        "Write a realistic software/engineering job posting (300-600 words).\n"
        "Requirements:\n"
        "1) Naturally weave in ALL of the following non-skill narrative phrases "
        "(hiring fluff, sentence fragments — not as skill bullets):\n"
        f"{blocked_text}\n\n"
        "2) Clearly require these legitimate technical/professional skills:\n"
        f"{seen_text}\n\n"
        "3) Use plain text only, no JSON, no markdown headers.\n\n"
        "Job posting:"
    )
    out = llm.generate(prompt, max_tokens=SYNTHETIC_JOB_MAX_TOKENS)
    text = " ".join((out or "").split()).strip()
    return text, blocked_truncated, seen_truncated


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
    blocked_keys: set[str],
    seen_keys: set[str],
    baseline_keys: Optional[set[str]] = None,
    validation_runs: int = VALIDATION_RUNS,
) -> dict:
    normalized_rule = _normalize_skill_name(str(candidate_rule))
    result = {
        "rule": normalized_rule,
        "accepted": False,
        "skip_reason": "",
        "baseline_blocked": [],
        "with_blocked": [],
        "pruned_blocked": [],
    }
    if not normalized_rule or not job_text.strip():
        result["skip_reason"] = "empty_rule_or_job"
        return result

    if baseline_keys is None:
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

    baseline_blocked = sorted(k for k in baseline_keys if k in blocked_keys)
    with_blocked = sorted(k for k in with_keys if k in blocked_keys)
    result["baseline_blocked"] = baseline_blocked
    result["with_blocked"] = with_blocked

    if len(with_blocked) >= len(baseline_blocked):
        result["skip_reason"] = "no_blocked_reduction"
        return result

    baseline_seen_count = len(baseline_keys & seen_keys)
    with_seen_count = len(with_keys & seen_keys)
    if with_seen_count < baseline_seen_count - SEEN_SKILL_LOSS_TOLERANCE:
        result["skip_reason"] = "seen_skills_lost"
        return result

    pruned = sorted(set(baseline_blocked) - set(with_blocked))
    if not pruned:
        result["skip_reason"] = "no_prunable_blocked"
        return result

    result["accepted"] = True
    result["pruned_blocked"] = pruned
    return result
