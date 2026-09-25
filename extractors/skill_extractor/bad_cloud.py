"""Deterministic bigram/unigram toxicity scoring for skill extraction."""

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from spejder.config import AppConfig, SKILL_BAD_NGRAM_WEIGHT_CAP_DEFAULT
from spejder.db import (
    count_bad_ngrams,
    decrement_bad_ngram_counts,
    ensure_db,
    get_bad_ngram_weights,
    get_skill_patterns,
    summarize_bad_ngrams,
    upsert_bad_ngram_counts,
)

from .normalization import _normalize_skill_name

THRESHOLD_FLOOR = 0.1
DEFAULT_THRESHOLD_MARGIN = 0.5
MATURE_GOOD_SKILL_MIN_AGE_DAYS = 1
DEFAULT_NGRAM_WEIGHT_CAP = SKILL_BAD_NGRAM_WEIGHT_CAP_DEFAULT


def _tokenize_for_cloud(skill: str) -> list[str]:
    normalized = _normalize_skill_name(skill)
    if not normalized:
        return []
    return re.findall(r"[a-z0-9+#.]+", normalized)


def _ngrams_for_cloud(tokens: list[str]) -> list[tuple[str, int]]:
    if not tokens:
        return []
    if len(tokens) == 1:
        return [(tokens[0], 1)]
    return [(f"{tokens[index]} {tokens[index + 1]}", 2) for index in range(len(tokens) - 1)]


def _ngrams_for_skill(skill: str) -> list[tuple[str, int]]:
    return _ngrams_for_cloud(_tokenize_for_cloud(skill))


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1.0 - weight) + ordered[high] * weight


def _score_from_weights(
    ngrams: list[tuple[str, int]],
    weights: dict[tuple[str, int], int],
) -> float:
    if not ngrams:
        return 0.0
    total = sum(weights.get(item, 0) for item in ngrams)
    return total / len(ngrams)


def _unique_ngrams(ngrams: list[tuple[str, int]]) -> list[tuple[str, int]]:
    return list(dict.fromkeys(ngrams))


def _ngrams_by_skill_keys(skills: list[str]) -> dict[str, list[tuple[str, int]]]:
    by_key: dict[str, list[tuple[str, int]]] = {}
    for skill in skills:
        normalized = _normalize_skill_name(str(skill))
        if not normalized:
            continue
        by_key[normalized.lower()] = _ngrams_for_skill(normalized)
    return by_key


def toxicity_scores_by_key(
    skills: list[str],
    db_path: str,
) -> dict[str, float]:
    by_key = _ngrams_by_skill_keys(skills)
    if not by_key:
        return {}
    all_ngrams = _unique_ngrams(
        [ngram for ngrams in by_key.values() for ngram in ngrams]
    )
    weights = get_bad_ngram_weights(db_path, all_ngrams) if all_ngrams else {}
    return {
        key: _score_from_weights(ngrams, weights)
        for key, ngrams in by_key.items()
    }


def toxicity_score(skill: str, db_path: str) -> float:
    normalized = _normalize_skill_name(skill)
    if not normalized:
        return 0.0
    scores = toxicity_scores_by_key([normalized], db_path)
    return scores.get(normalized.lower(), 0.0)


def ingest_blocked_skill(
    skill: str,
    db_path: str,
    max_weight: Optional[int] = None,
) -> int:
    return ingest_blocked_skills([skill], db_path, max_weight=max_weight)


def ingest_blocked_skills(
    skills: list[str],
    db_path: str,
    max_weight: Optional[int] = None,
) -> int:
    counts: dict[tuple[str, int], int] = {}
    for skill in skills or []:
        for ngram in _ngrams_for_skill(str(skill)):
            counts[ngram] = counts.get(ngram, 0) + 1
    if not counts:
        return 0
    return upsert_bad_ngram_counts(db_path, counts, max_weight=max_weight)


def _weight_cap(profile: Optional[AppConfig]) -> Optional[int]:
    """Return per-ngram cap, or None when uncapped (cap <= 0)."""
    if profile is None:
        return DEFAULT_NGRAM_WEIGHT_CAP
    raw = getattr(profile, "skill_bad_ngram_weight_cap", DEFAULT_NGRAM_WEIGHT_CAP)
    try:
        cap = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_NGRAM_WEIGHT_CAP
    if cap <= 0:
        return None
    return cap


def _threshold_margin(profile: AppConfig) -> float:
    margin = float(getattr(profile, "skill_bigram_threshold_margin", DEFAULT_THRESHOLD_MARGIN) or 0)
    if margin <= 0:
        return DEFAULT_THRESHOLD_MARGIN
    return margin


def _blocked_keys(profile: AppConfig) -> set[str]:
    return {
        _normalize_skill_name(str(item)).lower()
        for item in profile.blocked_skills or []
        if _normalize_skill_name(str(item))
    }


def _parse_iso_timestamp(raw: str) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _eligible_good_skill_rows(
    db_path: str,
    profile: AppConfig,
    *,
    require_mature: bool,
    min_age_days: int = MATURE_GOOD_SKILL_MIN_AGE_DAYS,
) -> list[str]:
    blocked = _blocked_keys(profile)
    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(min_age_days)))
    names: list[str] = []
    seen: set[str] = set()
    for row in get_skill_patterns(db_path, enabled_only=True):
        name = _normalize_skill_name(str(row.get("name", "")))
        key = name.lower()
        if not name or key in blocked or key in seen:
            continue
        source = str(row.get("source", "") or "").strip().lower()
        occurrences = int(row.get("occurrences", 0) or 0)
        if source == "detected" and occurrences < 1:
            continue
        if require_mature:
            created = _parse_iso_timestamp(str(row.get("created_at", "") or ""))
            if created is None or created > cutoff:
                continue
        seen.add(key)
        names.append(name)
    return names


def _mature_good_skill_names(
    db_path: str,
    profile: AppConfig,
    min_age_days: int = MATURE_GOOD_SKILL_MIN_AGE_DAYS,
) -> list[str]:
    mature = _eligible_good_skill_rows(
        db_path, profile, require_mature=True, min_age_days=min_age_days
    )
    if mature:
        return mature
    return _eligible_good_skill_rows(
        db_path, profile, require_mature=False, min_age_days=min_age_days
    )


def calibrate_threshold(db_path: str, profile: AppConfig) -> float:
    if count_bad_ngrams(db_path) == 0:
        return float("inf")

    good_names = _mature_good_skill_names(db_path, profile)
    good_score_map = toxicity_scores_by_key(good_names, db_path)
    good_scores = list(good_score_map.values())

    bad_names = [
        _normalize_skill_name(str(item))
        for item in profile.blocked_skills or []
    ]
    bad_names = [name for name in bad_names if name]
    bad_score_map = toxicity_scores_by_key(bad_names, db_path)
    bad_scores = list(bad_score_map.values())

    p95_good = _percentile(good_scores, 95.0)
    p50_bad = _percentile(bad_scores, 50.0)
    margin = _threshold_margin(profile)
    threshold = max(THRESHOLD_FLOOR, p95_good + margin * max(0.0, p50_bad - p95_good))

    if bad_scores and good_scores:
        return threshold
    if bad_scores:
        return max(THRESHOLD_FLOOR, _percentile(bad_scores, 25.0))
    return max(THRESHOLD_FLOOR, p95_good + margin)


def recalibrate_and_store_threshold(profile: AppConfig, db_path: str) -> float:
    """Compute and persist the toxicity threshold (single write path for sync)."""
    ensure_db(db_path)
    new_threshold = calibrate_threshold(db_path, profile)
    if new_threshold == float("inf"):
        profile.skill_bigram_toxicity_threshold = None
        return new_threshold
    profile.skill_bigram_toxicity_threshold = float(new_threshold)
    return float(new_threshold)


def resolve_toxicity_threshold(db_path: str, profile: Optional[AppConfig]) -> float:
    """Read stored threshold; compute once for cold start without writing profile."""
    if not profile:
        return float("inf")
    explicit = getattr(profile, "skill_bigram_toxicity_threshold", None)
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            pass
    return calibrate_threshold(db_path, profile)


def prune_blocked_skills_by_cloud(
    profile: AppConfig,
    db_path: str,
    threshold: Optional[float] = None,
    protect_keys: Optional[set[str]] = None,
) -> list[str]:
    if count_bad_ngrams(db_path) == 0:
        return []
    cutoff = threshold if threshold is not None else resolve_toxicity_threshold(db_path, profile)
    if cutoff == float("inf"):
        return []

    protected = {key.lower() for key in (protect_keys or set())}
    blocked_names = [
        _normalize_skill_name(str(item))
        for item in profile.blocked_skills or []
    ]
    score_map = toxicity_scores_by_key(
        [name for name in blocked_names if name],
        db_path,
    )
    kept: list[str] = []
    pruned: list[str] = []
    seen: set[str] = set()
    for item in profile.blocked_skills or []:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        if key in protected:
            kept.append(normalized)
            continue
        if score_map.get(key, 0.0) >= cutoff:
            pruned.append(normalized)
            continue
        kept.append(normalized)
    profile.blocked_skills = kept
    return pruned


def seed_bad_cloud_from_blocked_skills(
    db_path: str,
    blocked_skills: list[str],
    max_weight: Optional[int] = None,
) -> int:
    return ingest_blocked_skills(blocked_skills, db_path, max_weight=max_weight)


def ensure_bad_cloud_initialized(profile: AppConfig, db_path: str) -> dict:
    """One-time seed from blocked_skills and prune redundant blocked entries.

    Threshold calibration is owned by shared skill hygiene
    (``run_skill_hygiene_stages``) via recalibrate_and_store_threshold.
    """
    ensure_db(db_path)
    stats = {"seeded": False, "ngram_keys_upserted": 0, "pruned": []}
    if getattr(profile, "bad_cloud_seeded", False):
        return stats

    blocked = list(profile.blocked_skills or [])
    seed_protect_keys = {
        _normalize_skill_name(str(item)).lower()
        for item in blocked
        if _normalize_skill_name(str(item))
    }
    if blocked:
        stats["ngram_keys_upserted"] = seed_bad_cloud_from_blocked_skills(
            db_path,
            blocked,
            max_weight=_weight_cap(profile),
        )
    profile.bad_cloud_seeded = True
    stats["seeded"] = True

    stats["pruned"] = prune_blocked_skills_by_cloud(
        profile,
        db_path,
        protect_keys=seed_protect_keys,
    )
    return stats


def bad_cloud_status(db_path: str, profile: Optional[AppConfig]) -> dict:
    """Operator-facing cloud summary for the Skills tab."""
    ensure_db(db_path)
    summary = summarize_bad_ngrams(db_path)
    threshold = None
    blocked: list[str] = []
    if profile is not None:
        explicit = getattr(profile, "skill_bigram_toxicity_threshold", None)
        if explicit is not None:
            try:
                threshold = float(explicit)
            except (TypeError, ValueError):
                threshold = None
        seen: set[str] = set()
        for item in profile.blocked_skills or []:
            normalized = _normalize_skill_name(str(item))
            key = normalized.lower()
            if not normalized or key in seen:
                continue
            seen.add(key)
            blocked.append(normalized)
    return {
        "ngram_count": int(summary.get("ngram_count", 0) or 0),
        "total_weight": int(summary.get("total_weight", 0) or 0),
        "max_weight": int(summary.get("max_weight", 0) or 0),
        "weight_cap": _weight_cap(profile),
        "threshold": threshold,
        "blocked_count": len(blocked),
        "blocked_skills": blocked,
    }


def on_skills_forgiven(
    profile: AppConfig,
    db_path: str,
    skills: list[str],
) -> dict:
    """Decrement cloud weights for skills and drop them from blocked_skills.

    Softens ghost toxicity after mistaken or overly broad blocks. Does not
    restore deleted skill_patterns / job_skills rows.
    """
    ensure_db(db_path)
    counts: dict[tuple[str, int], int] = {}
    forgiven: list[str] = []
    seen: set[str] = set()
    for skill in skills or []:
        normalized = _normalize_skill_name(str(skill))
        key = normalized.lower()
        if not normalized or key in seen:
            continue
        seen.add(key)
        forgiven.append(normalized)
        for ngram in _ngrams_for_skill(normalized):
            counts[ngram] = counts.get(ngram, 0) + 1

    decremented = decrement_bad_ngram_counts(db_path, counts) if counts else 0

    kept: list[str] = []
    kept_keys: set[str] = set()
    removed_from_blocked = 0
    for item in profile.blocked_skills or []:
        normalized = _normalize_skill_name(str(item))
        key = normalized.lower()
        if not normalized or key in kept_keys:
            continue
        if key in seen:
            removed_from_blocked += 1
            continue
        kept_keys.add(key)
        kept.append(normalized)
    profile.blocked_skills = kept

    return {
        "skills": forgiven,
        "ngram_keys_decremented": decremented,
        "removed_from_blocked": removed_from_blocked,
        "blocked_remaining": len(profile.blocked_skills or []),
    }


def on_skills_blocked(
    profile: AppConfig,
    db_path: str,
    skills: list[str],
) -> dict:
    """Ingest blocked skills into the cloud and prune covered blocked entries.

    Recalibrates the threshold only when ``skill_recalibrate_on_block`` is true
    (optional lag-shortening path; sync hygiene remains the default writer).
    """
    ensure_db(db_path)
    ingested = ingest_blocked_skills(
        skills,
        db_path,
        max_weight=_weight_cap(profile),
    )

    threshold = resolve_toxicity_threshold(db_path, profile)
    protect_keys = {
        _normalize_skill_name(str(skill)).lower()
        for skill in skills or []
        if _normalize_skill_name(str(skill))
    }
    pruned = prune_blocked_skills_by_cloud(
        profile,
        db_path,
        threshold,
        protect_keys=protect_keys,
    )
    threshold_changed = False
    if bool(getattr(profile, "skill_recalibrate_on_block", False)):
        previous = getattr(profile, "skill_bigram_toxicity_threshold", None)
        threshold = recalibrate_and_store_threshold(profile, db_path)
        threshold_changed = previous != profile.skill_bigram_toxicity_threshold
        # Re-evaluate other blocked entries against the fresh cutoff.
        pruned = prune_blocked_skills_by_cloud(
            profile,
            db_path,
            None if threshold == float("inf") else threshold,
            protect_keys=protect_keys,
        )
    return {
        "ngrams_ingested": ingested,
        "ngram_keys_upserted": ingested,
        "threshold": threshold,
        "threshold_changed": threshold_changed,
        "pruned": pruned,
    }
