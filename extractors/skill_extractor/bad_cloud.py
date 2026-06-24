"""Deterministic bigram/unigram toxicity scoring for skill extraction."""

import re
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    count_bad_ngrams,
    ensure_db,
    get_bad_ngram_weights,
    get_skill_patterns,
    upsert_bad_ngram_counts,
)

from .normalization import _normalize_skill_name

THRESHOLD_FLOOR = 0.1
DEFAULT_THRESHOLD_MARGIN = 0.5


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


def ingest_blocked_skill(skill: str, db_path: str) -> int:
    return ingest_blocked_skills([skill], db_path)


def ingest_blocked_skills(skills: list[str], db_path: str) -> int:
    counts: dict[tuple[str, int], int] = {}
    for skill in skills or []:
        for ngram in _ngrams_for_skill(str(skill)):
            counts[ngram] = counts.get(ngram, 0) + 1
    if not counts:
        return 0
    return upsert_bad_ngram_counts(db_path, counts)


def _threshold_margin(profile: AppConfig) -> float:
    margin = float(getattr(profile, "skill_bigram_threshold_margin", DEFAULT_THRESHOLD_MARGIN) or 0)
    if margin <= 0:
        return DEFAULT_THRESHOLD_MARGIN
    return margin


def calibrate_threshold(db_path: str, profile: AppConfig) -> float:
    if count_bad_ngrams(db_path) == 0:
        return float("inf")

    good_scores: list[float] = []
    good_names = [
        _normalize_skill_name(str(row.get("name", "")))
        for row in get_skill_patterns(db_path, enabled_only=True)
    ]
    good_names = [name for name in good_names if name]
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


def resolve_toxicity_threshold(db_path: str, profile: Optional[AppConfig]) -> float:
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


def seed_bad_cloud_from_blocked_skills(db_path: str, blocked_skills: list[str]) -> int:
    return ingest_blocked_skills(blocked_skills, db_path)


def ensure_bad_cloud_initialized(profile: AppConfig, db_path: str) -> dict:
    """One-time seed from blocked_skills, calibrate threshold, prune redundant blocked entries."""
    ensure_db(db_path)
    stats = {"seeded": False, "ngram_keys_upserted": 0, "pruned": [], "threshold": None}
    if getattr(profile, "bad_cloud_seeded", False):
        return stats

    blocked = list(profile.blocked_skills or [])
    seed_protect_keys = {
        _normalize_skill_name(str(item)).lower()
        for item in blocked
        if _normalize_skill_name(str(item))
    }
    if blocked:
        stats["ngram_keys_upserted"] = seed_bad_cloud_from_blocked_skills(db_path, blocked)
    profile.bad_cloud_seeded = True
    stats["seeded"] = True

    if profile.skill_bigram_toxicity_threshold is None and count_bad_ngrams(db_path) > 0:
        profile.skill_bigram_toxicity_threshold = calibrate_threshold(db_path, profile)

    stats["threshold"] = resolve_toxicity_threshold(db_path, profile)
    stats["pruned"] = prune_blocked_skills_by_cloud(
        profile,
        db_path,
        stats["threshold"],
        protect_keys=seed_protect_keys,
    )
    return stats


def on_skills_blocked(
    profile: AppConfig,
    db_path: str,
    skills: list[str],
    *,
    recalibrate: bool = True,
) -> dict:
    ensure_db(db_path)
    ingested = ingest_blocked_skills(skills, db_path)

    if recalibrate and profile.skill_bigram_toxicity_threshold is None and ingested > 0:
        profile.skill_bigram_toxicity_threshold = calibrate_threshold(db_path, profile)

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
    return {
        "ngrams_ingested": ingested,
        "ngram_keys_upserted": ingested,
        "threshold": threshold,
        "pruned": pruned,
    }
