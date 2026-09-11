from __future__ import annotations

import re
from typing import Optional

from spejder.db import _normalize_position_link
from spejder.jobs.parsing.artifact_interpreter import href_matches_artifact
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact


def _token_jaccard(a: str, b: str) -> float:
    ta = {t for t in re.findall(r"[a-z0-9]+", (a or "").casefold()) if t}
    tb = {t for t in re.findall(r"[a-z0-9]+", (b or "").casefold()) if t}
    if not ta and not tb:
        return 1.0
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def titles_agree(a: str, b: str) -> bool:
    left = (a or "").strip().casefold()
    right = (b or "").strip().casefold()
    if left == right:
        return True
    return _token_jaccard(left, right) >= 0.7


def proposed_title_matches_recovered(proposed_title: str, recovered: dict[str, str]) -> bool:
    """Accept LLM titles that match parsed title or full Jobs2Web anchor text."""
    if titles_agree(proposed_title, recovered.get("title", "")):
        return True
    if titles_agree(proposed_title, recovered.get("raw_text", "")):
        return True
    proposed = (proposed_title or "").strip().casefold()
    parsed_title = (recovered.get("title") or "").strip().casefold()
    if parsed_title and proposed.startswith(parsed_title):
        return True
    return False


def _position_link_set(positions: list) -> dict[str, str]:
    """normalized link → title from LLM positions list."""
    out: dict[str, str] = {}
    if not isinstance(positions, list):
        return out
    for item in positions:
        if not isinstance(item, dict):
            continue
        raw = str(item.get("position_link") or item.get("link") or item.get("href") or "").strip()
        if not raw:
            continue
        normalized = _normalize_position_link(raw)
        if not normalized:
            continue
        title = str(item.get("title") or "").strip()
        out[normalized] = title
    return out


def validate_synth_thresholds(
    proposed: dict[str, str],
    recovered: dict[str, dict[str, str]],
    *,
    link_ratio: float,
    title_ratio: float,
) -> tuple[bool, str]:
    p_links = set(proposed.keys())
    r_links = set(recovered.keys())
    if len(p_links) < 1 or len(r_links) < 1:
        return False, "empty_proposed_or_recovered"
    intersection = p_links & r_links
    if len(intersection) / len(p_links) < float(link_ratio):
        return False, "link_ratio"
    if not intersection:
        return False, "no_intersection"
    agreed = 0
    for link in intersection:
        if proposed_title_matches_recovered(
            proposed.get(link, ""), recovered.get(link, {})
        ):
            agreed += 1
    if agreed / len(intersection) < float(title_ratio):
        return False, "title_ratio"
    return True, "ok"


def _draft_matches_recovered(artifact: CareerAlertArtifact, recovered: dict[str, dict[str, str]]) -> bool:
    for link in recovered:
        if href_matches_artifact(link, artifact):
            return True
    return False


def _match_rules_too_broad(artifact: CareerAlertArtifact) -> bool:
    """Reject empty match lists that would match every href (or every path on a host)."""
    hosts = [h for h in (artifact.match.host_substrings or []) if str(h).strip()]
    paths = [p for p in (artifact.match.path_includes or []) if str(p).strip()]
    return not hosts or not paths


def _recovered_too_broad(
    proposed: dict[str, str],
    recovered: dict[str, dict[str, str]],
    *,
    artifact: Optional[CareerAlertArtifact] = None,
) -> bool:
    """Reject when match rules pull in far more links than the LLM proposed."""
    n_proposed = len(proposed)
    n_recovered = len(recovered)
    if n_proposed < 1:
        return True
    # CTA digests often list many real jobs while the LLM only cites a sample.
    if artifact is not None and artifact.fields.from_anchor == "ancestor_strong_or_first_line":
        return n_recovered > max(40, n_proposed * 5)
    return n_recovered > max(3, n_proposed * 2)
