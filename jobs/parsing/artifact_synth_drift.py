"""Format-drift helpers for career-alert synth second pass."""

from __future__ import annotations

import logging
from typing import Optional

from spejder.jobs.parsing.artifact_interpreter import (
    artifact_prefilter_matches,
    interpret_artifact,
)
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import (
    list_loaded_artifacts,
    save_overlay_artifact,
)

logger = logging.getLogger(__name__)

_DRIFT_PROMPT_NOTE = """
Note: existing overlay rules for this host matched the email but recovered no
usable job titles on this HTML (format drift). Prefer alternate match path /
anchor_text_equals / from_anchor (including prev_sibling_text when the title
sits beside the CTA) so recovery succeeds on this variant.
"""


def _usable_recovered_titles(recovered: dict[str, dict[str, str]]) -> bool:
    cta = {
        "apply here",
        "apply now",
        "apply",
        "view job",
        "view role",
        "see job",
        "read more",
        "learn more",
    }
    for fields in (recovered or {}).values():
        title = str((fields or {}).get("title") or "").strip()
        if title and title.casefold() not in cta:
            return True
    return False


def find_stale_artifacts(
    html_text: str,
    artifacts: list[CareerAlertArtifact],
    *,
    links: Optional[list[str]] = None,
) -> list[CareerAlertArtifact]:
    """Artifacts that prefilter-match this digest but recover no usable titles."""
    stale: list[CareerAlertArtifact] = []
    link_list = list(links or [])
    for artifact in artifacts or []:
        if not artifact.enabled:
            continue
        if not artifact_prefilter_matches(html_text, link_list, artifact):
            continue
        recovered = interpret_artifact(html_text, artifact)
        if not _usable_recovered_titles(recovered):
            stale.append(artifact)
    return stale


def drift_prompt_note(stale: list[CareerAlertArtifact]) -> str:
    if not stale:
        return ""
    ids = ", ".join(a.id for a in stale[:5])
    return _DRIFT_PROMPT_NOTE + f"Stale artifact ids: {ids}\n"


def _hosts_overlap(left: CareerAlertArtifact, right: CareerAlertArtifact) -> bool:
    left_hosts = {h.casefold() for h in (left.match.host_substrings or []) if h.strip()}
    right_hosts = {h.casefold() for h in (right.match.host_substrings or []) if h.strip()}
    if not left_hosts or not right_hosts:
        return False
    for a in left_hosts:
        for b in right_hosts:
            if a in b or b in a:
                return True
    return False


def disable_stale_overlays_for(
    saved: CareerAlertArtifact,
    stale: list[CareerAlertArtifact],
    *,
    overlay_dir: Optional[str],
) -> list[str]:
    """Disable overlay (non-shipped) stale recipes that share a host with ``saved``."""
    if saved is None or not stale or overlay_dir is None:
        return []
    loaded = {
        item.artifact.id: item
        for item in list_loaded_artifacts(
            overlay_dir=overlay_dir,
            include_disabled=True,
        )
    }
    disabled: list[str] = []
    for artifact in stale:
        if artifact.id == saved.id:
            continue
        if not _hosts_overlap(saved, artifact):
            continue
        item = loaded.get(artifact.id)
        if item is None or item.origin != "overlay":
            continue
        if artifact.source not in ("llm_synth", "heuristic", "manual"):
            continue
        if not artifact.enabled:
            continue
        updated = artifact.model_copy(update={"enabled": False})
        try:
            save_overlay_artifact(updated, overlay_dir=overlay_dir)
        except OSError as exc:
            logger.warning("failed to disable stale overlay %s: %s", artifact.id, exc)
            continue
        disabled.append(artifact.id)
        logger.info(
            "disabled stale overlay after drift synth: id=%s replaced_by=%s",
            artifact.id,
            saved.id,
        )
    return disabled
