"""Load and merge shipped + overlay career-alert artifacts."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

from pydantic import ValidationError

from spejder.core import resolve_user_path
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact

logger = logging.getLogger(__name__)

SHIPPED_ARTIFACTS_DIR = os.path.join(os.path.dirname(__file__), "artifacts")
DEFAULT_OVERLAY_DIR = "./career_alert_artifacts"


@dataclass(frozen=True)
class LoadedArtifact:
    artifact: CareerAlertArtifact
    path: str
    origin: str  # "shipped" | "overlay"


def shipped_artifacts_dir() -> str:
    return SHIPPED_ARTIFACTS_DIR


def resolve_overlay_dir(overlay_dir: Optional[str] = None) -> str:
    raw = (overlay_dir or DEFAULT_OVERLAY_DIR).strip() or DEFAULT_OVERLAY_DIR
    return resolve_user_path(raw)


def _load_json_file(path: str) -> Optional[CareerAlertArtifact]:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("skipping unreadable artifact %s: %s", path, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("skipping non-object artifact %s", path)
        return None
    try:
        return CareerAlertArtifact.model_validate(data)
    except ValidationError as exc:
        logger.warning("skipping invalid artifact %s: %s", path, exc)
        return None


def _iter_json_paths(directory: str) -> list[str]:
    if not directory or not os.path.isdir(directory):
        return []
    paths = []
    for name in sorted(os.listdir(directory)):
        if not name.endswith(".json"):
            continue
        paths.append(os.path.join(directory, name))
    return paths


def load_artifacts_from_dir(directory: str, *, origin: str) -> dict[str, LoadedArtifact]:
    by_id: dict[str, LoadedArtifact] = {}
    for path in _iter_json_paths(directory):
        artifact = _load_json_file(path)
        if artifact is None:
            continue
        by_id[artifact.id] = LoadedArtifact(artifact=artifact, path=path, origin=origin)
    return by_id


def load_artifacts(
    *,
    overlay_dir: Optional[str] = None,
    disabled_ids: Optional[list[str]] = None,
    shipped_dir: Optional[str] = None,
) -> list[CareerAlertArtifact]:
    """Merge shipped + overlay when overlay_dir is set; otherwise shipped only."""
    loaded = list_loaded_artifacts(
        overlay_dir=overlay_dir,
        disabled_ids=disabled_ids,
        shipped_dir=shipped_dir,
        include_disabled=False,
    )
    return [item.artifact for item in loaded]


def list_loaded_artifacts(
    *,
    overlay_dir: Optional[str] = None,
    disabled_ids: Optional[list[str]] = None,
    shipped_dir: Optional[str] = None,
    include_disabled: bool = True,
) -> list[LoadedArtifact]:
    """Merge shipped + overlay when overlay_dir is set; otherwise shipped only.

    Callers without profile/artifacts_dir context (e.g. extract_job_entries with
    defaults) must not auto-merge ``./career_alert_artifacts``.
    """
    disabled = {str(x).strip() for x in (disabled_ids or []) if str(x).strip()}
    shipped = load_artifacts_from_dir(shipped_dir or SHIPPED_ARTIFACTS_DIR, origin="shipped")
    merged: dict[str, LoadedArtifact] = dict(shipped)
    if overlay_dir is not None:
        overlay = load_artifacts_from_dir(resolve_overlay_dir(overlay_dir), origin="overlay")
        merged.update(overlay)

    result: list[LoadedArtifact] = []
    for artifact_id in sorted(merged.keys(), key=lambda i: (-merged[i].artifact.priority, i)):
        item = merged[artifact_id]
        is_disabled = (not item.artifact.enabled) or (artifact_id in disabled)
        if is_disabled and not include_disabled:
            continue
        result.append(item)
    return result


def save_overlay_artifact(
    artifact: CareerAlertArtifact,
    *,
    overlay_dir: Optional[str] = None,
) -> str:
    """Persist artifact JSON under the user overlay directory (never the package tree)."""
    directory = resolve_overlay_dir(overlay_dir)
    os.makedirs(directory, exist_ok=True)
    safe_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in artifact.id)
    path = os.path.join(directory, f"{safe_id}.json")
    payload = artifact.model_dump(mode="json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    return path


def is_shipped_id(artifact_id: str, *, shipped_dir: Optional[str] = None) -> bool:
    shipped = load_artifacts_from_dir(shipped_dir or SHIPPED_ARTIFACTS_DIR, origin="shipped")
    return artifact_id in shipped
