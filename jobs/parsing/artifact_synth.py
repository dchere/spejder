"""LLM synthesis of career-alert artifacts with re-validation gate."""

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from pydantic import ValidationError

from spejder.config import AppConfig
from spejder.jobs.parsing.artifact_heuristic import (
    cta_labels_from_artifacts,
    draft_cta_ancestor_artifact,
)
from spejder.jobs.parsing.artifact_interpreter import interpret_artifact
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import (
    is_shipped_id,
    load_artifacts,
    resolve_overlay_dir,
    save_overlay_artifact,
)
from spejder.jobs.parsing.html_shrink import shrink_html_for_prompt
from spejder.llm import LocalLLM
from spejder.jobs.parsing.artifact_synth_json import (
    _extract_json_object,
)
from spejder.jobs.parsing.artifact_synth_validate import (
    _draft_matches_recovered,
    _match_rules_too_broad,
    _position_link_set,
    _recovered_too_broad,
    validate_synth_thresholds,
)

logger = logging.getLogger(__name__)

_CTA_TITLE_LABELS = frozenset(
    {
        "apply here",
        "apply now",
        "apply",
        "view job",
        "view role",
        "see job",
        "read more",
        "learn more",
    }
)

_SYNTH_MAX_TOKENS = 1600
_SYNTH_MAX_POSITIONS = 5

_SYNTH_PROMPT = """You extract job-alert parsing rules as JSON only.
Given shrunk HTML from a career-alert email, return a JSON object with:
{{
  "artifact": {{
    "id": "synth_<host>_<short>",
    "version": 1,
    "priority": 50,
    "enabled": true,
    "match": {{
      "host_substrings": ["example.com"],
      "path_includes": ["/job/"],
      "anchor_text_equals": []
    }},
    "extract": {{"mode": "filtered_links"}},
    "fields": {{
      "from_anchor": "jobs2web_middot_or_dash",
      "company": "Company Name",
      "source": "Source Label"
    }}
  }},
  "positions": [{{"position_link": "...", "title": "..."}}]
}}
Rules:
- Use only filtered_links extract mode and known from_anchor opcodes:
  jobs2web_middot_or_dash | anchor_text_compact | ancestor_strong_or_first_line.
- Emit the artifact object before the positions array.
- Include at most {max_positions} positions (enough to prove the match rules).
- Copy position_link values exactly from the HTML hrefs (already without query strings).
- Prefer short titles (job title only when the anchor mixes title and place).
- artifact.match must match those job links (use the real link host/path, not a careers marketing host).
- When anchors say only "Apply here"/"Apply now" and the title appears beside them
  (see " :: Title" context in the HTML), set:
  from_anchor=ancestor_strong_or_first_line,
  path_includes to the CTA path (often "/f/a/" for iCIMS),
  and anchor_text_equals=["Apply here"] (or the CTA label used).
- Reply with a single complete JSON object only, no markdown.

HTML:
{html}
"""


def _proposed_from_recovered(
    recovered: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Use interpreter titles when the LLM omitted/truncated the positions array."""
    usable: dict[str, str] = {}
    for link, fields in recovered.items():
        title = str((fields or {}).get("title") or "").strip()
        if not title or title.casefold() in _CTA_TITLE_LABELS:
            continue
        usable[link] = title
        if len(usable) >= _SYNTH_MAX_POSITIONS:
            break
    return usable


def _safe_overlay_filename(artifact_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in artifact_id)


def _ensure_synth_id(
    artifact: CareerAlertArtifact,
    html_text: str,
    *,
    overlay_dir: Optional[str] = None,
) -> CareerAlertArtifact:
    """Always assign synth_<host>_<html_hash6> (append counter if overlay file exists)."""
    host = "unknown"
    for sub in artifact.match.host_substrings or []:
        host = re.sub(r"[^a-z0-9]+", "", (sub or "").lower()) or host
        break
    if host == "unknown":
        for link in interpret_artifact(html_text, artifact):
            host = re.sub(r"[^a-z0-9]+", "", (urlparse(link).netloc or "").lower()) or host
            break
    digest = hashlib.sha1((html_text or "").encode("utf-8", errors="ignore")).hexdigest()[:6]
    base_id = f"synth_{host}_{digest}"
    candidate = base_id
    if overlay_dir is not None:
        directory = resolve_overlay_dir(overlay_dir)
        n = 2
        while os.path.exists(os.path.join(directory, f"{_safe_overlay_filename(candidate)}.json")):
            candidate = f"{base_id}_{n}"
            n += 1
    return artifact.model_copy(update={"id": candidate})


def _persist_validated_artifact(
    draft: CareerAlertArtifact,
    *,
    html_text: str,
    proposed: dict[str, str],
    recovered: dict[str, dict[str, str]],
    profile: AppConfig,
    llm: Optional[LocalLLM],
    overlay_dir: Optional[str],
) -> tuple[Optional[CareerAlertArtifact], str]:
    ok, reason = validate_synth_thresholds(
        proposed,
        recovered,
        link_ratio=float(profile.career_alert_synth_link_ratio),
        title_ratio=float(profile.career_alert_synth_title_ratio),
    )
    if not ok:
        return None, reason
    if not _draft_matches_recovered(draft, recovered):
        return None, "match_rules"
    if _recovered_too_broad(proposed, recovered, artifact=draft):
        return None, "match_too_broad"

    target_dir = overlay_dir if overlay_dir is not None else profile.career_alert_artifacts_dir
    draft = _ensure_synth_id(draft, html_text, overlay_dir=target_dir)
    if is_shipped_id(draft.id):
        return None, "shipped_id_collision"

    model_basename = None
    if llm is not None:
        model_basename = os.path.basename(str(getattr(llm, "model_path", "") or "")) or None
    eml_hash = hashlib.sha1((html_text or "").encode("utf-8", errors="ignore")).hexdigest()[:16]
    provenance = draft.source if draft.source in ("heuristic", "manual", "llm_synth") else "llm_synth"
    final = draft.model_copy(
        update={
            "source": provenance,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "model_path": model_basename,
            "parent_eml_hash": eml_hash,
            "enabled": True,
        }
    )
    try:
        path = save_overlay_artifact(final, overlay_dir=target_dir)
    except OSError as exc:
        logger.warning("career-alert synth persist failed: %s", exc)
        return None, "persist_error"
    logger.info("career-alert artifact synthesized: id=%s path=%s", final.id, path)
    return final, "ok"


def try_synthesize_artifact(
    html_text: str,
    llm: Optional[LocalLLM],
    profile: AppConfig,
    *,
    overlay_dir: Optional[str] = None,
    max_prompt_chars: Optional[int] = None,
    title_hint: str = "",
    text: str = "",
    from_hint: str = "",
) -> tuple[Optional[CareerAlertArtifact], str]:
    """
    Heuristic and/or shrink → LLM → validate → optionally persist overlay.
    Returns (artifact_or_None, reason).
    """
    if not html_text:
        return None, "missing_html"

    target_dir = (
        overlay_dir
        if overlay_dir is not None
        else profile.career_alert_artifacts_dir
    )
    label_artifacts = load_artifacts(
        overlay_dir=target_dir,
        disabled_ids=profile.career_alert_artifacts_disabled,
    )
    known_cta = cta_labels_from_artifacts(label_artifacts)

    # Deterministic CTA digests (iCIMS etc.) before spending an LLM call.
    heuristic = draft_cta_ancestor_artifact(
        html_text,
        title_hint=title_hint,
        text=text,
        from_hint=from_hint,
        known_cta_labels=known_cta,
    )
    if heuristic is not None:
        recovered = interpret_artifact(html_text, heuristic)
        proposed = _proposed_from_recovered(recovered)
        if proposed:
            saved, reason = _persist_validated_artifact(
                heuristic,
                html_text=html_text,
                proposed=proposed,
                recovered=recovered,
                profile=profile,
                llm=None,
                overlay_dir=overlay_dir,
            )
            if saved is not None:
                return saved, reason

    if llm is None:
        return None, "no_model"

    budget = max_prompt_chars
    if budget is None:
        n_ctx = int(getattr(profile, "n_ctx", 8192) or 8192)
        budget = max(2000, min(12000, n_ctx * 2))

    shrunk = shrink_html_for_prompt(html_text, max_chars=budget)
    if not shrunk.strip():
        return None, "empty_shrink"
    if len(shrunk) >= budget and shrunk.endswith("..."):
        # Still usable; proceed with truncated prompt
        pass

    prompt = _SYNTH_PROMPT.format(html=shrunk, max_positions=_SYNTH_MAX_POSITIONS)
    try:
        raw = llm.generate(prompt, max_tokens=_SYNTH_MAX_TOKENS)
    except (RuntimeError, OSError, ValueError, TypeError) as exc:
        logger.warning("career-alert synth LLM failed: %s", exc)
        return None, "llm_error"

    payload = _extract_json_object(raw)
    if not payload:
        return None, "bad_json"

    proposed = _position_link_set(payload.get("positions") or [])
    artifact_data = payload.get("artifact")
    if not isinstance(artifact_data, dict):
        return None, "missing_artifact"

    try:
        draft = CareerAlertArtifact.model_validate(artifact_data)
    except ValidationError as exc:
        logger.info("career-alert synth schema reject: %s", exc)
        return None, "schema"

    if draft.extract.mode != "filtered_links":
        return None, "unsupported_extract_mode"

    if _match_rules_too_broad(draft):
        return None, "empty_match_rules"

    target_dir = overlay_dir if overlay_dir is not None else profile.career_alert_artifacts_dir
    draft = _ensure_synth_id(draft, html_text, overlay_dir=target_dir)
    if is_shipped_id(draft.id):
        return None, "shipped_id_collision"

    recovered = interpret_artifact(html_text, draft)
    if not proposed:
        proposed = _proposed_from_recovered(recovered)
    return _persist_validated_artifact(
        draft.model_copy(update={"source": "llm_synth"}),
        html_text=html_text,
        proposed=proposed,
        recovered=recovered,
        profile=profile,
        llm=llm,
        overlay_dir=overlay_dir,
    )

