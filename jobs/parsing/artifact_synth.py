"""LLM synthesis of career-alert artifacts with re-validation gate."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from pydantic import ValidationError

from spejder.config import AppConfig
from spejder.db import _normalize_position_link
from spejder.jobs.parsing.artifact_heuristic import draft_cta_ancestor_artifact
from spejder.jobs.parsing.artifact_interpreter import href_matches_artifact, interpret_artifact
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import (
    is_shipped_id,
    resolve_overlay_dir,
    save_overlay_artifact,
)
from spejder.jobs.parsing.html_shrink import shrink_html_for_prompt
from spejder.llm import LocalLLM

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


def _balanced_json_slice(text: str, start: int) -> str:
    """Return substring of text starting at ``start`` with balanced ``{}`` or ``[]``."""
    if start < 0 or start >= len(text):
        return ""
    opener = text[start]
    closer = {"{": "}", "[": "]"}.get(opener)
    if closer is None:
        return ""
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def _extract_json_object(text: str) -> dict:
    payload = (text or "").strip()
    if not payload:
        return {}
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload, count=1, flags=re.IGNORECASE)
        payload = re.sub(r"\s*```$", "", payload, count=1)
    start = payload.find("{")
    if start == -1:
        return {}
    candidate = _balanced_json_slice(payload, start)
    if not candidate:
        end = payload.rfind("}")
        if end <= start:
            return {}
        candidate = payload[start : end + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return _recover_truncated_synth_payload(payload)


def _extract_named_object(text: str, key: str) -> dict:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*\{{', text)
    if not match:
        return {}
    brace_at = text.find("{", match.start())
    slice_text = _balanced_json_slice(text, brace_at)
    if not slice_text:
        return {}
    try:
        parsed = json.loads(slice_text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_partial_positions(text: str) -> list[dict]:
    match = re.search(r'"positions"\s*:\s*\[', text)
    if not match:
        return []
    bracket_at = text.find("[", match.start())
    slice_text = _balanced_json_slice(text, bracket_at)
    raw_items: list[str] = []
    if slice_text:
        try:
            parsed = json.loads(slice_text)
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        body = slice_text[1:-1]
    else:
        body = text[bracket_at + 1 :]
    depth = 0
    in_string = False
    escape = False
    start = None
    for index, ch in enumerate(body):
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if depth == 0:
                start = index
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                raw_items.append(body[start : index + 1])
                start = None
    positions: list[dict] = []
    for item in raw_items:
        try:
            parsed = json.loads(item)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            positions.append(parsed)
    return positions


def _recover_truncated_synth_payload(text: str) -> dict:
    """Recover artifact (+ any complete position objects) when output was cut mid-JSON."""
    artifact = _extract_named_object(text, "artifact")
    positions = _extract_partial_positions(text)
    if not artifact and not positions:
        return {}
    payload: dict = {}
    if artifact:
        payload["artifact"] = artifact
    if positions:
        payload["positions"] = positions
    return payload


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
) -> tuple[Optional[CareerAlertArtifact], str]:
    """
    Heuristic and/or shrink → LLM → validate → optionally persist overlay.
    Returns (artifact_or_None, reason).
    """
    if not html_text:
        return None, "missing_html"

    # Deterministic CTA digests (iCIMS etc.) before spending an LLM call.
    heuristic = draft_cta_ancestor_artifact(html_text)
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
