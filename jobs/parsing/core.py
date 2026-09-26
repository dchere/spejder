from typing import Optional

from spejder.db import _normalize_position_link, _provider_from_link

from .artifact_interpreter import interpret_artifacts
from .artifact_schema import CareerAlertArtifact
from .artifact_store import load_artifacts
from .companies import extract_company_title, sanitize_company_name
from .html_parser import _extract_html_entries_by_link
from .links import _is_job_link
from .linkedin import (
    _is_linkedin_boilerplate_entry,
    _is_linkedin_reference_position_link,
    _work_type_from_html_for_link,
)
from .merge import _ENTRY_FIELD_KEYS, merge_entry_fields
from .platform_registry import registered_platforms
from .text_parser import _extract_entries_from_text


def _fill_empty_fields(entry: dict, fields: dict) -> None:
    if not fields:
        return
    for key in _ENTRY_FIELD_KEYS:
        value = fields.get(key)
        if value and not entry.get(key):
            entry[key] = value


def extract_job_entries(
    doc: dict,
    *,
    artifacts: Optional[list[CareerAlertArtifact]] = None,
    artifacts_dir: Optional[str] = None,
    artifacts_disabled: Optional[list[str]] = None,
) -> list[dict]:
    text = doc.get("text", "") or ""
    html_text = doc.get("html", "") or ""
    title_hint = doc.get("title", "") or ""
    links = doc.get("links", []) or []
    if artifacts is None:
        artifact_list = load_artifacts(
            overlay_dir=artifacts_dir,
            disabled_ids=artifacts_disabled,
        )
    else:
        artifact_list = list(artifacts)
    artifact_by_link = interpret_artifacts(html_text, artifact_list, links=links)
    html_by_link = _extract_html_entries_by_link(html_text)
    # Vestas / Danfoss / Novo Nordisk Jobs2Web hosts: shipped artifacts only
    # (see jobs/parsing/artifacts/*.json). Python site extractors remain in
    # jobs2web.py for parity tests, not the merge path.
    platforms = registered_platforms()
    platform_maps = [spec.extract(html_text) for spec in platforms]

    by_text = _extract_entries_from_text(text)
    by_link = {}
    for entry in by_text:
        by_link[entry["position_link"]] = entry

    def _maps_for(link: str) -> tuple:
        # When an artifact matched the link, skip generic HTML fields so the
        # recipe (Jobs2Web middot split, etc.) is not blocked by raw anchor text.
        # Typed platforms still win via first-wins before the artifact map.
        html_fields = (
            {} if link in artifact_by_link else html_by_link.get(link, {})
        )
        maps: list[dict] = []
        for spec, by_platform in zip(platforms, platform_maps):
            fields = by_platform.get(link, {})
            if spec.merge_transform is not None:
                fields = spec.merge_transform(fields)
            maps.append(fields)
        maps.append(html_fields)
        maps.append(artifact_by_link.get(link, {}))
        return tuple(maps)

    for lnk, entry in by_link.items():
        merged = merge_entry_fields(*_maps_for(lnk))
        for key in _ENTRY_FIELD_KEYS:
            value = merged.get(key)
            if value:
                entry[key] = value

        if not entry.get("work_type"):
            wt = _work_type_from_html_for_link(html_text, lnk)
            if wt:
                entry["work_type"] = wt

        if not entry.get("source"):
            entry["source"] = _provider_from_link(lnk)

    for raw_link in links:
        if not raw_link:
            continue
        normalized = _normalize_position_link(raw_link)
        if not _is_job_link(normalized):
            continue
        if _is_linkedin_reference_position_link(raw_link, normalized):
            continue
        if normalized in by_link:
            continue
        merged = merge_entry_fields(*_maps_for(normalized))
        company, title = extract_company_title(text, title_hint)
        entry = {
            "company": merged.get("company") or "",
            "title": merged.get("title") or "",
            "place": merged.get("place") or "",
            "work_type": merged.get("work_type") or "",
            "position_link": normalized,
            "raw_text": merged.get("raw_text") or "",
            "source": merged.get("source") or "",
        }
        if not entry.get("company"):
            entry["company"] = company
        if not entry.get("title"):
            entry["title"] = title
        if not entry.get("work_type"):
            wt = _work_type_from_html_for_link(html_text, normalized)
            entry["work_type"] = wt if wt else "Unknown"
        if not entry.get("raw_text"):
            entry["raw_text"] = text[:2500]
        if not entry.get("source"):
            entry["source"] = _provider_from_link(normalized)
        by_link[normalized] = entry

    for normalized, art_fields in artifact_by_link.items():
        if normalized in by_link:
            continue
        if not (art_fields.get("title") or art_fields.get("company")):
            continue
        by_link[normalized] = {
            "company": art_fields.get("company") or "",
            "title": art_fields.get("title") or "",
            "place": art_fields.get("place") or "",
            "work_type": art_fields.get("work_type") or "Unknown",
            "position_link": normalized,
            "raw_text": art_fields.get("raw_text") or text[:2500],
            "source": art_fields.get("source") or _provider_from_link(normalized),
        }

    filtered_entries: list[dict] = []
    for entry in by_link.values():
        if "source" not in entry:
            entry["source"] = _provider_from_link(
                entry.get("position_link", ""))
        if entry.get("source") == "Getinge":
            entry["company"] = "Getinge"
        elif entry.get("source") == "Novo Nordisk":
            entry["company"] = "Novo Nordisk"
        else:
            entry["company"] = sanitize_company_name(entry.get("company", ""))
        if _is_linkedin_boilerplate_entry(entry):
            continue
        filtered_entries.append(entry)

    return filtered_entries
