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
from .platforms import (
    _extract_demant_entries_by_link,
    _extract_google_entries_by_link,
    _extract_jobindex_entries_by_link,
)
from .platforms_career_alerts import (
    _extract_danfoss_entries_by_link,
    _extract_djinni_entries_by_link,
    _extract_novonordisk_entries_by_link,
    _extract_oracle_cx_entries_by_link,
    _extract_thehub_entries_by_link,
    _extract_vestas_entries_by_link,
)
from .text_parser import _extract_entries_from_text

_ENTRY_FIELD_KEYS = ("title", "company", "place", "work_type", "raw_text", "source")


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
    jobindex_by_link = _extract_jobindex_entries_by_link(html_text)
    demant_by_link = _extract_demant_entries_by_link(html_text)
    danfoss_by_link = _extract_danfoss_entries_by_link(html_text)
    google_by_link = _extract_google_entries_by_link(html_text)
    vestas_by_link = _extract_vestas_entries_by_link(html_text)
    novonordisk_by_link = _extract_novonordisk_entries_by_link(html_text)
    thehub_by_link = _extract_thehub_entries_by_link(html_text)
    djinni_by_link = _extract_djinni_entries_by_link(html_text)
    oracle_by_link = _extract_oracle_cx_entries_by_link(html_text)

    by_text = _extract_entries_from_text(text)
    by_link = {}
    for entry in by_text:
        by_link[entry["position_link"]] = entry

    for lnk, entry in by_link.items():
        html_fields = html_by_link.get(lnk, {})
        ji_fields = jobindex_by_link.get(lnk, {})
        demant_fields = demant_by_link.get(lnk, {})
        danfoss_fields = danfoss_by_link.get(lnk, {})
        google_fields = google_by_link.get(lnk, {})
        vestas_fields = vestas_by_link.get(lnk, {})
        novonordisk_fields = novonordisk_by_link.get(lnk, {})
        thehub_fields = thehub_by_link.get(lnk, {})
        djinni_fields = djinni_by_link.get(lnk, {})
        oracle_fields = oracle_by_link.get(lnk, {})

        if google_fields.get("title"):
            entry["title"] = google_fields["title"]
        if google_fields.get("company"):
            entry["company"] = google_fields["company"]
        if google_fields.get("place"):
            entry["place"] = google_fields["place"]
        if google_fields.get("work_type"):
            entry["work_type"] = google_fields["work_type"]
        if google_fields.get("raw_text"):
            entry["raw_text"] = google_fields["raw_text"]
        if google_fields.get("source"):
            entry["source"] = google_fields["source"]

        if vestas_fields.get("title"):
            entry["title"] = vestas_fields["title"]
        if vestas_fields.get("company"):
            entry["company"] = vestas_fields["company"]
        if vestas_fields.get("place"):
            entry["place"] = vestas_fields["place"]
        if vestas_fields.get("work_type"):
            entry["work_type"] = vestas_fields["work_type"]
        if vestas_fields.get("raw_text"):
            entry["raw_text"] = vestas_fields["raw_text"]
        if vestas_fields.get("source"):
            entry["source"] = vestas_fields["source"]

        if novonordisk_fields.get("title"):
            entry["title"] = novonordisk_fields["title"]
        if novonordisk_fields.get("company"):
            entry["company"] = novonordisk_fields["company"]
        if novonordisk_fields.get("place"):
            entry["place"] = novonordisk_fields["place"]
        if novonordisk_fields.get("work_type"):
            entry["work_type"] = novonordisk_fields["work_type"]
        if novonordisk_fields.get("raw_text"):
            entry["raw_text"] = novonordisk_fields["raw_text"]
        if novonordisk_fields.get("source"):
            entry["source"] = novonordisk_fields["source"]

        if thehub_fields.get("title"):
            entry["title"] = thehub_fields["title"]
        if thehub_fields.get("company"):
            entry["company"] = thehub_fields["company"]
        if thehub_fields.get("place"):
            entry["place"] = thehub_fields["place"]
        if thehub_fields.get("work_type"):
            entry["work_type"] = thehub_fields["work_type"]
        if thehub_fields.get("raw_text"):
            entry["raw_text"] = thehub_fields["raw_text"]
        if thehub_fields.get("source"):
            entry["source"] = thehub_fields["source"]

        if djinni_fields.get("title"):
            entry["title"] = djinni_fields["title"]
        if djinni_fields.get("company"):
            entry["company"] = djinni_fields["company"]
        if djinni_fields.get("place"):
            entry["place"] = djinni_fields["place"]
        if djinni_fields.get("work_type"):
            entry["work_type"] = djinni_fields["work_type"]
        if djinni_fields.get("raw_text"):
            entry["raw_text"] = djinni_fields["raw_text"]
        if djinni_fields.get("source"):
            entry["source"] = djinni_fields["source"]

        if oracle_fields.get("title"):
            entry["title"] = oracle_fields["title"]
        if oracle_fields.get("company"):
            entry["company"] = oracle_fields["company"]
        if oracle_fields.get("place"):
            entry["place"] = oracle_fields["place"]
        if oracle_fields.get("work_type"):
            entry["work_type"] = oracle_fields["work_type"]
        if oracle_fields.get("raw_text"):
            entry["raw_text"] = oracle_fields["raw_text"]
        if oracle_fields.get("source"):
            entry["source"] = oracle_fields["source"]

        if danfoss_fields.get("title"):
            entry["title"] = danfoss_fields["title"]
        if danfoss_fields.get("company"):
            entry["company"] = danfoss_fields["company"]
        if danfoss_fields.get("place"):
            entry["place"] = danfoss_fields["place"]
        if danfoss_fields.get("work_type"):
            entry["work_type"] = danfoss_fields["work_type"]
        if danfoss_fields.get("raw_text"):
            entry["raw_text"] = danfoss_fields["raw_text"]
        if danfoss_fields.get("source"):
            entry["source"] = danfoss_fields["source"]

        if demant_fields.get("title"):
            entry["title"] = demant_fields["title"]
        if demant_fields.get("company"):
            entry["company"] = demant_fields["company"]
        if demant_fields.get("place"):
            entry["place"] = demant_fields["place"]
        if demant_fields.get("work_type"):
            entry["work_type"] = demant_fields["work_type"]
        if demant_fields.get("raw_text"):
            entry["raw_text"] = demant_fields["raw_text"]
        if demant_fields.get("source"):
            entry["source"] = demant_fields["source"]

        if ji_fields.get("title"):
            entry["title"] = ji_fields["title"]
        if ji_fields.get("company"):
            entry["company"] = ji_fields["company"]
        if ji_fields.get("place"):
            entry["place"] = ji_fields["place"]
        if ji_fields.get("raw_text"):
            entry["raw_text"] = ji_fields["raw_text"]

        platform_title = (
            google_fields.get("title")
            or thehub_fields.get("title")
            or djinni_fields.get("title")
            or danfoss_fields.get("title")
            or vestas_fields.get("title")
            or novonordisk_fields.get("title")
            or oracle_fields.get("title")
            or demant_fields.get("title")
            or ji_fields.get("title")
        )
        platform_company = (
            google_fields.get("company")
            or thehub_fields.get("company")
            or djinni_fields.get("company")
            or danfoss_fields.get("company")
            or vestas_fields.get("company")
            or novonordisk_fields.get("company")
            or oracle_fields.get("company")
            or demant_fields.get("company")
            or ji_fields.get("company")
        )
        platform_place = (
            google_fields.get("place")
            or thehub_fields.get("place")
            or djinni_fields.get("place")
            or danfoss_fields.get("place")
            or vestas_fields.get("place")
            or novonordisk_fields.get("place")
            or oracle_fields.get("place")
            or demant_fields.get("place")
            or ji_fields.get("place")
        )
        platform_work_type = (
            google_fields.get("work_type")
            or thehub_fields.get("work_type")
            or djinni_fields.get("work_type")
            or danfoss_fields.get("work_type")
            or vestas_fields.get("work_type")
            or novonordisk_fields.get("work_type")
            or oracle_fields.get("work_type")
            or demant_fields.get("work_type")
        )
        platform_raw_text = (
            google_fields.get("raw_text")
            or thehub_fields.get("raw_text")
            or djinni_fields.get("raw_text")
            or danfoss_fields.get("raw_text")
            or vestas_fields.get("raw_text")
            or novonordisk_fields.get("raw_text")
            or oracle_fields.get("raw_text")
            or demant_fields.get("raw_text")
            or ji_fields.get("raw_text")
        )

        if html_fields.get("title") and not platform_title:
            entry["title"] = html_fields["title"]
        if html_fields.get("company") and not platform_company:
            entry["company"] = html_fields["company"]
        if html_fields.get("place") and not platform_place:
            entry["place"] = html_fields["place"]

        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, lnk
        )
        if wt and not platform_work_type:
            entry["work_type"] = wt

        if html_fields.get("raw_text") and not platform_raw_text:
            entry["raw_text"] = html_fields["raw_text"]

        _fill_empty_fields(entry, artifact_by_link.get(lnk, {}))

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
        html_fields = html_by_link.get(normalized, {})
        ji_fields = jobindex_by_link.get(normalized, {})
        demant_fields = demant_by_link.get(normalized, {})
        danfoss_fields = danfoss_by_link.get(normalized, {})
        google_fields = google_by_link.get(normalized, {})
        vestas_fields = vestas_by_link.get(normalized, {})
        novonordisk_fields = novonordisk_by_link.get(normalized, {})
        thehub_fields = thehub_by_link.get(normalized, {})
        djinni_fields = djinni_by_link.get(normalized, {})
        oracle_fields = oracle_by_link.get(normalized, {})
        art_fields = artifact_by_link.get(normalized, {})
        company, title = extract_company_title(text, title_hint)
        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, normalized
        )
        by_link[normalized] = {
            "company": google_fields.get("company")
            or thehub_fields.get("company")
            or djinni_fields.get("company")
            or danfoss_fields.get("company")
            or vestas_fields.get("company")
            or novonordisk_fields.get("company")
            or oracle_fields.get("company")
            or demant_fields.get("company")
            or ji_fields.get("company")
            or html_fields.get("company")
            or art_fields.get("company")
            or company,
            "title": google_fields.get("title")
            or thehub_fields.get("title")
            or djinni_fields.get("title")
            or danfoss_fields.get("title")
            or vestas_fields.get("title")
            or novonordisk_fields.get("title")
            or oracle_fields.get("title")
            or demant_fields.get("title")
            or ji_fields.get("title")
            or html_fields.get("title")
            or art_fields.get("title")
            or title,
            "place": google_fields.get("place")
            or thehub_fields.get("place")
            or djinni_fields.get("place")
            or danfoss_fields.get("place")
            or vestas_fields.get("place")
            or novonordisk_fields.get("place")
            or oracle_fields.get("place")
            or demant_fields.get("place")
            or ji_fields.get("place")
            or html_fields.get("place")
            or art_fields.get("place")
            or "",
            "work_type": google_fields.get("work_type")
            or thehub_fields.get("work_type")
            or djinni_fields.get("work_type")
            or danfoss_fields.get("work_type")
            or vestas_fields.get("work_type")
            or novonordisk_fields.get("work_type")
            or oracle_fields.get("work_type")
            or demant_fields.get("work_type")
            or art_fields.get("work_type")
            or (wt if wt else "Unknown"),
            "position_link": normalized,
            "raw_text": google_fields.get("raw_text")
            or thehub_fields.get("raw_text")
            or djinni_fields.get("raw_text")
            or danfoss_fields.get("raw_text")
            or vestas_fields.get("raw_text")
            or novonordisk_fields.get("raw_text")
            or oracle_fields.get("raw_text")
            or demant_fields.get("raw_text")
            or ji_fields.get("raw_text")
            or html_fields.get("raw_text")
            or art_fields.get("raw_text")
            or text[:2500],
            "source": google_fields.get("source")
            or thehub_fields.get("source")
            or djinni_fields.get("source")
            or danfoss_fields.get("source")
            or vestas_fields.get("source")
            or novonordisk_fields.get("source")
            or oracle_fields.get("source")
            or demant_fields.get("source")
            or art_fields.get("source")
            or _provider_from_link(normalized),
        }

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


