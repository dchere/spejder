from typing import Optional

from spejder.config import AppConfig
from spejder.jobs.parsing.utils import split_title_trailing_i_place
from spejder.llm import LocalLLM
from spejder.managers.language_manager import (
    get_title_english_for_row as _get_title_english_for_row,
)
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)
from spejder.parsers.web_parser import (
    _append_page_context_to_raw_text,
    _extract_place_from_page_text,
    _get_position_page_context,
)
from spejder.workflows.text_prepend import _prepend_summary_to_raw_text, _prepend_title_to_raw_text


def _resolve_title_and_place(title: str, place: str) -> tuple[str, str]:
    title_clean = str(title or "").strip()
    place_clean = str(place or "").strip()
    if place_clean and place_clean.lower() != "unknown":
        return title_clean, place_clean

    if " - " in title_clean:
        maybe_title, maybe_place = title_clean.rsplit(" - ", 1)
        maybe_title = maybe_title.strip()
        maybe_place = maybe_place.strip()
        if maybe_title and maybe_place:
            return maybe_title, maybe_place

    parsed_title, parsed_place = split_title_trailing_i_place(title_clean)
    if parsed_place:
        return parsed_title, parsed_place
    return title_clean, place_clean


def _enrich_raw_text_with_position_page(
    db_path: str,
    row: dict,
    page_context_cache: Optional[dict] = None,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    title_translation_cache: Optional[dict] = None,
) -> str:
    raw = (row.get("raw_text") or "").strip()
    title_for_prompt = _get_title_english_for_row(
        db_path,
        row,
        runtime_profile=runtime_profile,
        title_translation_cache=title_translation_cache,
    )
    raw = _prepend_title_to_raw_text(title_for_prompt, raw)
    raw = _prepend_summary_to_raw_text(
        _translate_text_to_english_if_needed(
            row.get("summary", "") or "",
            runtime_profile=runtime_profile,
        ),
        raw,
    )
    link = (row.get("position_link") or "").strip()
    if not link:
        return raw

    page_context = _get_position_page_context(
        link,
        runtime_profile=runtime_profile,
        page_context_cache=page_context_cache,
    )
    place_hint = _extract_place_from_page_text(link, page_context)
    existing_place = str(row.get("place", "") or "").strip()
    if place_hint and (not existing_place or existing_place.lower() == "unknown"):
        row["place"] = place_hint
        job_id = int(row.get("id", 0) or 0)
        if job_id:
            from spejder.db import set_job_place

            set_job_place(db_path, job_id, place_hint)

    merged = _append_page_context_to_raw_text(raw, link, page_context)
    if merged:
        row["raw_text"] = merged
        return merged
    return raw


def _build_title_fields(
    db_path: str,
    row: dict,
    runtime_profile: Optional[AppConfig] = None,
    title_translation_cache: Optional[dict] = None,
) -> dict:
    title, place = _resolve_title_and_place(
        str(row.get("title", "") or ""),
        str(row.get("place", "") or ""),
    )
    row_for_title = dict(row)
    row_for_title["title"] = title
    row_for_title["place"] = place
    return {
        "title": title,
        "title_english": _get_title_english_for_row(
            db_path,
            row_for_title,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        ),
        "place": place,
    }
