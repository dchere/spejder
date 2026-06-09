from typing import Optional

from spejder.config import AppConfig
from spejder.llm import LocalLLM
from spejder.managers.language_manager import (
    get_title_english_for_row as _get_title_english_for_row,
)
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)
from spejder.parsers.web_parser import _append_page_context_to_raw_text, _get_position_page_context
from spejder.workflows.text_prepend import _prepend_summary_to_raw_text, _prepend_title_to_raw_text


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
    return {
        "title": str(row.get("title", "") or ""),
        "title_english": _get_title_english_for_row(
            db_path,
            row,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        ),
    }
