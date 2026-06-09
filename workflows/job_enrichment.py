"""Job enrichment facade — re-exports split workflow helpers."""

from spejder.managers.language_manager import (
    finalize_title_english as _finalize_title_english,
)
from spejder.managers.language_manager import (
    get_title_english_for_row as _get_title_english_for_row,
)
from spejder.managers.language_manager import (
    normalize_title_compare_key as _normalize_title_compare_key,
)
from spejder.managers.language_manager import (
    translate_text_to_english_if_needed as _translate_text_to_english_if_needed,
)
from spejder.managers.language_manager import (
    translate_title_to_english as _translate_title_to_english,
)
from spejder.workflows.job_descriptions import (
    _build_description_summary,
    _fallback_description_text,
    _generate_missing_descriptions_for_ingest,
    _has_invalid_description_marker,
    _is_low_quality_description,
    _summary_for_display,
)
from spejder.workflows.job_easy_apply import _is_easy_apply_item
from spejder.workflows.job_skills_materialize import (
    materialize_job_skills,
    materialize_jobs_skills,
    materialize_relevant_and_applied_skills,
)
from spejder.workflows.job_text_enrichment import (
    _build_title_fields,
    _enrich_raw_text_with_position_page,
)
from spejder.workflows.text_prepend import _is_invalid_summary_text
from spejder.workflows.job_translation import make_translate_job_entry_for_storage

__all__ = [
    "make_translate_job_entry_for_storage",
    "_generate_missing_descriptions_for_ingest",
    "_build_description_summary",
    "_build_title_fields",
    "_enrich_raw_text_with_position_page",
    "_fallback_description_text",
    "_finalize_title_english",
    "_get_title_english_for_row",
    "_has_invalid_description_marker",
    "_is_easy_apply_item",
    "_is_invalid_summary_text",
    "_is_low_quality_description",
    "_normalize_title_compare_key",
    "_summary_for_display",
    "_translate_text_to_english_if_needed",
    "_translate_title_to_english",
    "materialize_job_skills",
    "materialize_jobs_skills",
    "materialize_relevant_and_applied_skills",
]
