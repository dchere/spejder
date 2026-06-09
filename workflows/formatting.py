
import html as html_lib

from spejder.managers.language_manager import normalize_title_compare_key as _normalize_title_compare_key
from spejder.managers.language_manager import normalize_title_text as _normalize_title_text
from spejder.workflows.text_prepend import (
    _is_invalid_summary_text,
    _prepend_summary_to_raw_text,
    _prepend_title_to_raw_text,
)


def _render_title_english_line(item: dict) -> str:
    title = _normalize_title_text(str(item.get("title", "") or ""))
    title_english = _normalize_title_text(str(item.get("title_english", "") or ""))
    if not title or not title_english:
        return ""
    if _normalize_title_compare_key(title) == _normalize_title_compare_key(title_english):
        return ""
    return f'<p><strong>In English:</strong> {html_lib.escape(title_english)}</p>'
