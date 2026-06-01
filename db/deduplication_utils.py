import re
def _cross_source_dedupe_key(source: str, company: str, title: str) -> str:
    from spejder.jobs.deduplication import _normalize_company_key, _normalize_title_key
    norm_comp = _normalize_company_key(company)
    norm_title = _normalize_title_key(title)
    if "jobindex" in (source or "").lower():
        prefix = "ji"
    else:
        prefix = "li"
    return f"{prefix}:{norm_comp}:{norm_title}"
