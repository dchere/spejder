import re
import time
from typing import Callable, Optional

from spejder.config import AppConfig
from spejder.db import (
    get_applied_jobs,
    get_jobs_by_category,
    get_jobs_for_description_refresh,
    set_job_description,
)
from spejder.extractors.skill_extractor import (
    _get_or_extract_job_skills,
)
from spejder.llm import LocalLLM
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
from spejder.parsers.web_parser import _append_page_context_to_raw_text, _get_position_page_context
from spejder.workflows.formatting import _prepend_summary_to_raw_text, _prepend_title_to_raw_text


def make_translate_job_entry_for_storage(
    runtime_profile: AppConfig,
    text_translation_cache: dict[str, str],
    title_translation_cache: dict[str, str],
) -> Callable[[dict], dict]:
    def _translate(entry: dict) -> dict:
        entry = dict(entry)
        entry["raw_text"] = _translate_text_to_english_if_needed(
            str(entry.get("raw_text", "") or ""),
            runtime_profile=runtime_profile,
            translation_cache=text_translation_cache,
        )
        title_value = str(entry.get("title", "") or "")
        try:
            title_english = _translate_title_to_english(
                title_value,
                runtime_profile=runtime_profile,
                title_translation_cache=title_translation_cache,
            )
        except Exception:
            try:
                title_english = _translate_text_to_english_if_needed(
                    title_value,
                    runtime_profile=runtime_profile,
                    translation_cache=text_translation_cache,
                )
            except Exception:
                title_english = title_value
        final_title_english = _finalize_title_english(title_english, title_value)
        if _normalize_title_compare_key(final_title_english) == _normalize_title_compare_key(title_value):
            final_title_english = ""
        entry["title_english"] = final_title_english
        return entry

    return _translate


def _generate_missing_descriptions_for_ingest(
    db_path: str,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    allow_empty: bool = False,
    progress: bool = False,
    progress_label: str = "Description generation",
) -> tuple[int, int]:
    rows = get_jobs_for_description_refresh(
        db_path,
        category="",
        source="",
        links=[],
        job_ids=[],
        limit=0,
        missing_only=True,
        unviewed_only=True,
    )

    updated = 0
    skipped = 0
    total_rows = len(rows)
    started_at = time.monotonic()

    def _fmt_eta(seconds: float) -> str:
        seconds = max(0, int(seconds))
        mins, secs = divmod(seconds, 60)
        hrs, mins = divmod(mins, 60)
        if hrs > 0:
            return f"{hrs}h {mins}m {secs}s"
        if mins > 0:
            return f"{mins}m {secs}s"
        return f"{secs}s"

    if progress:
        print(f"{progress_label}: starting ({total_rows} items)")

    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    for idx, row in enumerate(rows, start=1):
        if progress:
            elapsed = time.monotonic() - started_at
            avg_per_item = elapsed / max(1, idx - 1)
            remaining = max(0, total_rows - idx + 1)
            eta_sec = avg_per_item * remaining if idx > 1 else 0
            print(
                f"{progress_label}: {idx}/{total_rows} "
                f"(updated={updated}, skipped={skipped}, elapsed={_fmt_eta(elapsed)}, eta={_fmt_eta(eta_sec)})"
            )

        source_raw = row.get("raw_text", "") or ""
        raw = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            runtime_profile=runtime_profile,
        )
        if not raw:
            skipped += 1
            continue

        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            runtime_profile=runtime_profile,
            page_context_cache=page_context_cache,
        )
        if (
            not description
            or _has_invalid_description_marker(description)
            or _is_low_quality_description(
                description,
                raw_text=raw,
                title=_get_title_english_for_row(
                    db_path,
                    row,
                    runtime_profile=runtime_profile,
                    title_translation_cache=title_translation_cache
                )
            )
        ):
            description = _fallback_description_text("", source_raw or raw)
        if not description and not allow_empty:
            skipped += 1
            continue

        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    if progress:
        total_elapsed = time.monotonic() - started_at
        print(
            f"{progress_label}: done (updated={updated}, skipped={skipped}, elapsed={_fmt_eta(total_elapsed)})"
        )

    return updated, skipped


def _is_invalid_summary_text(text: str) -> bool:
    low = " ".join((text or "").split()).strip().lower()
    if not low:
        return True
    bad_markers = [
        "llm summary failed",
        "model path does not exist",
        "traceback",
        "exception:",
    ]
    return any(marker in low for marker in bad_markers)


def _summary_for_display(summary: str, raw_text: str, max_chars: int = 260) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    if summary_clean and not _is_invalid_summary_text(summary_clean):
        return summary_clean[:max_chars]
    return " ".join((raw_text or "").split())[:max_chars]


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
        title_translation_cache=title_translation_cache
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


def materialize_job_skills(
    db_path: str,
    row: dict,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    page_context_cache: Optional[dict] = None,
    title_translation_cache: Optional[dict] = None,
    limit: int = 10,
    rescore: bool = False,
) -> tuple[str, str]:
    """Enrich job text, extract skills, persist to job_skills. Returns (skills_text, enriched_raw)."""
    job_id = int(row.get("id", 0) or 0)
    raw_text = _enrich_raw_text_with_position_page(
        db_path,
        row,
        page_context_cache=page_context_cache,
        llm=llm,
        runtime_profile=runtime_profile,
        title_translation_cache=title_translation_cache,
    )
    skills_text = _get_or_extract_job_skills(
        db_path,
        job_id,
        raw_text,
        llm=llm,
        profile=runtime_profile,
        position_link=row.get("position_link", ""),
        page_context_cache=page_context_cache,
        limit=limit,
    )
    if rescore and job_id and runtime_profile is not None and skills_text:
        from spejder.jobs.scoring import rescore_job_by_id

        rescore_job_by_id(db_path, runtime_profile, job_id)
    return skills_text, raw_text


def materialize_jobs_skills(
    db_path: str,
    rows: list[dict],
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    limit: int = 10,
    rescore: bool = False,
    skip_cached: bool = False,
    progress_label: str = "",
) -> int:
    """Materialize skills for multiple jobs. Returns count of jobs that received skills."""
    if not rows:
        return 0

    from spejder.db import get_job_skills

    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    updated = 0
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        job_id = int(row.get("id", 0) or 0)
        if not job_id:
            continue
        if skip_cached and get_job_skills(db_path, job_id):
            continue

        skills_text, _ = materialize_job_skills(
            db_path,
            row,
            llm=llm,
            runtime_profile=runtime_profile,
            page_context_cache=page_context_cache,
            title_translation_cache=title_translation_cache,
            limit=limit,
            rescore=rescore,
        )
        if skills_text:
            updated += 1
        if progress_label and (idx % 25 == 0 or idx == total):
            print(f"{progress_label}: checked={idx}/{total}, updated={updated}")
    return updated


def _collect_relevant_and_applied_rows(db_path: str) -> list[dict]:
    rows: list[dict] = []
    seen_ids: set[int] = set()
    for row in get_applied_jobs(db_path, limit=0):
        rid = int(row.get("id", 0) or 0)
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            rows.append(row)
    for cat in ("relevant", "not relevant"):
        for row in get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True):
            rid = int(row.get("id", 0) or 0)
            if rid and rid not in seen_ids:
                seen_ids.add(rid)
                rows.append(row)
    return rows


def materialize_relevant_and_applied_skills(
    db_path: str,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    limit: int = 10,
    rescore: bool = True,
    skip_cached: bool = True,
    progress_label: str = "Skill materialization",
) -> int:
    """Phase-2 batch: enrich, extract, persist, and optionally rescore scoped jobs."""
    rows = _collect_relevant_and_applied_rows(db_path)
    if progress_label:
        print(f"{progress_label}: starting (jobs={len(rows)})")
    updated = materialize_jobs_skills(
        db_path,
        rows,
        llm=llm,
        runtime_profile=runtime_profile,
        limit=limit,
        rescore=rescore,
        skip_cached=skip_cached,
        progress_label=progress_label,
    )
    if progress_label:
        print(f"{progress_label}: done (updated={updated})")
    return updated


def _build_description_summary(
    raw_text: str,
    llm: LocalLLM = None,
    position_link: str = "",
    runtime_profile: Optional[AppConfig] = None,
    page_context_cache: Optional[dict] = None,
) -> str:
    cleaned = " ".join((raw_text or "").split())

    def clean_model_output(text: str) -> str:
        out = text or ""
        out = out.replace("```", " ")
        if re.search(r"\bto be concise\s*:", out, flags=re.IGNORECASE):
            out = re.split(r"\bto be concise\s*:\s*", out, maxsplit=1, flags=re.IGNORECASE)[-1]
        out = re.sub(r"\bplaintext\b", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\bsummary\s*:\s*", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def remove_repeated_phrases(text: str) -> str:
        words = text.split()
        if len(words) < 8:
            return text

        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[:size] == words[size : size * 2]:
                words = words[:size] + words[size * 2 :]
                break

        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[-size:] == words[-size * 2 : -size]:
                words = words[:-size]
                break

        return " ".join(words)

    if llm:
        page_context = _get_position_page_context(
            position_link,
            runtime_profile=runtime_profile,
            page_context_cache=page_context_cache,
        )
        if not cleaned and not page_context:
            return ""

        page_block = (
            f"Position page context (if useful):\n{page_context}\n\n" if page_context else ""
        )
        prompt = (
            "Summarize this job description in English. "
            "Keep only key responsibilities, key requirements, and main purpose, no general information and common words. "
            "Use the Description as primary truth; use page context only to refine missing details.\n\n"
            "Do not include job title, company name, location, or other metadata in the summary unless they are explicitly explain the position. "
            f"Description:\n{cleaned}\n\n"
            f"{page_block}"
            "Summary:"
        )
        try:
            out = llm.generate(prompt, max_tokens=1024)
            return remove_repeated_phrases(clean_model_output(out))
        except Exception:
            pass

    return ""


def _has_invalid_description_marker(text: str) -> bool:
    return "[POSITION_PAGE_CONTEXT" in (text or "").upper()


def _is_low_quality_description(
    description: str,
    raw_text: str = "",
    title: str = "",
) -> bool:
    desc = " ".join((description or "").split()).strip()
    if not desc:
        return False

    low = desc.lower()
    bad_markers = [
        "you are an ai assistant",
        "translated title",
        "original title",
        "english title",
        "return only",
        "step 1",
        "llm summary failed",
        "model path does not exist",
    ]
    if any(marker in low for marker in bad_markers):
        return True

    # Word spam / repeated fragments are usually model failure artifacts.
    if re.search(r"\b(\w+)(?:\s+\1){4,}\b", low):
        return True

    words = re.findall(r"[a-zA-Z0-9+#.-]+", low)
    if len(words) >= 45:
        unique_ratio = len(set(words)) / max(1, len(words))
        if unique_ratio < 0.42:
            return True

    raw_clean = " ".join((raw_text or "").split()).strip()
    if raw_clean and len(raw_clean) < 240 and len(desc) > 360:
        return True

    title_tokens = [
        token
        for token in re.findall(r"[a-zA-Z0-9+#.-]+", (title or "").lower())
        if len(token) > 2
    ]
    if title_tokens and len(words) >= 30:
        repeats = sum(low.count(token) for token in set(title_tokens))
        if repeats > max(6, len(words) // 4):
            return True

    return False


def _fallback_description_text(description: str, raw_text: str, max_chars: int = 280) -> str:
    if (description or "").strip() and not _is_invalid_summary_text(description):
        return description
    compact = " ".join((raw_text or "").split())
    # Remove legacy summary-failure artifacts that may be persisted in raw text.
    compact = re.sub(
        r"\bSummary\s*:\s*LLM\s+summary\s+failed\s*:[^.\n]*(?:\.|$)",
        " ",
        compact,
        flags=re.IGNORECASE,
    )
    compact = re.sub(
        r"\bLLM\s+summary\s+failed\s*:[^.\n]*(?:\.|$)",
        " ",
        compact,
        flags=re.IGNORECASE,
    )
    compact = re.sub(
        r"\bModel\s+path\s+does\s+not\s+exist\s*:[^.\n]*(?:\.|$)",
        " ",
        compact,
        flags=re.IGNORECASE,
    )
    compact = re.sub(r"\[POSITION_PAGE_CONTEXT[^\]]*\]", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\(\s*settings\s*\)", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bPUBLISHED\s*:\s*\d{1,2}-\d{1,2}-\d{4}\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bRetrieved\s+from\s+Jobcenter\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bCheck\s+the\s+job\s+satisfaction\s*:\s*[^.\n]*", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\b\d{1,3}(?:,\d{3})*\s+ratings\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bSave\s+job\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bView\s+job\b", " ", compact, flags=re.IGNORECASE)
    compact = " ".join(compact.split())
    if not compact:
        return ""

    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars].rstrip() + "..."


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
            title_translation_cache=title_translation_cache
        ),
    }


def _is_linkedin_item(source: str, position_link: str) -> bool:
    source_low = (source or "").strip().lower()
    link_low = (position_link or "").strip().lower()
    return source_low == "linkedin" or "linkedin.com/" in link_low


def _has_easy_apply_signal(*parts: str) -> bool:
    compact = " ".join(" ".join((part or "").split()) for part in parts if part)
    return bool(compact and _EASY_APPLY_REGEX.search(compact))


def _is_easy_apply_item(item: dict) -> bool:
    source = str(item.get("source", ""))
    position_link = str(item.get("position_link", ""))
    if not _is_linkedin_item(source, position_link):
        return False
    reason = str(item.get("relevance_reason", ""))
    if re.search(r"\beasy_apply\s*=\s*true\b", reason, flags=re.IGNORECASE):
        return True
    return _has_easy_apply_signal(
        str(item.get("title", "")),
        str(item.get("summary", "")),
        str(item.get("description", "")),
        str(item.get("raw_text", "")),
    )

_EASY_APPLY_REGEX = re.compile(r'(?i)easy\s*apply|apply\s*with\s*linkedin')
