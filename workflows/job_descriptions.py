import re
import time
from typing import Optional

from spejder.config import AppConfig
from spejder.db import get_jobs_for_description_refresh, set_job_description
from spejder.llm import LocalLLM
from spejder.managers.language_manager import (
    get_title_english_for_row as _get_title_english_for_row,
)
from spejder.parsers.web_parser import _get_position_page_context
from spejder.workflows.job_text_enrichment import _enrich_raw_text_with_position_page
from spejder.workflows.text_prepend import _is_invalid_summary_text


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
                    title_translation_cache=title_translation_cache,
                ),
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


def _summary_for_display(summary: str, raw_text: str, max_chars: int = 260) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    if summary_clean and not _is_invalid_summary_text(summary_clean):
        return summary_clean[:max_chars]
    return " ".join((raw_text or "").split())[:max_chars]


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
        except (OSError, RuntimeError, ValueError, TypeError):
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
