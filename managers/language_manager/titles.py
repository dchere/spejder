
import logging
import re
from collections import Counter
from typing import Optional

from spejder.core import AppConfig
from spejder.db import set_job_title_english
from spejder.db.utils import TITLE_GARBAGE_MARKERS
from spejder.llm import LocalLLM

from .detection import translation_source_language
from .engines import get_translation_runtime
from .text_translation import translate_text_to_english_if_needed

logger = logging.getLogger(__name__)


def normalize_title_text(text: str) -> str:
    t_clean = str(text or "").strip()
    low = t_clean.lower()
    for marker in TITLE_GARBAGE_MARKERS:
        if low.startswith(marker):
            t_clean = t_clean[len(marker) :].lstrip(" :").strip()
            low = t_clean.lower()
    t_clean = re.sub(r"\s+", " ", t_clean)
    return t_clean.strip()


def clean_translated_title_output(text: str) -> str:
    cleaned = " ".join((text or "").replace("```", " ").split()).strip()
    if not cleaned:
        return ""

    cleaned = re.sub(r"^english\s+title\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^translated\s+title(?:\s+text)?\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" \"'`[]()")

    marker_pattern = re.compile(
        r"\b(?:translated\s+title|translated\s+title\s+text|original\s+title|original\s+text|english\s+title|english\s+translation|translation\s+result|return\s+value|return\s+only|unchanged\s+title|step\s+1|note:|you\s+are\s+an\s+ai\s+assistant|translate\s+this\s+job\s+title\s+to\s+english|this\s+translation|translation\s+conveys|the\s+translated\s+title|is\s+already\s+in\s+english)\b",
        flags=re.IGNORECASE,
    )
    match = marker_pattern.search(cleaned)
    if match:
        cleaned = cleaned[: match.start()].rstrip(" -|:;,/")

    while cleaned.count("(") > cleaned.count(")") and "(" in cleaned:
        cleaned = cleaned.rsplit("(", 1)[0].rstrip(" -|:;,/")

    return cleaned.strip(" \"'`[]()").rstrip(" -|:;,/")[:180]


def is_plausible_translated_title(candidate: str, original: str) -> bool:
    text = " ".join((candidate or "").split()).strip()
    base = " ".join((original or "").split()).strip()
    if not text:
        return False
    if len(text) > max(80, int(len(base) * 2.2)):
        return False

    low = text.lower()
    bad_fragments = [
        "translation",
        "translated title",
        "original title",
        "you are an ai assistant",
        "return only",
        "step 1",
        "this title",
        "in english",
    ]
    if any(fragment in low for fragment in bad_fragments):
        return False

    words = re.findall(r"[a-zA-Z0-9+#.&/-]+", text)
    if len(words) > 14:
        return False
    if len(words) >= 6:
        counts = Counter(word.lower() for word in words)
        most_common = counts.most_common(1)[0][1] if counts else 0
        if most_common >= max(4, len(words) // 2):
            return False
        if len(counts) <= 2:
            return False
    if text.count(".") >= 2:
        return False
    return True


def normalize_title_compare_key(text: str) -> str:
    compact = " ".join((text or "").split()).strip().lower()
    compact = re.sub(r"[^\w]+", "", compact, flags=re.UNICODE)
    return compact


def translate_title_to_english(
    title: str,
    llm: Optional[LocalLLM] = None,
    runtime_profile: Optional[AppConfig] = None,
    title_translation_cache: Optional[dict] = None,
) -> str:
    title_clean = normalize_title_text(title)
    if not title_clean:
        return ""

    cache_key = title_clean.lower()
    if title_translation_cache is not None and cache_key in title_translation_cache:
        return str(title_translation_cache.get(cache_key, title_clean) or title_clean)

    result = title_clean
    source_lang = translation_source_language(title_clean, runtime_profile=runtime_profile)
    if source_lang is not None:
        runtime = get_translation_runtime(runtime_profile, source_lang=source_lang)
        if runtime is None:
            logger.warning(
                "title translation model is not available for source language %s; keeping original title",
                source_lang,
            )
            if title_translation_cache is not None:
                title_translation_cache[cache_key] = result
            return result
        tokenizer, model, device = runtime
        try:
            encoded = tokenizer(title_clean, return_tensors="pt", truncation=True)
            encoded = {
                key: value.to(device) if hasattr(value, "to") else value
                for key, value in encoded.items()
            }
            generated = model.generate(**encoded, max_new_tokens=64, num_beams=4)
            english = tokenizer.batch_decode(generated, skip_special_tokens=True)[0]
            english = clean_translated_title_output(english)
        except Exception as exc:
            raise RuntimeError(f"title translation execution failed: {exc}") from exc

        if not english:
            raise RuntimeError("title translation returned empty text")
        if not is_plausible_translated_title(english, title_clean):
            raise RuntimeError(f"title translation returned implausible text: {english}")
        result = english

    if title_translation_cache is not None:
        title_translation_cache[cache_key] = result
    return result


def finalize_title_english(candidate: str, original: str) -> str:
    original_clean = normalize_title_text(original)
    candidate_clean = normalize_title_text(candidate)
    if not original_clean:
        return candidate_clean
    if not candidate_clean:
        return original_clean
    if normalize_title_compare_key(candidate_clean) == normalize_title_compare_key(original_clean):
        return original_clean
    if not is_plausible_translated_title(candidate_clean, original_clean):
        return original_clean
    return candidate_clean


def get_title_english_for_row(
    db_path: str,
    row: dict,
    runtime_profile: Optional[AppConfig] = None,
    title_translation_cache: Optional[dict] = None,
) -> str:
    title_clean = normalize_title_text(row.get("title", ""))
    if not title_clean:
        row["title_english"] = ""
        return ""

    row_id = int(row.get("id", 0) or 0)
    stored = normalize_title_text(row.get("title_english", ""))
    if stored:
        stored_final = finalize_title_english(stored, title_clean)
        # Reuse only plausible cached English titles; otherwise recompute.
        if normalize_title_compare_key(stored_final) != normalize_title_compare_key(title_clean):
            row["title_english"] = stored_final
            if row_id > 0 and stored_final != stored:
                set_job_title_english(db_path, row_id, stored_final)
            return stored_final
        row["title_english"] = ""
        if row_id > 0:
            set_job_title_english(db_path, row_id, "")

    try:
        title_english = translate_title_to_english(
            title_clean,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        )
    except RuntimeError:
        try:
            title_english = translate_text_to_english_if_needed(
                title_clean,
                runtime_profile=runtime_profile,
            )
        except RuntimeError:
            title_english = title_clean

    title_english = finalize_title_english(title_english, title_clean)
    persisted_title_english = title_english
    if normalize_title_compare_key(persisted_title_english) == normalize_title_compare_key(
        title_clean
    ):
        persisted_title_english = ""
    row["title_english"] = persisted_title_english or title_english

    if row_id > 0:
        set_job_title_english(db_path, row_id, persisted_title_english)
    return row["title_english"]
