from .normalization import _normalize_skill_name
# pylint: disable=all
"""
Skill extractor for parsing...
"""
import json
import os
import re
from collections import Counter
from contextlib import suppress
from typing import Optional

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH, load_profile, load_runtime_profile
from spejder.db import (
    ensure_db,
    delete_skill_from_db,
    get_job_skills,
    set_job_skills,
    get_skill_patterns as get_db_skill_patterns,
    get_applied_jobs,
    get_jobs_by_category,
    upsert_skill_pattern,
    migrate_profile_skill_patterns_to_db
)
from spejder.llm import LocalLLM
from spejder.managers.language_manager import translate_text_to_english_if_needed as _translate_text_to_english_if_needed
from spejder.managers.profile_manager import _save_profile, _block_skill_in_profile
from spejder.parsers.cv_parser import load_cv_text

SKILL_CLEANUP_GENERIC_PHRASES = set()
SKILL_CLEANUP_STOPWORDS = set()
SKILL_CLEANUP_PREFIXES = ()

def _split_skills_from_text(text: str) -> list[str]:
    compact = (text or "").replace("\n", ",")
    compact = re.sub(r"[;|/]+", ",", compact)
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    out = []
    seen = set()
    for part in parts:
        item = _normalize_skill_name(part)
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _format_skills(skills: list[str], limit: int = 10) -> str:
    compact = []
    seen = set()
    for skill in skills:
        normalized = _normalize_skill_name(skill)
        key = normalized.lower()
        if normalized and key not in seen:
            seen.add(key)
            compact.append(normalized)
        if len(compact) >= limit:
            break
    return ", ".join(compact)


def _clean_model_output(text: str) -> str:
    out = text or ""
    out = out.replace("```", " ")
    out = re.sub(r"\bskills?\s*:\s*", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\boutput\s*:\s*", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\bplaintext\b", " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s*[-*]\s*", ", ", out)
    out = re.sub(r"\s*\d+[.)]\s*", ", ", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _extract_json_object(text: str) -> dict:
    payload = (text or "").strip()
    if not payload:
        return {}
    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    candidate = payload[start : end + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def _to_items(value) -> list[dict]:
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        if isinstance(item, str):
            out.append({"name": item, "confidence": 1.0, "evidence": ""})
        elif isinstance(item, dict):
            out.append(item)
    return out

