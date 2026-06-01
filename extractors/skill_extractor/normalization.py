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

def _normalize_skill_name(skill: str) -> str:
    s = (skill or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"^[-*\d.)\s]+", "", s)
    s = re.sub(r"^(?:a|an|as|at|you|but)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^you\s+(?:will|can|have|are|should|must)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(?:good|great|strong|solid|excellent|proven|quality|high\s+quality)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:degree|bachelor(?:'s)?|master(?:'s)?|phd|doctorate)"
        r"\s+in\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:experience\s+with|experienced\s+with|hands-?on\s+with|"
        r"knowledge\s+of|familiarity\s+with)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(
        r"\b(?:required|required:|requirements?|qualifications?|must have|nice to have)\b",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip(" ,.;:-")
    if not s:
        return ""
    if len(s.split()) > 5:
        return ""
    if len(s) < 2:
        return ""
    return s

