# pylint: disable=all
from spejder.db import *
from spejder.db.utils import _normalize_skill_name_key
from spejder.db import _provider_from_link, _normalize_position_link
from spejder.extractors.skill_extractor import _blocked_skill_keys
import re
import json
import base64
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from collections.abc import Callable
from typing import Optional
from html import unescape
from bs4 import BeautifulSoup
from collections import Counter
from spejder.config import AppConfig

COMPANY_NOISE_TOKENS = {'danmark', 'denmark', 'aps', 'a', 's', 'as', 'ab', 'oy', 'ltd', 'llc', 'inc', 'group', 'holding'}
LEARNING_STOPWORDS = {'about', 'above', 'after', 'again', 'against', 'all', 'also', 'and', 'any', 'are', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'can', 'company', 'could', 'danish', 'denmark', 'developer', 'email', 'for', 'from', 'have', 'into', 'job', 'jobs', 'just', 'more', 'not', 'our', 'out', 'position', 'role', 'than', 'that', 'the', 'their', 'them', 'there', 'these', 'this', 'those', 'through', 'under', 'using', 'very', 'want', 'when', 'where', 'which', 'with', 'you', 'your'}
EASY_APPLY_PATTERN = re.compile(r'\beasy\s*apply\b', flags=re.IGNORECASE)
def _suggest_keywords_from_labeled_jobs(
    db_path: str, max_keywords: int = 20
) -> tuple[list[str], list[str], int]:
    rows = get_jobs_for_keyword_suggestions(db_path)

    relevant_docs = 0
    not_relevant_docs = 0
    relevant_df = Counter()
    not_relevant_df = Counter()

    for category, title, company, place, work_type, raw_text in rows:
        text = "\n".join(
            [
                title or "",
                company or "",
                place or "",
                work_type or "",
                raw_text or "",
            ]
        )
        tokens = set(_tokenize_learning_text(text))
        if not tokens:
            continue
        if category == "relevant":
            relevant_docs += 1
            relevant_df.update(tokens)
        elif category == "not relevant":
            not_relevant_docs += 1
            not_relevant_df.update(tokens)

    total_labeled = relevant_docs + not_relevant_docs
    if relevant_docs == 0 or not_relevant_docs == 0:
        return [], [], total_labeled

    candidates = set(relevant_df.keys()) | set(not_relevant_df.keys())
    include_ranked = []
    exclude_ranked = []

    for token in candidates:
        rel_df = relevant_df[token]
        nrel_df = not_relevant_df[token]
        if rel_df + nrel_df < 2:
            continue

        rel_rate = rel_df / max(1, relevant_docs)
        nrel_rate = nrel_df / max(1, not_relevant_docs)
        delta = rel_rate - nrel_rate

        if delta >= 0.2 and rel_df >= 2:
            include_ranked.append((delta, rel_df, token))
        elif delta <= -0.2 and nrel_df >= 2:
            exclude_ranked.append((abs(delta), nrel_df, token))

    include_ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    exclude_ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)

    learned_include = [token for _, _, token in include_ranked[:max_keywords]]
    learned_exclude = [
        token
        for _, _, token in exclude_ranked[:max_keywords]
        if token not in learned_include
    ]
    return learned_include, learned_exclude, total_labeled



def _suggest_missing_skills_from_applied_jobs(
    db_path: str, profile: AppConfig, max_items: int = 25
) -> list[str]:
    rows = get_jobs_for_skill_suggestions(db_path)

    if not rows:
        return []

    skill_patterns = _profile_skill_patterns(profile)
    if not skill_patterns:
        return []

    user_skills = {
        _normalize_skill_name_key(s)
        for s in (profile.user_skills or [])
        if _normalize_skill_name_key(str(s))
    }
    blocked_skills = _blocked_skill_keys(profile)

    freq: Counter = Counter()
    display_by_key: dict[str, str] = {}
    for title, summary, raw_text in rows:
        text = "\n".join([(title or ""), (summary or ""), (raw_text or "")])
        skills = _extract_job_skills(text, skill_patterns)
        for skill in skills:
            key = _normalize_skill_name_key(skill)
            if not key or key in user_skills or key in blocked_skills:
                continue
            display_by_key.setdefault(key, skill)
            freq[key] += 1

    ordered = [
        display_by_key.get(name, name) for name, _ in freq.most_common(max_items)
    ]
    return ordered[:max_items]






def _unique_keywords(items: list) -> list[str]:
    seen = set()
    out = []
    for x in items:
        cleaned = str(x).strip()
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            out.append(cleaned)
    return out


