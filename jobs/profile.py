# pylint: disable=all
from spejder.db import *
from spejder.db import _provider_from_link, _normalize_position_link
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
def load_profile(profile_path: Optional[str]) -> AppConfig:
    from spejder.config import AppConfig
    profile = AppConfig.load(profile_path)

    base_include = profile.include_keywords or []
    base_exclude = profile.exclude_keywords or []
    learned_include = profile.learned_include_keywords or []
    learned_exclude = profile.learned_exclude_keywords or []

    profile.include_keywords = _unique_keywords(
        list(base_include) + list(learned_include)
    )
    profile.exclude_keywords = _unique_keywords(
        list(base_exclude) + list(learned_exclude)
    )
    profile.learned_include_keywords = _unique_keywords(
        list(learned_include))
    profile.learned_exclude_keywords = _unique_keywords(
        list(learned_exclude))
    profile.user_skills = _unique_keywords(
        list(profile.user_skills or [])
    )
    profile.blocked_skills = _unique_keywords(
        list(profile.blocked_skills or [])
    )
    profile.missing_skills_suggestions = _unique_keywords(
        list(profile.missing_skills_suggestions)
    )
    return profile



def update_profile_from_db_signals(
    db_path: str, profile_path: str, max_keywords: int = 20
) -> dict[str, int]:
    from spejder.config import AppConfig
    profile = AppConfig.load(profile_path)

    learned_include, learned_exclude, labeled_count = (
        _suggest_keywords_from_labeled_jobs(db_path, max_keywords=max_keywords)
    )
    profile.learned_include_keywords = learned_include
    profile.learned_exclude_keywords = learned_exclude

    max_missing_items = profile.missing_skills_max_items
    missing_skills = _suggest_missing_skills_from_applied_jobs(
        db_path, profile, max_items=max_missing_items
    )
    profile.missing_skills_suggestions = missing_skills

    profile.save(profile_path)

    return {
        "labeled_count": int(labeled_count),
        "learned_include_count": len(learned_include),
        "learned_exclude_count": len(learned_exclude),
        "missing_skills_count": len(missing_skills),
    }



