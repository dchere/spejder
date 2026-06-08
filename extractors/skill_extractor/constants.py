"""Shared constants for skill extraction heuristics."""

import re

SKILL_CUE_PATTERN = re.compile(
    r"(?i)(?:skills?|requirements?|qualifications?|you have|"
    r"your profile|your background|about you|we expect|what you bring|"
    r"who you are|we are looking for|you bring)"
)

SKILL_CLEANUP_GENERIC_SINGLE = re.compile(r"^([a-z])\1*$")

# TODO: populate from profile or curated lists when cleanup heuristics are expanded.
SKILL_CLEANUP_GENERIC_PHRASES: set[str] = set()
SKILL_CLEANUP_STOPWORDS: set[str] = set()
SKILL_CLEANUP_PREFIXES: tuple[str, ...] = ()
