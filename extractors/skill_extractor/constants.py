"""Shared constants for skill extraction heuristics."""

import re

SKILL_CUE_PATTERN = re.compile(
    r"(?i)(?:skills?|requirements?|qualifications?|you have|"
    r"your profile|your background|about you|we expect|what you bring|"
    r"who you are|we are looking for|you bring)"
)

# Repeated single-letter tokens (e.g. "aa") — structural CLI cleanup only.
SKILL_CLEANUP_GENERIC_SINGLE = re.compile(r"^([a-z])\1*$")
