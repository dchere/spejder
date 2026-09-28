"""Sense-disambiguation gates for ambiguous short skill tokens.

Keeps folded ``name_key`` identity. Extra evidence is checked against the
original (case-preserving) job text for a small allowlist only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .constants import SKILL_CUE_PATTERN
from .normalization import _normalize_skill_name


@dataclass(frozen=True)
class _ShortTokenRule:
    """Evidence rules for one ambiguous short skill key (folded)."""

    # Unambiguous aliases — any hit accepts the skill.
    aliases: tuple[re.Pattern[str], ...]
    # Bare token matcher on original text (may be case-sensitive).
    bare_token: re.Pattern[str]
    # Window around a bare hit that supports the skill sense.
    positive_collocates: re.Pattern[str]
    # Window / local context that marks a function-word use.
    negative_context: re.Pattern[str] | None = None
    # Optional: accept capital/all-caps bare form when not negative.
    capital_bare: re.Pattern[str] | None = None
    # When False, a nearby skills/requirements cue alone is not enough
    # (used for pronoun-heavy tokens like ``it``).
    cue_alone_ok: bool = True
    # When False, capital/all-caps bare form still needs collocates/aliases.
    capital_alone_ok: bool = True


_CUE_WINDOW = 200

# Programming / tech collocates shared by several short language tokens.
_LANG_COLLOCATES = re.compile(
    r"(?i)\b(?:"
    r"golang|programming|programmer|language|languages|developer|development|"
    r"backend|back-end|frontend|front-end|fullstack|full-stack|software|"
    r"engineer|engineering|stack|code|coding|compiler|runtime|gopher|"
    r"python|java|rust|kotlin|swift|typescript|javascript|ruby|scala|"
    r"microservices|concurrency|goroutine"
    r")\b"
)

_GO_NEGATIVE = re.compile(
    r"(?i)(?:"
    r"\b(?:to|will|can|must|should|would|could|may|might|let'?s|we|they|you|"
    r"i|he|she|please|don'?t|do not)\s+go\b|"
    r"\bgo\s+(?:to|above|beyond|ahead|home|back|through|into|with|for|on|"
    r"live|away|out|over|under|after|before|around|along)\b|"
    r"\bon\s+the\s+go\b|"
    r"\bready\s+to\s+go\b|"
    r"\bgo\s+get(?:ting)?\b"
    r")"
)

_IT_COLLOCATES = re.compile(
    r"(?i)\b(?:"
    r"support|department|services|infrastructure|operations|systems|"
    r"security|network|helpdesk|help\s*desk|administrator|admin|"
    r"specialist|technician|consultant|manager|engineer|engineering|"
    r"software|hardware|desktop|service\s+desk|cyber|cloud|devops"
    r")\b"
)

_IT_NEGATIVE = re.compile(
    r"(?i)(?:"
    r"\bit\s+(?:is|was|has|had|will|can|should|does|did|would|could|may|"
    r"might|seems|looks|feels|means|takes|comes|goes|needs|requires)\b|"
    r"\b(?:and|but|as|if|when|that|which|because|while|although|unless|"
    r"where|whether)\s+it\b|"
    r"\b(?:make|makes|making|find|finds|keep|keeps|get|gets|give|gives|"
    r"let|lets)\s+it\b|"
    r"\bit\s+(?:all|yourself|themselves)\b"
    r")"
)

_AI_COLLOCATES = re.compile(
    r"(?i)\b(?:"
    r"artificial\s+intelligence|machine\s+learning|ml|llm|gpt|model|models|"
    r"neural|deep\s+learning|nlp|computer\s+vision|prompt|generative|"
    r"openai|langchain|inference|training|data\s+science"
    r")\b"
)

_ML_COLLOCATES = re.compile(
    r"(?i)\b(?:"
    r"machine\s+learning|artificial\s+intelligence|ai|deep\s+learning|"
    r"model|models|neural|sklearn|scikit|tensorflow|pytorch|xgboost|"
    r"data\s+science|nlp|inference|training|supervised|unsupervised"
    r")\b"
)

_SHORT_TOKEN_RULES: dict[str, _ShortTokenRule] = {
    "go": _ShortTokenRule(
        aliases=(re.compile(r"(?i)\bgolang\b"),),
        bare_token=re.compile(r"(?i)\bgo\b"),
        positive_collocates=_LANG_COLLOCATES,
        negative_context=_GO_NEGATIVE,
        capital_bare=re.compile(r"\b(?:Go|GO)\b"),
    ),
    "it": _ShortTokenRule(
        aliases=(
            re.compile(r"(?i)\binformation\s+technology\b"),
            re.compile(r"(?i)\bi\.\s*t\.\b"),
        ),
        bare_token=re.compile(r"(?i)\bit\b"),
        positive_collocates=_IT_COLLOCATES,
        negative_context=_IT_NEGATIVE,
        # Prefer all-caps IT as skill evidence; mixed "It" is usually English.
        capital_bare=re.compile(r"\bIT\b"),
        cue_alone_ok=False,
        capital_alone_ok=False,
    ),
    "ai": _ShortTokenRule(
        aliases=(re.compile(r"(?i)\bartificial\s+intelligence\b"),),
        bare_token=re.compile(r"(?i)\bai\b"),
        positive_collocates=_AI_COLLOCATES,
        capital_bare=re.compile(r"\b(?:AI|Ai)\b"),
    ),
    "ml": _ShortTokenRule(
        aliases=(re.compile(r"(?i)\bmachine\s+learning\b"),),
        bare_token=re.compile(r"(?i)\bml\b"),
        positive_collocates=_ML_COLLOCATES,
        capital_bare=re.compile(r"\b(?:ML|Ml)\b"),
    ),
}

SHORT_AMBIGUOUS_SKILL_KEYS: frozenset[str] = frozenset(_SHORT_TOKEN_RULES)


def _window_around(text: str, start: int, end: int, radius: int = _CUE_WINDOW) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    return text[left:right]


def _bare_hit_supported(rule: _ShortTokenRule, source: str, match: re.Match[str]) -> bool:
    """Return True when one bare-token occurrence looks like the skill sense."""
    start, end = match.start(), match.end()
    # Local negative check uses a tighter span so distant verb uses don't poison
    # a later skill-list hit in the same JD.
    local = _window_around(source, start, end, radius=40)
    if rule.negative_context is not None and rule.negative_context.search(local):
        return False

    window = _window_around(source, start, end)
    if rule.positive_collocates.search(window):
        return True
    capital_hit = (
        rule.capital_bare is not None and rule.capital_bare.search(match.group(0))
    )
    if capital_hit and rule.capital_alone_ok:
        # Capital form alone is weak but useful when collocates are absent
        # (e.g. terse "Go, Rust" lists).
        return True
    if rule.cue_alone_ok and SKILL_CUE_PATTERN.search(window):
        return True
    # Stricter tokens (e.g. IT): capital form + skill cue, still no collocates.
    if (
        capital_hit
        and not rule.capital_alone_ok
        and SKILL_CUE_PATTERN.search(window)
    ):
        return True
    return False


def _short_token_evidence_ok(skill_name: str, source_text: str) -> bool:
    """Return True if ``skill_name`` may be emitted for ``source_text``.

    Non-allowlisted skills always pass. Allowlisted short tokens need an
    unambiguous alias and/or a bare hit with collocate, skill-cue, or capital
    evidence — and not only function-word uses.
    """
    key = _normalize_skill_name(skill_name).lower()
    if not key or key not in _SHORT_TOKEN_RULES:
        return True

    source = " ".join((source_text or "").split())
    if not source:
        return False

    rule = _SHORT_TOKEN_RULES[key]
    if any(alias.search(source) for alias in rule.aliases):
        return True

    supported = False
    for match in rule.bare_token.finditer(source):
        if _bare_hit_supported(rule, source, match):
            supported = True
            break
    return supported
