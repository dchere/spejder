"""Pydantic schema for declarative career-alert format artifacts."""

from __future__ import annotations

import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


AnchorParseOp = Literal["jobs2web_middot_or_dash", "anchor_text_compact"]
ExtractMode = Literal["filtered_links", "css"]
ArtifactProvenance = Literal["shipped", "llm_synth", "manual"]

# Cap untrusted overlay/LLM path regexes to limit ReDoS surface.
_MAX_PATH_REGEX_LEN = 80
_NESTED_QUANTIFIER = re.compile(
    r"\([^)]*[+*{][^)]*\)[+*{]|"  # (a+)+ / (a*){2}
    r"[+*][+?]|"  # a++ / a*+ possessive-ish
    r"\{(?:\d+)?,(?:\d+)?\}\s*[+*{]"  # {1,}+ / {2,}*
)


def compile_safe_path_regex(pattern: str) -> Optional[re.Pattern[str]]:
    """Compile require_path_regex or return None if unsafe / invalid."""
    text = (pattern or "").strip()
    if not text or len(text) > _MAX_PATH_REGEX_LEN:
        return None
    if _NESTED_QUANTIFIER.search(text):
        return None
    try:
        return re.compile(text, flags=re.IGNORECASE)
    except re.error:
        return None


class MatchConfig(BaseModel):
    host_substrings: list[str] = Field(min_length=1)
    path_includes: list[str] = Field(default_factory=lambda: ["/job/"], min_length=1)
    require_path_regex: Optional[str] = None

    @field_validator("host_substrings", "path_includes")
    @classmethod
    def _nonblank_substrings(cls, value: list[str]) -> list[str]:
        cleaned = [str(item).strip() for item in (value or []) if str(item).strip()]
        if not cleaned:
            raise ValueError("must contain at least one non-blank substring")
        return cleaned

    @field_validator("require_path_regex")
    @classmethod
    def _safe_path_regex(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if compile_safe_path_regex(text) is None:
            raise ValueError("require_path_regex rejected as unsafe or invalid")
        return text


class ExtractConfig(BaseModel):
    mode: ExtractMode = "filtered_links"
    selector: Optional[str] = None


class FieldRecipes(BaseModel):
    """Closed opcode set for field extraction (no arbitrary code)."""

    from_anchor: AnchorParseOp = "jobs2web_middot_or_dash"
    company: Optional[str] = None
    source: Optional[str] = None
    title_max: int = 180
    place_max: int = 180
    raw_text_max: int = 2500

    @field_validator("from_anchor")
    @classmethod
    def _known_anchor_op(cls, value: str) -> str:
        allowed = {"jobs2web_middot_or_dash", "anchor_text_compact"}
        if value not in allowed:
            raise ValueError(f"unknown from_anchor opcode: {value!r}")
        return value


class CareerAlertArtifact(BaseModel):
    id: str
    version: int = 1
    priority: int = 0
    enabled: bool = True
    match: MatchConfig
    extract: ExtractConfig = Field(default_factory=ExtractConfig)
    fields: FieldRecipes
    source: ArtifactProvenance = "shipped"
    created_at: Optional[str] = None
    model_path: Optional[str] = None
    parent_eml_hash: Optional[str] = None

    @field_validator("id")
    @classmethod
    def _nonempty_id(cls, value: str) -> str:
        cleaned = (value or "").strip()
        if not cleaned:
            raise ValueError("artifact id must be non-empty")
        return cleaned
