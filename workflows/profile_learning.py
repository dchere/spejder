"""Shared profile keyword learning stage for GUI sync and process-inbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

from spejder.config import AppConfig
from spejder.jobs import update_profile_from_db_signals

StageCallback = Callable[[str, str], None]

_KEYWORD_LIST_ATTRS = (
    "learned_include_keywords",
    "learned_exclude_keywords",
)
_SUGGESTION_LIST_ATTRS = (
    "missing_skills_suggestions",
)


@dataclass(frozen=True)
class ProfileLearningResult:
    """Outcome of ``run_profile_keyword_learning``."""

    learning_info: dict[str, int]
    keywords_changed: bool
    suggestions_changed: bool

    @property
    def profile_changed(self) -> bool:
        """True when any learned list (keywords or suggestions) differed."""
        return self.keywords_changed or self.suggestions_changed


def _list_snapshot(profile: AppConfig, attrs: tuple[str, ...]) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(getattr(profile, attr, None) or []) for attr in attrs)


def run_profile_keyword_learning(
    db_path: str,
    profile_path: str,
    *,
    on_stage: Optional[StageCallback] = None,
) -> ProfileLearningResult:
    """Emit ``profile_learning``, update learned lists from DB, detect dirty lists.

    Snapshots ``learned_include_keywords``, ``learned_exclude_keywords``, and
    ``missing_skills_suggestions`` from disk before and after
    ``update_profile_from_db_signals``. ``keywords_changed`` covers include/exclude;
    ``suggestions_changed`` covers missing-skills suggestions. ``profile_changed``
    is true when either flag is set.
    """
    if on_stage is not None:
        on_stage("profile_learning", "Learning profile keywords")

    before_profile = AppConfig.load(profile_path)
    before_keywords = _list_snapshot(before_profile, _KEYWORD_LIST_ATTRS)
    before_suggestions = _list_snapshot(before_profile, _SUGGESTION_LIST_ATTRS)
    learning_info = update_profile_from_db_signals(db_path, profile_path)
    after_profile = AppConfig.load(profile_path)
    after_keywords = _list_snapshot(after_profile, _KEYWORD_LIST_ATTRS)
    after_suggestions = _list_snapshot(after_profile, _SUGGESTION_LIST_ATTRS)
    return ProfileLearningResult(
        learning_info=learning_info,
        keywords_changed=before_keywords != after_keywords,
        suggestions_changed=before_suggestions != after_suggestions,
    )
