"""Typed built-in host extractors registered for `extract_job_entries`.

New hosts opt into the merge path with ``@register_platform`` instead of
editing a hand-built tuple in ``core.py``.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Callable, Optional

ExtractorFn = Callable[[str], dict[str, dict[str, str]]]
MergeTransform = Callable[[dict], dict]


@dataclass(frozen=True)
class RegisteredPlatform:
    name: str
    order: int
    _module: str
    _qualname: str
    merge_transform: Optional[MergeTransform] = None

    def extract(self, html_text: str) -> dict[str, dict[str, str]]:
        # Resolve at call time so unittest.mock.patch on the module attribute works.
        fn = getattr(sys.modules[self._module], self._qualname)
        return fn(html_text)


_PLATFORMS: dict[str, RegisteredPlatform] = {}
_LOADED = False


def register_platform(
    name: str,
    *,
    order: int,
    merge_transform: Optional[MergeTransform] = None,
) -> Callable[[ExtractorFn], ExtractorFn]:
    """Declare a built-in host extractor and its first-wins merge priority."""

    def decorator(fn: ExtractorFn) -> ExtractorFn:
        if name in _PLATFORMS:
            raise ValueError(f"duplicate platform registration: {name}")
        _PLATFORMS[name] = RegisteredPlatform(
            name=name,
            order=int(order),
            _module=fn.__module__,
            _qualname=fn.__name__,
            merge_transform=merge_transform,
        )
        return fn

    return decorator


def registered_platforms() -> tuple[RegisteredPlatform, ...]:
    ensure_platforms_loaded()
    return tuple(
        sorted(_PLATFORMS.values(), key=lambda spec: (spec.order, spec.name))
    )


def ensure_platforms_loaded() -> None:
    """Import platform modules so ``@register_platform`` side effects run."""
    global _LOADED
    if _LOADED:
        return
    # Import for registration side effects only.
    from spejder.jobs.parsing import platforms as _platforms  # noqa: F401
    from spejder.jobs.parsing import (  # noqa: F401
        platforms_career_alerts as _career_alerts,
    )

    _LOADED = True
