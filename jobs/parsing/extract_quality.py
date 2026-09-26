"""Cheap extract quality checks for inbox position parsing."""

from __future__ import annotations

from spejder.jobs.parsing.artifact_interpreter import _CTA_LABELS

# Titles that look like digest chrome rather than a role name.
_BOILERPLATE_TITLE_PHRASES = frozenset(
    {
        "jobs similar to",
        "new jobs match your preferences",
        "job alert",
        "viewed jobs",
        "according to your selected",
    }
)


def entry_weak_reasons(entry: dict) -> list[str]:
    """Return reason codes when an extracted entry is too weak to trust/upsert."""
    reasons: list[str] = []
    title = " ".join(str(entry.get("title") or "").split())
    company = " ".join(str(entry.get("company") or "").split())
    title_key = title.casefold()
    company_key = company.casefold()

    if not title:
        reasons.append("empty_title")
    elif title_key in _CTA_LABELS:
        reasons.append("cta_title")
    elif any(phrase in title_key for phrase in _BOILERPLATE_TITLE_PHRASES):
        reasons.append("boilerplate_title")

    if company_key and any(phrase in company_key for phrase in _BOILERPLATE_TITLE_PHRASES):
        reasons.append("boilerplate_company")

    source = " ".join(str(entry.get("source") or "").split())
    if (
        title
        and source
        and title_key == source.casefold()
        and not company
    ):
        reasons.append("title_is_source")

    return reasons


def is_strong_entry(entry: dict) -> bool:
    return not entry_weak_reasons(entry)


def partition_entries(entries: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split entries into (strong, weak)."""
    strong: list[dict] = []
    weak: list[dict] = []
    for entry in entries:
        if is_strong_entry(entry):
            strong.append(entry)
        else:
            weak.append(entry)
    return strong, weak


def weak_reason_summary(weak_entries: list[dict]) -> str:
    """Comma-joined unique reason codes from weak entries (stable order)."""
    seen: list[str] = []
    for entry in weak_entries:
        for reason in entry_weak_reasons(entry):
            if reason not in seen:
                seen.append(reason)
    return ",".join(seen)
