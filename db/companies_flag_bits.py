"""Company promote flag bit constants and pure helpers (no companies import)."""

from __future__ import annotations

COMPANY_FLAG_PROMOTE = 1
COMPANY_FLAG_PROMOTE_TOUCHED = 2
COMPANY_FLAG_AVOID = 4


def company_has_flag(flags: int, bit: int) -> bool:
    return bool(int(flags or 0) & int(bit))


def with_flag(flags: int, bit: int, enabled: bool) -> int:
    value = int(flags or 0)
    bit = int(bit)
    if enabled:
        return value | bit
    return value & ~bit


def companies_flags_column_missing(cur) -> bool:
    """True when companies.flags is absent (or companies table missing)."""
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='companies'"
    )
    if cur.fetchone() is None:
        return True
    cur.execute("PRAGMA table_info(companies)")
    cols = {row[1] for row in cur.fetchall()}
    return "flags" not in cols


def merge_promote_flags(participant_flags: list[int]) -> int:
    """Merge: Promote=OR; touched if any touched or Promote disagreed; Avoid=OR."""
    if not participant_flags:
        return 0
    values = [int(f or 0) for f in participant_flags]
    promote_bits = [company_has_flag(f, COMPANY_FLAG_PROMOTE) for f in values]
    touched_bits = [
        company_has_flag(f, COMPANY_FLAG_PROMOTE_TOUCHED) for f in values
    ]
    avoid = any(company_has_flag(f, COMPANY_FLAG_AVOID) for f in values)
    promote = any(promote_bits)
    disagreed = any(promote_bits) and not all(promote_bits)
    touched = any(touched_bits) or disagreed
    flags = 0
    if promote:
        flags = with_flag(flags, COMPANY_FLAG_PROMOTE, True)
    if touched:
        flags = with_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED, True)
    if avoid:
        flags = with_flag(flags, COMPANY_FLAG_AVOID, True)
    return flags


__all__ = [
    "COMPANY_FLAG_PROMOTE",
    "COMPANY_FLAG_PROMOTE_TOUCHED",
    "COMPANY_FLAG_AVOID",
    "company_has_flag",
    "with_flag",
    "companies_flags_column_missing",
    "merge_promote_flags",
]
