"""Shared raw-text prepend helpers (no workflow imports)."""


def _is_invalid_summary_text(text: str) -> bool:
    low = " ".join((text or "").split()).strip().lower()
    if not low:
        return True
    bad_markers = [
        "llm summary failed",
        "model path does not exist",
        "traceback",
        "exception:",
    ]
    return any(marker in low for marker in bad_markers)


def _prepend_title_to_raw_text(title: str, raw_text: str, max_chars: int = 9000) -> str:
    title_clean = " ".join((title or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not title_clean:
        return raw_clean

    prefixed = f"Title: {title_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]


def _prepend_summary_to_raw_text(summary: str, raw_text: str, max_chars: int = 9000) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not summary_clean or _is_invalid_summary_text(summary_clean):
        return raw_clean

    prefixed = f"Summary: {summary_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]
