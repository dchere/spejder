"""Truncated-JSON recovery for career-alert synth payloads."""
import json
import re

def _balanced_json_slice(text: str, start: int) -> str:
    """Return substring of text starting at ``start`` with balanced ``{}`` or ``[]``."""
    if start < 0 or start >= len(text):
        return ""
    opener = text[start]
    closer = {"{": "}", "[": "]"}.get(opener)
    if closer is None:
        return ""
    depth = 0
    in_string = False
    escape = False
    for index in range(start, len(text)):
        ch = text[index]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return text[start : index + 1]
    return ""


def _extract_json_object(text: str) -> dict:
    payload = (text or "").strip()
    if not payload:
        return {}
    if payload.startswith("```"):
        payload = re.sub(r"^```(?:json)?\s*", "", payload, count=1, flags=re.IGNORECASE)
        payload = re.sub(r"\s*```$", "", payload, count=1)
    start = payload.find("{")
    if start == -1:
        return {}
    candidate = _balanced_json_slice(payload, start)
    if not candidate:
        end = payload.rfind("}")
        if end <= start:
            return {}
        candidate = payload[start : end + 1]
    try:
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return _recover_truncated_synth_payload(payload)


def _extract_named_object(text: str, key: str) -> dict:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*\{{', text)
    if not match:
        return {}
    brace_at = text.find("{", match.start())
    slice_text = _balanced_json_slice(text, brace_at)
    if not slice_text:
        return {}
    try:
        parsed = json.loads(slice_text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_partial_positions(text: str) -> list[dict]:
    match = re.search(r'"positions"\s*:\s*\[', text)
    if not match:
        return []
    bracket_at = text.find("[", match.start())
    slice_text = _balanced_json_slice(text, bracket_at)
    raw_items: list[str] = []
    if slice_text:
        try:
            parsed = json.loads(slice_text)
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        body = slice_text[1:-1]
    else:
        body = text[bracket_at + 1 :]
    depth = 0
    in_string = False
    escape = False
    start = None
    for index, ch in enumerate(body):
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
            continue
        if ch == "{":
            if depth == 0:
                start = index
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                raw_items.append(body[start : index + 1])
                start = None
    positions: list[dict] = []
    for item in raw_items:
        try:
            parsed = json.loads(item)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
        if isinstance(parsed, dict):
            positions.append(parsed)
    return positions


def _recover_truncated_synth_payload(text: str) -> dict:
    """Recover artifact (+ any complete position objects) when output was cut mid-JSON."""
    artifact = _extract_named_object(text, "artifact")
    positions = _extract_partial_positions(text)
    if not artifact and not positions:
        return {}
    payload: dict = {}
    if artifact:
        payload["artifact"] = artifact
    if positions:
        payload["positions"] = positions
    return payload
