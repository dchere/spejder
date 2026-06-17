import re

from .utils import first_non_empty

_LINKEDIN_COMPANY_NOISE = re.compile(
    r"\s+according to your selected(?:\s+.*)?$",
    re.IGNORECASE,
)


def sanitize_company_name(company: str) -> str:
    cleaned = (company or "").strip()
    if not cleaned:
        return ""
    cleaned = _LINKEDIN_COMPANY_NOISE.sub("", cleaned).strip(" -|:")
    return cleaned[:180]


def extract_company_title(text: str, title_hint: str = "") -> tuple[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    line0 = first_non_empty(lines)
    title = title_hint.strip() if title_hint else ""

    if not title:
        title = line0[:180]

    company = ""

    if ":" in title and " - " in title:
        _, right = title.split(":", 1)
        right = right.strip()
        if " - " in right:
            comp, role = right.split(" - ", 1)
            comp = comp.strip(" \"'“”|:-")
            role = role.strip(" \"'“”|:-")
            if comp:
                company = comp
            if role:
                title = role

    pattern_at = re.search(r"(.+?)\s+at\s+(.+)", title, flags=re.IGNORECASE)
    if pattern_at:
        title = pattern_at.group(1).strip(" -|:")
        company = pattern_at.group(2).strip(" -|:")

    if not company:
        m_alert = re.search(
            r"^(?P<company>.+?)\s*-\s*job alert notification$",
            title,
            flags=re.IGNORECASE,
        )
        if m_alert:
            company = m_alert.group("company").strip(" \"'“”|:-")[:180]

    if not company:
        # "New job opportunities in Danske Bank" / "jobs posted to Acme Corp"
        m_in = re.search(
            r"\b(?:opportunities?|jobs?|openings?)\s+(?:in|at|to)\s+([A-Z][^\n.!?]{2,60}?)(?:\s*$|\s+today|\s+now|\s+posted)",
            title,
            flags=re.IGNORECASE,
        )
        if m_in:
            company = m_in.group(1).strip(" \"'|:-")[:180]

    if not company:
        # Subject like "New jobs posted from jobs.tetrapak.com" — extract domain brand
        m_domain = re.search(
            r"\bfrom\s+(?:jobs\.|careers\.|career\.)([a-z0-9][a-z0-9\-]*)\b",
            title,
            flags=re.IGNORECASE,
        )
        if m_domain:
            brand = m_domain.group(1).replace("-", " ").title()
            if len(brand) >= 2:
                company = brand

    if not company:
        for ln in lines[:20]:
            if re.search(
                r"\b(company|employer|organization)\b", ln, flags=re.IGNORECASE
            ):
                parts = re.split(r":", ln, maxsplit=1)
                if len(parts) == 2 and parts[1].strip():
                    company = parts[1].strip()[:180]
                    break

    if not company:
        m = re.search(
            r"\b([A-Z][A-Za-z0-9&.,\- ]{2,50})(?:\s+is\s+hiring|\s+careers|\s+jobs?)\b",
            text,
        )
        if m:
            company = m.group(1).strip()

    return sanitize_company_name(company), title[:180]


