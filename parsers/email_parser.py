"""Email parser for job-alert .eml files."""
import os
import re
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser

from bs4 import BeautifulSoup


def _extract_links_from_text(text: str) -> list[str]:
    links = re.findall(r"https?://[^\s<>'\"]+", text)
    cleaned = []
    for link in links:
        item = link.strip().rstrip(").,;]")
        if item not in cleaned:
            cleaned.append(item)
    return cleaned


def _parse_eml_file(path: str) -> dict:
    with open(path, "rb") as f:
        msg = BytesParser(policy=policy.default).parse(f)

    subject = ""
    if msg.get("Subject"):
        subject = str(make_header(decode_header(msg.get("Subject")))).strip()

    plain_text_parts = []
    html_parts = []
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ("text/plain", "text/html"):
            try:
                payload = part.get_payload(decode=True)
                charset = part.get_content_charset() or "utf-8"
                content = payload.decode(charset, errors="ignore") if payload else ""
            except Exception:
                content = part.get_payload() if isinstance(part.get_payload(), str) else ""

            if ctype == "text/plain":
                plain_text_parts.append(content)
            else:
                html_parts.append(content)

    text = "\n".join(plain_text_parts).strip()
    html = "\n".join(html_parts).strip()
    links = _extract_links_from_text(text)

    if html:
        soup = BeautifulSoup(html, "html.parser")
        html_links = [a.get("href") for a in soup.find_all("a", href=True)]
        for lnk in html_links:
            if lnk and lnk not in links:
                links.append(lnk)

    if not text and html_parts:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator="\n").strip()
        links = [a.get("href") for a in soup.find_all("a", href=True)]

    title = subject
    if not title:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        title = lines[0][:180] if lines else ""

    return {
        "id": os.path.abspath(path),
        "path": os.path.abspath(path),
        "text": text,
        "html": html,
        "links": links,
        "title": title,
    }


def parse_email_file(path: str) -> dict:
    """Parse a job-alert .eml file and extract text, HTML body, and links.

    Returns dict with keys: id, path, text, html, links, title
    """
    if not path.lower().endswith(".eml"):
        raise ValueError(f"Unsupported file type (expected .eml): {path}")
    return _parse_eml_file(path)


def load_files(folder: str, exts: list[str] = None) -> list[dict]:
    """Walk a folder and parse supported files (default: .eml)."""
    if exts is None:
        exts = [".eml"]

    docs = []
    for root, _, files in os.walk(folder):
        for fn in files:
            if any(fn.lower().endswith(e) for e in exts):
                path = os.path.join(root, fn)
                try:
                    doc = parse_email_file(path)
                    docs.append(doc)
                except Exception:
                    # ignore parse errors for now
                    continue
    return docs
