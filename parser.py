import os
import re
from email import policy
from email.header import decode_header, make_header
from email.parser import BytesParser
from bs4 import BeautifulSoup
from typing import List, Dict


def _extract_links_from_text(text: str) -> List[str]:
    links = re.findall(r"https?://[^\s<>'\"]+", text)
    cleaned = []
    for link in links:
        item = link.strip().rstrip(").,;]")
        if item not in cleaned:
            cleaned.append(item)
    return cleaned


def _parse_eml_file(path: str) -> Dict:
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


def parse_html_file(path: str) -> Dict:
    """Parse an HTML or EML file and extract text and links.

    Returns dict with keys: id, path, text, links
    """
    if path.lower().endswith(".eml"):
        return _parse_eml_file(path)

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read()

    soup = BeautifulSoup(raw, "html.parser")
    # If it's an .eml that contains plain text fallback, BeautifulSoup still works
    text = soup.get_text(separator="\n").strip()
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    if not title:
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(separator=" ").strip()

    return {
        "id": os.path.abspath(path),
        "path": os.path.abspath(path),
        "text": text,
        "html": raw,
        "links": links,
        "title": title,
    }


def load_files(folder: str, exts: List[str] = None) -> List[Dict]:
    """Walk a folder and parse supported files (default: .html, .htm, .eml)."""
    if exts is None:
        exts = [".html", ".htm", ".eml"]

    docs = []
    for root, _, files in os.walk(folder):
        for fn in files:
            if any(fn.lower().endswith(e) for e in exts):
                path = os.path.join(root, fn)
                try:
                    doc = parse_html_file(path)
                    docs.append(doc)
                except Exception:
                    # ignore parse errors for now
                    continue
    return docs
