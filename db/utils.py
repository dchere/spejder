import base64
import sqlite3
import re
import time
import json
from datetime import datetime, timezone
from html import unescape
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse

EMERSON_ORACLE_FA_HOST = "hdjq.fa.us2.oraclecloud.com"

TITLE_GARBAGE_MARKERS = [
    "translated title",
    "translated title text",
    "original title",
    "original text",
    "english title",
]

def sanitize_job_title(title: str) -> str:
    if not title:
        return ""
    t = str(title).strip()
    low = t.lower()
    for marker in TITLE_GARBAGE_MARKERS:
        if low.startswith(marker):
            t = t[len(marker):].strip()
            low = t.lower()
    return t

SQLITE_TIMEOUT_SECONDS = 15
SQLITE_BUSY_TIMEOUT_MS = 15000


def _decode_mandrill_track_link(link: str) -> str:
    low = (link or "").lower()
    if "mandrillapp.com/track/click/" not in low:
        return link

    parsed = urlparse(link)
    q = parse_qs(parsed.query)
    token = (q.get("p", [""])[0] or "").strip()
    if not token:
        return link

    try:
        padded = token + ("=" * (-len(token) % 4))
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode(
            "utf-8", errors="ignore"
        )
        outer = json.loads(decoded)
        payload = outer.get("p", "")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            return link
        raw_url = str(payload.get("url", "") or "").strip()
        if not raw_url:
            return link
        clean = unescape(unquote(raw_url)).replace("\\/", "/")
        return clean or link
    except Exception:
        return link



def _normalize_position_link(link: str) -> str:
    link = _decode_mandrill_track_link(link)
    link = link.strip()
    if not link:
        return ""

    parsed = urlparse(link)
    low = link.lower()

    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", low)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"

    m = re.search(r"thehub\.io/jobs/([0-9a-f]{12,})", low)
    if m:
        return f"https://thehub.io/jobs/{m.group(1)}"

    if re.search(
        r"(?:careers\.google\.com|google\.com)/.+/jobs/results/\d+",
        low,
    ):
        if parsed.path:
            return f"https://careers.google.com{parsed.path}".rstrip("/")
        return ""

    if "jobindex.dk" in low:
        job_id = _extract_jobindex_id(link)
        if job_id:
            return f"https://www.jobindex.dk/jobannonce/{job_id}"

        q = parse_qs(parsed.query)
        ttid = q.get("ttid", [""])[0]
        if ttid:
            return ""

    if "jobs.teradyne.com" in low and "/job/" in low and parsed.path:
        return f"https://jobs.teradyne.com{parsed.path}".rstrip("/")

    if "careers.nordea.com" in low and "/job/" in low and parsed.path:
        return f"https://careers.nordea.com{parsed.path}".rstrip("/")

    if "careers.novonordisk.com" in low and "/job/" in low and parsed.path:
        return f"https://careers.novonordisk.com{parsed.path}".rstrip("/")

    if "careers.vestas.com" in low and re.search(r"/job/.+/\d+", low) and parsed.path:
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

    if (
        re.search(r"\.fa\.[a-z0-9]+\.oraclecloud\.com", (parsed.netloc or "").lower())
        and "/candidateexperience/" in (parsed.path or "").lower()
        and re.search(r"/job/\d+/?", (parsed.path or "").lower())
    ):
        netloc_clean = re.sub(r":(443|80)$", "", parsed.netloc or "")
        return (
            f"https://{netloc_clean}{parsed.path}"
            if netloc_clean and parsed.path
            else link
        ).rstrip("/")

    if (
        re.search(r"\.fa\.ocs\.oraclecloud\.(?:com|eu)",
                  (parsed.netloc or "").lower())
        and "/candidateexperience/" in (parsed.path or "").lower()
        and re.search(r"/job/\d+/?$", (parsed.path or "").lower())
    ):
        netloc_clean = re.sub(r":(443|80)$", "", parsed.netloc or "")
        return (
            f"https://{netloc_clean}{parsed.path}"
            if netloc_clean and parsed.path
            else link
        ).rstrip("/")

    if "jobs.tetrapak.com" in low and re.search(r"/job/[^/]+/\d+", low) and parsed.path:
        return f"http://jobs.tetrapak.com{parsed.path}".rstrip("/")

    base = (
        f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.scheme and parsed.netloc
        else link
    )
    return base.rstrip("/")



def _extract_jobindex_id(link: str) -> str:
    low = (link or "").lower()

    m = re.search(r"/jobannonce/([hr]\d+)", low)
    if m:
        return m.group(1)

    m = re.search(r"/bruger/dine-job/([hr]\d+)", low)
    if m:
        return m.group(1)

    parsed = urlparse(link)
    q = parse_qs(parsed.query)

    jobid = (q.get("jobid", [""])[0] or "").strip().lower()
    if re.fullmatch(r"[hr]\d+", jobid):
        return jobid

    tval = (q.get("t", [""])[0] or "").strip().lower()
    if re.fullmatch(r"[hr]\d+", tval):
        return tval

    return ""



def _provider_from_link(link: str) -> str:
    low = (link or "").lower()
    if "linkedin.com" in low:
        return "LinkedIn"
    if "thehub.io" in low:
        return "The Hub"
    if "careers.google.com" in low or "google.com/about/careers/applications/jobs/results/" in low:
        return "Google Careers"
    if "jobindex.dk" in low:
        return "Jobindex"
    if "careers.nordea.com" in low:
        return "Nordea"
    if "careers.novonordisk.com" in low:
        return "Novo Nordisk"
    parsed = urlparse(link)
    netloc_clean = re.sub(r":(443|80)$", "", (parsed.netloc or "")).lower()
    path_low = (parsed.path or "").lower()
    if netloc_clean == EMERSON_ORACLE_FA_HOST and "/candidateexperience/" in path_low:
        return "Emerson Career Site"
    if re.search(r"\.fa\.[a-z0-9]+\.oraclecloud\.com", low) and "/candidateexperience/" in low:
        return "Oracle CX"
    if re.search(r"\.fa\.ocs\.oraclecloud\.(?:com|eu)", low) and "/candidateexperience/" in low:
        return "Oracle CX"
    if "careers.demant.com" in low:
        return "Demant"
    if "jobs.danfoss.com" in low:
        return "Danfoss"
    if "careers.vestas.com" in low:
        return "Vestas"
    if "jobs.teradyne.com" in low:
        return "Teradyne"
    if "careers.nttdata-solutions.com" in low:
        return "NTT DATA Business Solutions"
    if "careers.getinge.com" in low:
        return "Getinge"
    if "jobs.tetrapak.com" in low:
        return "Tetra Pak"

    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "Unknown"

def get_job_link(db_path: str, job_id: int) -> tuple[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT position_link FROM jobs WHERE id=?", (job_id,))
        return cur.fetchone()
    finally:
        conn.close()



def _normalize_skill_name_key(name: str) -> str:
    return " ".join((name or "").strip().lower().split())
