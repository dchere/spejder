import json
import os
import re
import sqlite3
from collections import Counter
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from bs4 import BeautifulSoup


DEFAULT_PROFILE = {
    "include_keywords": [
        "python",
        "backend",
        "data",
        "machine learning",
        "ai",
        "developer",
        "engineer",
        "remote",
    ],
    "exclude_keywords": [
        "sales",
        "marketing",
        "nurse",
        "driver",
        "on-site",
        "onsite",
        "unpaid",
    ],
    "min_score": 2,
    "learned_include_keywords": [],
    "learned_exclude_keywords": [],
    "default_inbox": "./inbox",
    "default_db": "./jobs.db",
    "default_report_dir": "./outbox",
    "default_model": "",
    "max_input_chars": 4500,
    "server_host": "127.0.0.1",
    "server_port": 8765,
}


LEARNING_STOPWORDS = {
    "about", "above", "after", "again", "against", "all", "also", "and", "any", "are", "because",
    "been", "before", "being", "below", "between", "both", "but", "can", "company", "could", "danish",
    "denmark", "developer", "email", "for", "from", "have", "into", "job", "jobs", "just", "more",
    "not", "our", "out", "position", "role", "than", "that", "the", "their", "them", "there", "these",
    "this", "those", "through", "under", "using", "very", "want", "when", "where", "which", "with",
    "you", "your",
}

SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _unique_keywords(values: List[str]) -> List[str]:
    seen = set()
    out = []
    for value in values:
        key = (value or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _tokenize_learning_text(text: str) -> List[str]:
    if not text:
        return []
    raw = re.findall(r"[a-zA-Z][a-zA-Z0-9+#.-]{2,}", text.lower())
    cleaned = []
    for token in raw:
        token = token.strip(".-")
        if len(token) < 3:
            continue
        if token in LEARNING_STOPWORDS:
            continue
        cleaned.append(token)
    return cleaned


def _suggest_keywords_from_labeled_jobs(db_path: str, max_keywords: int = 20) -> Tuple[List[str], List[str], int]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT category, title, company, place, work_type, raw_text
            FROM jobs
            WHERE category IN ('relevant', 'not relevant')
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    relevant_docs = 0
    not_relevant_docs = 0
    relevant_df = Counter()
    not_relevant_df = Counter()

    for category, title, company, place, work_type, raw_text in rows:
        text = "\n".join([
            title or "",
            company or "",
            place or "",
            work_type or "",
            raw_text or "",
        ])
        tokens = set(_tokenize_learning_text(text))
        if not tokens:
            continue
        if category == "relevant":
            relevant_docs += 1
            relevant_df.update(tokens)
        elif category == "not relevant":
            not_relevant_docs += 1
            not_relevant_df.update(tokens)

    total_labeled = relevant_docs + not_relevant_docs
    if relevant_docs == 0 or not_relevant_docs == 0:
        return [], [], total_labeled

    candidates = set(relevant_df.keys()) | set(not_relevant_df.keys())
    include_ranked = []
    exclude_ranked = []

    for token in candidates:
        rel_df = relevant_df[token]
        nrel_df = not_relevant_df[token]
        if rel_df + nrel_df < 2:
            continue

        rel_rate = rel_df / max(1, relevant_docs)
        nrel_rate = nrel_df / max(1, not_relevant_docs)
        delta = rel_rate - nrel_rate

        if delta >= 0.2 and rel_df >= 2:
            include_ranked.append((delta, rel_df, token))
        elif delta <= -0.2 and nrel_df >= 2:
            exclude_ranked.append((abs(delta), nrel_df, token))

    include_ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    exclude_ranked.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)

    learned_include = [token for _, _, token in include_ranked[:max_keywords]]
    learned_exclude = [token for _, _, token in exclude_ranked[:max_keywords] if token not in learned_include]
    return learned_include, learned_exclude, total_labeled


def ensure_db(db_path: str):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
        has_jobs = cur.fetchone() is not None

        if has_jobs:
            cur.execute("PRAGMA table_info(jobs)")
            cols = {row[1] for row in cur.fetchall()}
            if "source_path" in cols:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS jobs_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT,
                        company TEXT,
                        title TEXT,
                        place TEXT,
                        work_type TEXT,
                        position_link TEXT UNIQUE NOT NULL,
                        raw_text TEXT,
                        description_raw TEXT,
                        description TEXT,
                        viewed INTEGER DEFAULT 0,
                        applied INTEGER DEFAULT 0,
                        relevance_score REAL DEFAULT 0,
                        relevant INTEGER DEFAULT 0,
                        category TEXT DEFAULT 'not relevant',
                        relevance_reason TEXT,
                        summary TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                cur.execute(
                    """
                    INSERT OR IGNORE INTO jobs_new
                        (source, company, title, place, work_type, position_link, raw_text, description_raw, description, viewed, applied, relevance_score, relevant, category, relevance_reason, summary, created_at, updated_at)
                    SELECT
                        '',
                        company,
                        title,
                        '',
                        'Unknown',
                        CASE
                            WHEN position_link IS NULL OR position_link = '' THEN 'legacy://' || id
                            ELSE position_link
                        END,
                        raw_text,
                        '',
                        '',
                        0,
                        0,
                        COALESCE(relevance_score, 0),
                        COALESCE(relevant, 0),
                        COALESCE(category, 'not relevant'),
                        relevance_reason,
                        summary,
                        COALESCE(created_at, datetime('now')),
                        COALESCE(updated_at, datetime('now'))
                    FROM jobs
                    """
                )
                cur.execute("DROP TABLE jobs")
                cur.execute("ALTER TABLE jobs_new RENAME TO jobs")

            if "place" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN place TEXT")
            if "work_type" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN work_type TEXT")
            if "viewed" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN viewed INTEGER DEFAULT 0")
            if "applied" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN applied INTEGER DEFAULT 0")
            if "source" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN source TEXT")
            if "description_raw" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN description_raw TEXT")
            if "description" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN description TEXT")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                company TEXT,
                title TEXT,
                place TEXT,
                work_type TEXT,
                position_link TEXT UNIQUE NOT NULL,
                raw_text TEXT,
                description_raw TEXT,
                description TEXT,
                viewed INTEGER DEFAULT 0,
                applied INTEGER DEFAULT 0,
                relevance_score REAL DEFAULT 0,
                relevant INTEGER DEFAULT 0,
                category TEXT DEFAULT 'not relevant',
                relevance_reason TEXT,
                summary TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            DELETE FROM jobs
            WHERE NOT (
                lower(position_link) LIKE '%linkedin.com/%jobs/view/%'
                OR (
                    lower(position_link) LIKE '%jobindex.dk%'
                    AND (
                        lower(position_link) LIKE '%jobid=%'
                        OR lower(position_link) LIKE '%/jobannonce/h%'
                        OR lower(position_link) LIKE '%/jobannonce/r%'
                    )
                )
            )
            """
        )

        cur.execute(
            """
            DELETE FROM jobs
            WHERE lower(source) = 'linkedin'
              AND (
                lower(title) LIKE 'jobs similar to%'
                OR lower(title) LIKE 'new jobs match your preferences%'
                OR lower(title) LIKE 'job alert%'
                OR lower(raw_text) LIKE '%jobs similar to%'
                OR lower(raw_text) LIKE '%new jobs match your preferences%'
              )
            """
        )

        cur.execute("UPDATE jobs SET work_type='Unknown' WHERE work_type IS NULL OR work_type='' ")
        cur.execute("UPDATE jobs SET viewed=0 WHERE viewed IS NULL")
        cur.execute("UPDATE jobs SET applied=0 WHERE applied IS NULL")
        cur.execute("UPDATE jobs SET source='' WHERE source IS NULL")
        cur.execute("UPDATE jobs SET description_raw='' WHERE description_raw IS NULL")
        cur.execute("UPDATE jobs SET description_raw='' WHERE TRIM(description_raw)<>''")
        cur.execute("UPDATE jobs SET description='' WHERE description IS NULL")

        cur.execute("SELECT id, position_link FROM jobs")
        rows = cur.fetchall()
        by_norm = {}
        for rid, link in rows:
            norm = _normalize_position_link(link or "")
            if not norm:
                continue
            by_norm.setdefault(norm, []).append((rid, link or ""))

        for norm, items in by_norm.items():
            items = sorted(items, key=lambda x: x[0])
            keep_id = items[0][0]
            for rid, _ in items[1:]:
                cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
            cur.execute("SELECT position_link FROM jobs WHERE id=?", (keep_id,))
            row = cur.fetchone()
            if row and row[0] != norm:
                cur.execute("UPDATE jobs SET position_link=? WHERE id=?", (norm, keep_id))

        cur.execute("SELECT id, position_link, source FROM jobs")
        for rid, link, source in cur.fetchall():
            provider = _provider_from_link(link or "")
            if provider and (not source or source.strip() != provider):
                cur.execute("UPDATE jobs SET source=? WHERE id=?", (provider, rid))

        conn.commit()
    finally:
        conn.close()


def load_profile(profile_path: Optional[str]) -> Dict:
    if not profile_path:
        return DEFAULT_PROFILE.copy()
    with open(profile_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    profile = DEFAULT_PROFILE.copy()
    profile.update(data)

    base_include = profile.get("include_keywords", []) or []
    base_exclude = profile.get("exclude_keywords", []) or []
    learned_include = profile.get("learned_include_keywords", []) or []
    learned_exclude = profile.get("learned_exclude_keywords", []) or []

    profile["include_keywords"] = _unique_keywords(list(base_include) + list(learned_include))
    profile["exclude_keywords"] = _unique_keywords(list(base_exclude) + list(learned_exclude))
    profile["learned_include_keywords"] = _unique_keywords(list(learned_include))
    profile["learned_exclude_keywords"] = _unique_keywords(list(learned_exclude))
    return profile


def update_profile_from_db_signals(db_path: str, profile_path: str, max_keywords: int = 20) -> Dict[str, int]:
    profile = DEFAULT_PROFILE.copy()
    if profile_path and os.path.exists(profile_path):
        with open(profile_path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        profile.update(existing)

    learned_include, learned_exclude, labeled_count = _suggest_keywords_from_labeled_jobs(db_path, max_keywords=max_keywords)
    profile["learned_include_keywords"] = learned_include
    profile["learned_exclude_keywords"] = learned_exclude

    os.makedirs(os.path.dirname(os.path.abspath(profile_path)), exist_ok=True)
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    return {
        "labeled_count": int(labeled_count),
        "learned_include_count": len(learned_include),
        "learned_exclude_count": len(learned_exclude),
    }


def _work_type_from_html_for_link(html_text: str, normalized_link: str) -> str:
    if not html_text or not normalized_link:
        return ""
    low = html_text.lower()
    m = re.search(r"/jobs/view/(\d+)", normalized_link.lower())
    if not m:
        return ""
    job_id = m.group(1)

    candidates = [f"/jobs/view/{job_id}", f"/comm/jobs/view/{job_id}"]
    link_positions = []
    for needle in candidates:
        link_positions.extend([m.start() for m in re.finditer(re.escape(needle), low)])

    if not link_positions:
        return ""

    token_patterns = {
        "Hybrid": r"\bhybrid\b",
        "Remote": r"\bremote\b",
        "On-site": r"\bon-site\b|\bonsite\b",
    }

    best_type = ""
    best_distance = None
    for work_type, pattern in token_patterns.items():
        token_positions = [m.start() for m in re.finditer(pattern, low)]
        if not token_positions:
            continue
        for link_pos in link_positions:
            nearest_distance = min(abs(token_pos - link_pos) for token_pos in token_positions)
            if best_distance is None or nearest_distance < best_distance:
                best_distance = nearest_distance
                best_type = work_type

    if best_distance is not None and best_distance <= 6000:
        return best_type
    return ""


def _infer_work_type_from_text(text: str) -> str:
    low = (text or "").lower()
    if "hybrid" in low:
        return "Hybrid"
    if "remote" in low:
        return "Remote"
    if "on-site" in low or "onsite" in low:
        return "On-site"
    return ""


def _parse_card_text_fields(card_text: str) -> Dict[str, str]:
    compact = " ".join((card_text or "").split())
    if not compact:
        return {"title": "", "company": "", "place": "", "work_type": ""}

    work_type = _infer_work_type_from_text(compact)
    match = re.search(
        r"^(?P<title>.+?)\s+(?P<company>[^·\(]{2,}?)\s*·\s*(?P<place>[^\(]{2,}?)\s*(?:\((?P<wt>Hybrid|Remote|On-site|Onsite)\))",
        compact,
        flags=re.IGNORECASE,
    )
    if not match:
        return {"title": "", "company": "", "place": "", "work_type": work_type}

    title = match.group("title").strip(" -|:")[:180]
    company = match.group("company").strip(" -|:")[:180]
    place = match.group("place").strip(" -|:")[:180]
    wt = match.group("wt") or ""
    if wt:
        wt_low = wt.lower()
        if wt_low == "onsite":
            work_type = "On-site"
        elif wt_low == "on-site":
            work_type = "On-site"
        elif wt_low == "hybrid":
            work_type = "Hybrid"
        elif wt_low == "remote":
            work_type = "Remote"

    return {
        "title": title,
        "company": company,
        "place": place,
        "work_type": work_type,
    }


def _parse_anchor_fragments(fragments: List[str]) -> Dict[str, str]:
    if not fragments:
        return {"title": "", "company": "", "place": "", "work_type": ""}

    detail_index = -1
    for i, frag in enumerate(fragments):
        if "·" in frag:
            detail_index = i
            break

    if detail_index > 0:
        title = " ".join(fragments[:detail_index]).strip(" -|:")[:180]
    else:
        title = fragments[0].strip(" -|:")[:180]

    company = ""
    place = ""
    work_type = _infer_work_type_from_text(" ".join(fragments))

    detail_line = ""
    for frag in fragments:
        if "·" in frag:
            detail_line = frag
            break

    if detail_line:
        left, right = detail_line.split("·", 1)
        company = left.strip(" -|:")[:180]
        right = right.strip()

        wt_match = re.search(r"\((Hybrid|Remote|On-site|Onsite)\)", right, flags=re.IGNORECASE)
        if wt_match:
            wt = wt_match.group(1).lower()
            if wt in ("on-site", "onsite"):
                work_type = "On-site"
            elif wt == "hybrid":
                work_type = "Hybrid"
            elif wt == "remote":
                work_type = "Remote"
            right = re.sub(r"\((Hybrid|Remote|On-site|Onsite)\)", "", right, flags=re.IGNORECASE).strip()

        place = right.strip(" -|:")[:180]

    return {
        "title": title,
        "company": company,
        "place": place,
        "work_type": work_type,
    }


def _extract_html_entries_by_link(html_text: str) -> Dict[str, Dict[str, str]]:
    if not html_text:
        return {}
    soup = BeautifulSoup(html_text, "html.parser")
    by_link: Dict[str, Dict[str, str]] = {}

    def field_score(fields: Dict[str, str], has_detail: bool) -> Tuple[int, int, int]:
        count = sum(1 for key in ["title", "company", "place", "work_type"] if fields.get(key))
        richness = len(fields.get("title", "")) + len(fields.get("company", "")) + len(fields.get("place", ""))
        return (1 if has_detail else 0, count, richness)

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not _is_job_link(normalized):
            continue

        fragments = [s.strip() for s in anchor.stripped_strings if s and s.strip()]
        fields = _parse_anchor_fragments(fragments)
        has_detail = any("·" in frag for frag in fragments)

        node = anchor
        card_text = ""
        for _ in range(8):
            if not node:
                break
            txt = " ".join(node.get_text(" ", strip=True).split())
            if txt and len(txt) >= 30:
                card_text = txt
            if node.name in ("tr", "table", "li", "div", "td") and 30 <= len(txt) <= 900:
                card_text = txt
                break
            node = node.parent

        if has_detail and (not fields.get("company") or not fields.get("place")):
            card_fields = _parse_card_text_fields(card_text)
            for key in ["title", "company", "place", "work_type"]:
                if not fields.get(key) and card_fields.get(key):
                    fields[key] = card_fields[key]

        fields["raw_text"] = (" | ".join(fragments) if fragments else card_text)[:800]
        fields["_has_detail"] = "1" if has_detail else "0"

        current = by_link.get(normalized)
        current_has_detail = (current or {}).get("_has_detail") == "1"
        if not current or field_score(fields, has_detail) > field_score(current, current_has_detail):
            by_link[normalized] = fields

    for value in by_link.values():
        value.pop("_has_detail", None)

    return by_link


def first_non_empty(lines: List[str]) -> str:
    for line in lines:
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


def extract_company_title(text: str, title_hint: str = "") -> Tuple[str, str]:
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
        for ln in lines[:20]:
            if re.search(r"\b(company|employer|organization)\b", ln, flags=re.IGNORECASE):
                parts = re.split(r":", ln, maxsplit=1)
                if len(parts) == 2 and parts[1].strip():
                    company = parts[1].strip()[:180]
                    break

    if not company:
        m = re.search(r"\b([A-Z][A-Za-z0-9&.,\- ]{2,50})(?:\s+is\s+hiring|\s+careers|\s+jobs?)\b", text)
        if m:
            company = m.group(1).strip()

    return company[:180], title[:180]


def _is_job_link(link: str) -> bool:
    low = link.lower()
    if "linkedin.com/comm/jobs/view/" in low or "linkedin.com/jobs/view/" in low:
        return True
    if "jobindex.dk" in low and (
        "jobid=" in low
        or re.search(r"/jobannonce/[hr]\d+", low)
        or re.search(r"/bruger/dine-job/[hr]\d+", low)
    ):
        return True
    if "careers.demant.com" in low and "/job/" in low:
        return True
    return False


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


def _normalize_position_link(link: str) -> str:
    link = link.strip()
    parsed = urlparse(link)
    low = link.lower()

    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", low)
    if m:
        return f"https://www.linkedin.com/jobs/view/{m.group(1)}"

    if "jobindex.dk" in low:
        job_id = _extract_jobindex_id(link)
        if job_id:
            return f"https://www.jobindex.dk/jobannonce/{job_id}"

        q = parse_qs(parsed.query)
        ttid = q.get("ttid", [""])[0]
        if ttid:
            return ""

    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme and parsed.netloc else link
    return base.rstrip("/")


def _provider_from_link(link: str) -> str:
    low = (link or "").lower()
    if "linkedin.com" in low:
        return "LinkedIn"
    if "jobindex.dk" in low:
        return "Jobindex"
    if "careers.demant.com" in low:
        return "Demant"

    parsed = urlparse(link)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "Unknown"


def _is_linkedin_reference_position_link(raw_link: str, normalized_link: str) -> bool:
    low = (raw_link or "").lower()
    if "linkedin.com" not in low:
        return False

    parsed = urlparse(raw_link)
    q = parse_qs(parsed.query)
    reference_id = (q.get("referenceJobId", [""])[0] or q.get("referencejobid", [""])[0] or "").strip()
    if not reference_id or not reference_id.isdigit():
        return False

    m = re.search(r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", (normalized_link or "").lower())
    if not m:
        return False

    return m.group(1) == reference_id


def _is_linkedin_boilerplate_entry(entry: Dict) -> bool:
    source = (entry.get("source") or _provider_from_link(entry.get("position_link", ""))).lower()
    if source != "linkedin":
        return False

    boilerplate_phrases = [
        "jobs similar to",
        "new jobs match your preferences",
        "job alert",
        "viewed jobs",
    ]

    title = (entry.get("title") or "").strip().lower()
    company = (entry.get("company") or "").strip().lower()
    place = (entry.get("place") or "").strip().lower()

    return any(phrase in value for phrase in boilerplate_phrases for value in [title, company, place])


def _extract_entries_from_text(text: str) -> List[Dict]:
    lines = [ln.strip() for ln in text.splitlines()]
    entries = []

    def score_title(line: str) -> int:
        low = line.lower()
        keys = ["developer", "engineer", "specialist", "manager", "scientist", "lead", "architect", "analyst", "consultant", ".net", "software"]
        return sum(1 for k in keys if k in low)

    def score_company(line: str) -> int:
        low = line.lower()
        keys = ["group", "inc", "aps", "a/s", "ltd", "gmbh", "company", "danmark"]
        return sum(1 for k in keys if k in low)

    def score_place(line: str) -> int:
        low = line.lower()
        keys = ["aarhus", "copenhagen", "odense", "lystrup", "humleb", "denmark", "municipality"]
        score = sum(1 for k in keys if k in low)
        if "," in line:
            score += 1
        return score

    for idx, line in enumerate(lines):
        if "view job:" not in line.lower():
            continue
        link_match = re.search(r"https?://\S+", line)
        if not link_match:
            continue

        link = _normalize_position_link(link_match.group(0))
        if not _is_job_link(link):
            continue

        candidates = []
        stop_phrases = [
            "this company is actively hiring",
            "apply with resume",
            "view job",
            "new jobs match your preferences",
            "job alert",
        ]
        start = max(0, idx - 10)
        for j in range(start, idx):
            candidate = lines[j].strip()
            clow = candidate.lower()
            if candidate and "http" not in clow and not any(sp in clow for sp in stop_phrases) and "----" not in candidate:
                candidates.append(candidate)

        title = ""
        company = ""
        place = ""
        if candidates:
            by_title = sorted(candidates, key=lambda s: (score_title(s), len(s)), reverse=True)
            by_company = sorted(candidates, key=lambda s: (score_company(s), len(s)), reverse=True)
            by_place = sorted(candidates, key=lambda s: (score_place(s), -len(s)), reverse=True)

            title = by_title[0] if score_title(by_title[0]) > 0 else (candidates[-3] if len(candidates) >= 3 else candidates[0])
            company = by_company[0] if score_company(by_company[0]) > 0 else (candidates[-2] if len(candidates) >= 2 else "")
            place = by_place[0] if score_place(by_place[0]) > 0 else (candidates[-1] if len(candidates) >= 3 else "")

            # prevent duplicates between fields
            used = {title}
            if company in used and len(candidates) > 1:
                for c in candidates:
                    if c not in used:
                        company = c
                        break
            used.add(company)
            if place in used:
                for c in candidates:
                    if c not in used:
                        place = c
                        break

        local_chunk = " ".join(lines[max(0, idx - 20): min(len(lines), idx + 5)]).lower()
        if "remote" in local_chunk:
            work_type = "Remote"
        elif "hybrid" in local_chunk:
            work_type = "Hybrid"
        elif "on-site" in local_chunk or "onsite" in local_chunk:
            work_type = "On-site"
        elif place:
            work_type = "On-site"
        else:
            work_type = "Unknown"

        snippet_start = max(0, idx - 4)
        snippet_end = min(len(lines), idx + 2)
        raw_text = "\n".join([s for s in lines[snippet_start:snippet_end] if s])

        entries.append(
            {
                "company": company[:180],
                "title": title[:180],
                "place": place[:180],
                "work_type": work_type,
                "position_link": link,
                "raw_text": raw_text,
                "description_raw": "",
            }
        )

    return entries


def _extract_jobindex_entries_by_link(html_text: str) -> Dict[str, Dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: Dict[str, Dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        if "jobindex.dk" not in (parsed.netloc or "").lower():
            continue

        job_id = _extract_jobindex_id(href)
        if not job_id:
            continue

        normalized = f"https://www.jobindex.dk/jobannonce/{job_id}"
        block = anchor.find_parent("table") or anchor.find_parent("tr")
        if not block:
            continue

        compact = " ".join(block.get_text(" ", strip=True).split())
        if len(compact) < 30:
            continue

        title = ""
        title_candidates = []
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            t2 = _extract_jobindex_id(href2)
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if t2 == job_id and txt2 and txt2.lower() not in {"view job", "apply", "about the company", "save job", "settings"}:
                title_candidates.append(txt2)
        if title_candidates:
            title = max(title_candidates, key=len)[:180]

        company = ""
        for link_node in block.find_all("a", href=True):
            href2 = link_node.get("href") or ""
            txt2 = " ".join(link_node.get_text(" ", strip=True).split())
            if txt2 and "jobindex.dk" not in href2.lower():
                company = txt2[:180]
                break
        if not company:
            fragments = [s.strip() for s in block.stripped_strings if s and s.strip()]
            if fragments:
                company = fragments[0][:180]

        place = ""
        if title:
            m_place = re.search(re.escape(title) + r"\s+(.{2,80}?)\s+\d+\s+min\b", compact, flags=re.IGNORECASE)
            if m_place:
                place = m_place.group(1).strip(" -|:")[:180]

        description_raw = ""
        m_desc = re.search(r"settings\s*\)\s*(.*?)\s*PUBLISHED\s*:", compact, flags=re.IGNORECASE)
        if m_desc:
            description_raw = m_desc.group(1).strip()
        else:
            m_desc2 = re.search(r"\d+\s+min\s*\(.*?\)\s*(.*?)\s*PUBLISHED\s*:", compact, flags=re.IGNORECASE)
            if m_desc2:
                description_raw = m_desc2.group(1).strip()

        by_link[normalized] = {
            "title": title,
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "description_raw": description_raw[:4000],
            "source": "Jobindex",
        }

    return by_link


def _extract_demant_entries_by_link(html_text: str) -> Dict[str, Dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: Dict[str, Dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "careers.demant.com" not in host or "/job/" not in path:
            continue

        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        compact = " ".join(anchor.get_text(" ", strip=True).split())
        if not compact:
            continue

        title = compact
        place = ""
        match = re.match(r"^(?P<title>.+?)\s*-\s*(?P<place>.+)$", compact)
        if match:
            title = (match.group("title") or "").strip()
            place = (match.group("place") or "").strip()

        by_link[normalized] = {
            "title": title[:180],
            "company": "Demant Group",
            "place": place[:180],
            "work_type": "Unknown",
            "raw_text": compact[:2500],
            "description_raw": "",
            "source": "Demant",
        }

    return by_link


def extract_job_entries(doc: Dict) -> List[Dict]:
    text = doc.get("text", "") or ""
    html_text = doc.get("html", "") or ""
    title_hint = doc.get("title", "") or ""
    links = doc.get("links", []) or []
    html_by_link = _extract_html_entries_by_link(html_text)
    jobindex_by_link = _extract_jobindex_entries_by_link(html_text)
    demant_by_link = _extract_demant_entries_by_link(html_text)

    by_text = _extract_entries_from_text(text)
    by_link = {}
    for entry in by_text:
        by_link[entry["position_link"]] = entry

    for lnk, entry in by_link.items():
        html_fields = html_by_link.get(lnk, {})
        ji_fields = jobindex_by_link.get(lnk, {})
        demant_fields = demant_by_link.get(lnk, {})

        if demant_fields.get("title"):
            entry["title"] = demant_fields["title"]
        if demant_fields.get("company"):
            entry["company"] = demant_fields["company"]
        if demant_fields.get("place"):
            entry["place"] = demant_fields["place"]
        if demant_fields.get("work_type"):
            entry["work_type"] = demant_fields["work_type"]
        if demant_fields.get("raw_text"):
            entry["raw_text"] = demant_fields["raw_text"]
        if demant_fields.get("description_raw"):
            entry["description_raw"] = demant_fields["description_raw"]
        if demant_fields.get("source"):
            entry["source"] = demant_fields["source"]

        if ji_fields.get("title"):
            entry["title"] = ji_fields["title"]
        if ji_fields.get("company"):
            entry["company"] = ji_fields["company"]
        if ji_fields.get("place"):
            entry["place"] = ji_fields["place"]
        if ji_fields.get("description_raw"):
            entry["description_raw"] = ji_fields["description_raw"]
        if ji_fields.get("raw_text"):
            entry["raw_text"] = ji_fields["raw_text"]

        if html_fields.get("title"):
            entry["title"] = html_fields["title"]
        if html_fields.get("company"):
            entry["company"] = html_fields["company"]
        if html_fields.get("place"):
            entry["place"] = html_fields["place"]

        wt = html_fields.get("work_type") or _work_type_from_html_for_link(html_text, lnk)
        if wt:
            entry["work_type"] = wt

        if html_fields.get("raw_text"):
            entry["raw_text"] = html_fields["raw_text"]

        entry["source"] = _provider_from_link(lnk)

    for raw_link in links:
        if not raw_link:
            continue
        normalized = _normalize_position_link(raw_link)
        if not _is_job_link(normalized):
            continue
        if _is_linkedin_reference_position_link(raw_link, normalized):
            continue
        if normalized in by_link:
            continue
        html_fields = html_by_link.get(normalized, {})
        ji_fields = jobindex_by_link.get(normalized, {})
        demant_fields = demant_by_link.get(normalized, {})
        company, title = extract_company_title(text, title_hint)
        wt = html_fields.get("work_type") or _work_type_from_html_for_link(html_text, normalized)
        by_link[normalized] = {
            "company": demant_fields.get("company") or ji_fields.get("company") or html_fields.get("company") or company,
            "title": demant_fields.get("title") or ji_fields.get("title") or html_fields.get("title") or title,
            "place": demant_fields.get("place") or ji_fields.get("place") or html_fields.get("place") or "",
            "work_type": demant_fields.get("work_type") or (wt if wt else "Unknown"),
            "position_link": normalized,
            "raw_text": demant_fields.get("raw_text") or ji_fields.get("raw_text") or html_fields.get("raw_text") or text[:2500],
            "description_raw": demant_fields.get("description_raw") or ji_fields.get("description_raw") or "",
            "source": demant_fields.get("source") or _provider_from_link(normalized),
        }

    filtered_entries: List[Dict] = []
    for entry in by_link.values():
        if "description_raw" not in entry:
            entry["description_raw"] = ""
        if "source" not in entry:
            entry["source"] = _provider_from_link(entry.get("position_link", ""))
        if _is_linkedin_boilerplate_entry(entry):
            continue
        filtered_entries.append(entry)

    return filtered_entries


def upsert_job(db_path: str, job: Dict) -> bool:
    now = datetime.utcnow().isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        position_link = job.get("position_link", "")
        cur.execute("SELECT 1 FROM jobs WHERE position_link=? LIMIT 1", (position_link,))
        is_new_record = cur.fetchone() is None
        if is_new_record:
            cur.execute(
                """
                            INSERT INTO jobs (source, company, title, place, work_type, position_link, raw_text, description_raw, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.get("source") or _provider_from_link(position_link),
                    job.get("company", ""),
                    job.get("title", ""),
                                    job.get("place", ""),
                                    job.get("work_type", "Unknown"),
                    position_link,
                    job.get("raw_text", ""),
                                    "",
                    now,
                    now,
                ),
            )
            conn.commit()
        return is_new_record
    finally:
        conn.close()


def score_relevance(text: str, profile: Dict) -> Tuple[float, str, int, str]:
    include = [k.lower().strip() for k in profile.get("include_keywords", []) if k.strip()]
    exclude = [k.lower().strip() for k in profile.get("exclude_keywords", []) if k.strip()]
    min_score = float(profile.get("min_score", 1))

    corpus = text.lower()
    score = 0.0
    hit_inc = []
    hit_exc = []

    for kw in include:
        if kw in corpus:
            score += 1.5
            hit_inc.append(kw)
    for kw in exclude:
        if kw in corpus:
            score -= 2.0
            hit_exc.append(kw)

    relevant = 1 if score >= min_score else 0
    if score >= min_score:
        category = "relevant"
    else:
        category = "not relevant"

    reason = f"score={score:.1f}; include={hit_inc[:6]}; exclude={hit_exc[:6]}"
    return score, reason, relevant, category


def apply_relevance(db_path: str, profile: Dict, prune_irrelevant: bool = False) -> Tuple[int, int]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, title, company, raw_text, relevance_reason FROM jobs")
        rows = cur.fetchall()
        relevant_count = 0

        for rid, title, company, raw_text, relevance_reason in rows:
            manual_reason = (relevance_reason or "").strip().lower()
            if manual_reason == "manual_feedback=relevant":
                relevant_count += 1
                continue
            if manual_reason == "manual_feedback=not relevant":
                continue

            composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
            score, reason, relevant, category = score_relevance(composed, profile)
            cur.execute(
                "UPDATE jobs SET relevance_score=?, relevance_reason=?, relevant=?, category=?, updated_at=? WHERE id=?",
                (score, reason, relevant, category, datetime.utcnow().isoformat(), rid),
            )
            if relevant:
                relevant_count += 1

        if prune_irrelevant:
            cur.execute("DELETE FROM jobs WHERE category='not relevant'")

        conn.commit()
        return len(rows), relevant_count
    finally:
        conn.close()


def get_relevant_jobs(db_path: str, limit: int = 0) -> List[Dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = "SELECT id, company, title, place, work_type, position_link, raw_text, relevance_score FROM jobs WHERE relevant=1 ORDER BY relevance_score DESC, updated_at DESC"
        if limit and limit > 0:
            q += f" LIMIT {int(limit)}"
        cur.execute(q)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": _provider_from_link(r[5] or ""),
                "company": r[1] or "",
                "title": r[2] or "",
                "place": r[3] or "",
                "work_type": r[4] or "Unknown",
                "position_link": r[5] or "",
                "raw_text": r[6] or "",
                "relevance_score": float(r[7] or 0),
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_by_category(db_path: str, category: str, limit: int = 0, unviewed_only: bool = False) -> List[Dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description_raw, description "
            "FROM jobs WHERE category=?"
        )
        params = [category]
        if unviewed_only:
            q += " AND viewed=0"
        q += " ORDER BY relevance_score DESC, updated_at DESC"
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[6] or "")) if len(r) > 1 else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "place": r[4] or "",
                "work_type": r[5] or "Unknown",
                "position_link": r[6] or "",
                "raw_text": r[7] or "",
                "relevance_score": float(r[8] or 0),
                "relevance_reason": r[9] or "",
                "summary": r[10] or "",
                "viewed": int(r[11] or 0),
                "applied": int(r[12] or 0),
                "description_raw": r[13] or "",
                "description": r[14] or "",
                "category": category,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_applied_jobs(db_path: str, limit: int = 0) -> List[Dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description_raw, description, category "
            "FROM jobs WHERE applied=1 ORDER BY updated_at DESC"
        )
        params: List = []
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[6] or "")) if len(r) > 1 else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "place": r[4] or "",
                "work_type": r[5] or "Unknown",
                "position_link": r[6] or "",
                "raw_text": r[7] or "",
                "relevance_score": float(r[8] or 0),
                "relevance_reason": r[9] or "",
                "summary": r[10] or "",
                "viewed": int(r[11] or 0),
                "applied": int(r[12] or 0),
                "description_raw": r[13] or "",
                "description": r[14] or "",
                "category": r[15] or "relevant",
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_jobs_for_description_refresh(
    db_path: str,
    category: str = "",
    source: str = "",
    links: List[str] = None,
    job_ids: List[int] = None,
    limit: int = 0,
    missing_only: bool = True,
) -> List[Dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, category, description_raw, description "
            "FROM jobs WHERE 1=1"
        )
        params: List = []

        q += " AND (applied IS NULL OR applied=0)"

        if category:
            q += " AND category=?"
            params.append(category)

        if source:
            q += " AND LOWER(source)=LOWER(?)"
            params.append(source)

        if links:
            normalized_links = [_normalize_position_link(link) for link in links if (link or "").strip()]
            if normalized_links:
                placeholders = ",".join(["?"] * len(normalized_links))
                q += f" AND position_link IN ({placeholders})"
                params.extend(normalized_links)

        if job_ids:
            normalized_ids = [int(job_id) for job_id in job_ids]
            if normalized_ids:
                placeholders = ",".join(["?"] * len(normalized_ids))
                q += f" AND id IN ({placeholders})"
                params.extend(normalized_ids)

        if missing_only:
            q += " AND (description IS NULL OR TRIM(description)='')"

        q += " ORDER BY updated_at DESC"
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))

        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[6] or "")) if len(r) > 1 else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "place": r[4] or "",
                "work_type": r[5] or "Unknown",
                "position_link": r[6] or "",
                "raw_text": r[7] or "",
                "category": r[8] or "",
                "description_raw": r[9] or "",
                "description": r[10] or "",
            }
            for r in rows
        ]
    finally:
        conn.close()


def set_job_summary(db_path: str, job_id: int, summary: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET summary=?, updated_at=? WHERE id=?",
            (summary, datetime.utcnow().isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_description(db_path: str, job_id: int, description: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET description=?, updated_at=? WHERE id=?",
            (description, datetime.utcnow().isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_description_raw(db_path: str, job_id: int, description_raw: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET description_raw=?, updated_at=? WHERE id=?",
            ("", datetime.utcnow().isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def set_job_feedback(db_path: str, job_id: int, signal: str) -> bool:
    normalized = (signal or "").strip().lower()
    if normalized not in {"relevant", "not relevant"}:
        raise ValueError(f"Unsupported signal: {signal}")

    relevant = 1 if normalized == "relevant" else 0
    now = datetime.utcnow().isoformat()

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        applied = 0 if normalized == "not relevant" else None
        cur.execute(
            """
            UPDATE jobs
            SET relevant=?, category=?, relevance_reason=?, applied=COALESCE(?, applied), updated_at=?
            WHERE id=?
            """,
            (relevant, normalized, f"manual_feedback={normalized}", applied, now, int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_viewed(db_path: str, job_id: int, viewed: bool) -> bool:
    now = datetime.utcnow().isoformat()
    viewed_int = 1 if viewed else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE jobs SET viewed=?, applied=CASE WHEN ?=0 THEN 0 ELSE applied END, updated_at=? WHERE id=?",
            (viewed_int, viewed_int, now, int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_applied(db_path: str, job_id: int, applied: bool) -> bool:
    now = datetime.utcnow().isoformat()
    applied_int = 1 if applied else 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        if applied_int == 1:
            cur.execute(
                """
                UPDATE jobs
                SET applied=1, viewed=1, relevant=1, category='relevant', relevance_reason='manual_feedback=relevant', updated_at=?
                WHERE id=?
                """,
                (now, int(job_id)),
            )
        else:
            cur.execute(
                "UPDATE jobs SET applied=0, updated_at=? WHERE id=?",
                (now, int(job_id)),
            )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def ingest_docs_to_db(
    db_path: str,
    docs: List[Dict],
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
) -> Dict[str, int]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    for doc in docs:
        entries = extract_job_entries(doc)
        for entry in entries:
            if not entry.get("position_link"):
                continue
            is_new_record = upsert_job(db_path, entry)
            if is_new_record and on_new_record:
                on_new_record()
            if is_new_record:
                inserted_new += 1
            else:
                skipped_existing += 1
            processed += 1
            if on_progress:
                on_progress(processed, inserted_new, skipped_existing)
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
    }
