import json
import os
import re
import sqlite3
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

DEFAULT_PROFILE_FILE = "default_profile.json"

FALLBACK_DEFAULT_PROFILE = {
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
    "report_max_positions": 7,
    "skill_learning_max_positions": 180,
    "skill_learning_min_occurrences": 3,
    "skill_learning_max_new_patterns": 20,
    "skill_match_weight": 1.2,
    "skill_missing_penalty": 0.15,
    "easy_apply_bonus": 0.75,
    "missing_skills_max_items": 25,
    "skill_new_confidence_threshold": 0.9,
    "skill_new_max_per_job": 2,
    "user_skills": [],
    "blocked_skills": [],
    "missing_skills_suggestions": [],
    "known_skill_patterns": [],
}


def _default_profile_file_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_PROFILE_FILE)


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_default_profile() -> dict:
    profile = FALLBACK_DEFAULT_PROFILE.copy()

    file_path = _default_profile_file_path()
    try:
        with open(file_path, encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            profile.update(loaded)
    except Exception:
        pass

    list_fields = [
        "include_keywords",
        "exclude_keywords",
        "learned_include_keywords",
        "learned_exclude_keywords",
        "user_skills",
        "blocked_skills",
        "missing_skills_suggestions",
    ]
    for field in list_fields:
        if not isinstance(profile.get(field), list):
            profile[field] = list(FALLBACK_DEFAULT_PROFILE.get(field, []))

    profile["min_score"] = _safe_float(
        profile.get("min_score"), FALLBACK_DEFAULT_PROFILE["min_score"]
    )
    profile["max_input_chars"] = _safe_int(
        profile.get("max_input_chars"), FALLBACK_DEFAULT_PROFILE["max_input_chars"]
    )
    profile["server_port"] = _safe_int(
        profile.get("server_port"), FALLBACK_DEFAULT_PROFILE["server_port"]
    )
    profile["report_max_positions"] = _safe_int(
        profile.get("report_max_positions"),
        FALLBACK_DEFAULT_PROFILE["report_max_positions"],
    )
    profile["skill_learning_max_positions"] = _safe_int(
        profile.get("skill_learning_max_positions"),
        FALLBACK_DEFAULT_PROFILE["skill_learning_max_positions"],
    )
    profile["skill_learning_min_occurrences"] = _safe_int(
        profile.get("skill_learning_min_occurrences"),
        FALLBACK_DEFAULT_PROFILE["skill_learning_min_occurrences"],
    )
    profile["skill_learning_max_new_patterns"] = _safe_int(
        profile.get("skill_learning_max_new_patterns"),
        FALLBACK_DEFAULT_PROFILE["skill_learning_max_new_patterns"],
    )
    profile["skill_match_weight"] = _safe_float(
        profile.get("skill_match_weight"), FALLBACK_DEFAULT_PROFILE["skill_match_weight"]
    )
    profile["skill_missing_penalty"] = _safe_float(
        profile.get("skill_missing_penalty"),
        FALLBACK_DEFAULT_PROFILE["skill_missing_penalty"],
    )
    profile["easy_apply_bonus"] = _safe_float(
        profile.get("easy_apply_bonus"),
        FALLBACK_DEFAULT_PROFILE["easy_apply_bonus"],
    )
    profile["missing_skills_max_items"] = _safe_int(
        profile.get("missing_skills_max_items"),
        FALLBACK_DEFAULT_PROFILE["missing_skills_max_items"],
    )
    profile["skill_new_confidence_threshold"] = _safe_float(
        profile.get("skill_new_confidence_threshold"),
        FALLBACK_DEFAULT_PROFILE["skill_new_confidence_threshold"],
    )
    profile["skill_new_max_per_job"] = _safe_int(
        profile.get("skill_new_max_per_job"),
        FALLBACK_DEFAULT_PROFILE["skill_new_max_per_job"],
    )

    default_path_fields = [
        "default_inbox",
        "default_db",
        "default_report_dir",
        "default_model",
        "server_host",
    ]
    for field in default_path_fields:
        value = profile.get(field, FALLBACK_DEFAULT_PROFILE.get(field, ""))
        profile[field] = str(value) if value is not None else ""

    raw_patterns = profile.get("known_skill_patterns")
    clean_patterns = []
    if isinstance(raw_patterns, list):
        for item in raw_patterns:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "")).strip()
            pattern = str(item.get("pattern", "")).strip()
            if not name or not pattern:
                continue
            clean_patterns.append({"name": name, "pattern": pattern})
    profile["known_skill_patterns"] = clean_patterns

    return profile


DEFAULT_PROFILE = _load_default_profile()


LEARNING_STOPWORDS = {
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "also",
    "and",
    "any",
    "are",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "can",
    "company",
    "could",
    "danish",
    "denmark",
    "developer",
    "email",
    "for",
    "from",
    "have",
    "into",
    "job",
    "jobs",
    "just",
    "more",
    "not",
    "our",
    "out",
    "position",
    "role",
    "than",
    "that",
    "the",
    "their",
    "them",
    "there",
    "these",
    "this",
    "those",
    "through",
    "under",
    "using",
    "very",
    "want",
    "when",
    "where",
    "which",
    "with",
    "you",
    "your",
}

SQLITE_TIMEOUT_SECONDS = 30.0
SQLITE_BUSY_TIMEOUT_MS = 30000
JOB_RETENTION_DAYS = 90
EASY_APPLY_PATTERN = re.compile(r"\beasy\s*apply\b", flags=re.IGNORECASE)

TITLE_GARBAGE_MARKERS = [
    "translated title",
    "translated title text",
    "original title",
    "original text",
    "english title",
    "english translation",
    "translation result",
    "translation:",
    "return value",
    "return only",
    "unchanged title",
    "step 1",
    "you are an ai assistant",
    "translate this job title to english",
    "the english title",
    "this translated title",
    "note:",
]


def _normalize_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def sanitize_job_title(title: str) -> str:
    clean = " ".join((title or "").split()).strip().strip('"\'“”')
    if not clean:
        return ""

    low = clean.lower()
    cut_positions = [low.find(marker) for marker in TITLE_GARBAGE_MARKERS if marker in low]
    if cut_positions:
        clean = clean[: min(cut_positions)].rstrip(" -|:;,/")

    while clean.count("(") > clean.count(")") and "(" in clean:
        clean = clean.rsplit("(", 1)[0].rstrip(" -|:;,/")
    while clean.count("[") > clean.count("]") and "[" in clean:
        clean = clean.rsplit("[", 1)[0].rstrip(" -|:;,/")

    clean = clean.strip().strip('"\'“”').rstrip(" -|:;,/")

    match = re.match(r"^(?P<outer>.+?)\s*\((?P<inner>.+?)\)$", clean)
    if match:
        outer = (match.group("outer") or "").strip()
        inner = (match.group("inner") or "").strip()
        if outer and inner and _normalize_title_key(outer) == _normalize_title_key(inner):
            clean = outer

    return clean[:180]


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _unique_keywords(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        key = (value or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _tokenize_learning_text(text: str) -> list[str]:
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


def _normalize_skill_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _blocked_skill_keys(profile: Optional[dict]) -> set[str]:
    values = (profile or {}).get("blocked_skills") or []
    return {
        _normalize_skill_key(str(item))
        for item in values
        if _normalize_skill_key(str(item))
    }


def _profile_skill_patterns(profile: dict) -> list[tuple[str, str]]:
    raw = profile.get("known_skill_patterns") or []
    blocked_keys = _blocked_skill_keys(profile)
    out: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not name or not pattern or _normalize_skill_key(name) in blocked_keys:
            continue
        out.append((name, pattern))
    return out


def _extract_required_skills_from_text(
    text: str, skill_patterns: list[tuple[str, str]]
) -> list[str]:
    low = (text or "").lower()
    if not low or not skill_patterns:
        return []

    hits: list[tuple[int, str]] = []
    for name, pattern in skill_patterns:
        try:
            m = re.search(pattern, low, flags=re.IGNORECASE)
        except re.error:
            continue
        if m:
            hits.append((m.start(), name.strip()))

    hits.sort(key=lambda x: x[0])
    out: list[str] = []
    seen = set()
    for _, name in hits:
        key = _normalize_skill_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _suggest_keywords_from_labeled_jobs(
    db_path: str, max_keywords: int = 20
) -> tuple[list[str], list[str], int]:
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
        text = "\n".join(
            [
                title or "",
                company or "",
                place or "",
                work_type or "",
                raw_text or "",
            ]
        )
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
    learned_exclude = [
        token
        for _, _, token in exclude_ranked[:max_keywords]
        if token not in learned_include
    ]
    return learned_include, learned_exclude, total_labeled


def _suggest_missing_skills_from_applied_jobs(
    db_path: str, profile: dict, max_items: int = 25
) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT title, summary, raw_text
            FROM jobs
            WHERE applied=1
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    skill_patterns = _profile_skill_patterns(profile)
    if not skill_patterns:
        return []

    user_skills = {
        _normalize_skill_key(s)
        for s in (profile.get("user_skills") or [])
        if _normalize_skill_key(str(s))
    }
    blocked_skills = _blocked_skill_keys(profile)

    freq: Counter = Counter()
    display_by_key: dict[str, str] = {}
    for title, summary, raw_text in rows:
        text = "\n".join([(title or ""), (summary or ""), (raw_text or "")])
        skills = _extract_required_skills_from_text(text, skill_patterns)
        for skill in skills:
            key = _normalize_skill_key(skill)
            if not key or key in user_skills or key in blocked_skills:
                continue
            display_by_key.setdefault(key, skill)
            freq[key] += 1

    ordered = [
        display_by_key.get(name, name) for name, _ in freq.most_common(max_items)
    ]
    return ordered[:max_items]


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
                        (source, company, title, place, work_type, position_link, raw_text, description, viewed, applied, relevance_score, relevant, category, relevance_reason, summary, created_at, updated_at)
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
                cur.execute("PRAGMA table_info(jobs)")
                cols = {row[1] for row in cur.fetchall()}

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
            if "description" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN description TEXT")

            cur.execute("PRAGMA table_info(jobs)")
            cols = {row[1] for row in cur.fetchall()}
            if "description_raw" in cols:
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
                        (id, source, company, title, place, work_type, position_link, raw_text, description, viewed, applied, relevance_score, relevant, category, relevance_reason, summary, created_at, updated_at)
                    SELECT
                        id,
                        source,
                        company,
                        title,
                        place,
                        work_type,
                        position_link,
                        raw_text,
                        description,
                        viewed,
                        applied,
                        relevance_score,
                        relevant,
                        category,
                        relevance_reason,
                        summary,
                        created_at,
                        updated_at
                    FROM jobs
                    """
                )
                cur.execute("DROP TABLE jobs")
                cur.execute("ALTER TABLE jobs_new RENAME TO jobs")

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
            CREATE TABLE IF NOT EXISTS skill_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                name_key TEXT UNIQUE NOT NULL,
                pattern TEXT NOT NULL,
                source TEXT DEFAULT 'seed',
                occurrences INTEGER DEFAULT 0,
                weight REAL DEFAULT 0,
                enabled INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_at TEXT
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS job_skills (
                job_id INTEGER NOT NULL,
                skill_id INTEGER NOT NULL,
                PRIMARY KEY (job_id, skill_id),
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                FOREIGN KEY (skill_id) REFERENCES skill_patterns(id) ON DELETE CASCADE
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
                OR (
                    lower(position_link) LIKE '%jobs.danfoss.com%'
                    AND lower(position_link) LIKE '%/job/%'
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

        # Auto-prune old positions by creation date to keep DB focused on recent jobs.
        cur.execute(
            """
            DELETE FROM jobs
            WHERE created_at IS NOT NULL
              AND TRIM(created_at) <> ''
              AND datetime(replace(created_at, 'T', ' ')) < datetime('now', ?)
            """,
            (f"-{int(JOB_RETENTION_DAYS)} days",),
        )

        cur.execute(
            "UPDATE jobs SET work_type='Unknown' WHERE work_type IS NULL OR work_type='' "
        )
        cur.execute("UPDATE jobs SET viewed=0 WHERE viewed IS NULL")
        cur.execute("UPDATE jobs SET applied=0 WHERE applied IS NULL")
        cur.execute("UPDATE jobs SET source='' WHERE source IS NULL")
        cur.execute("UPDATE jobs SET description='' WHERE description IS NULL")

        cur.execute("SELECT id, title FROM jobs")
        for rid, title in cur.fetchall():
            cleaned_title = sanitize_job_title(title or "")
            if cleaned_title and cleaned_title != (title or ""):
                cur.execute(
                    "UPDATE jobs SET title=?, updated_at=? WHERE id=?",
                    (cleaned_title, datetime.now(timezone.utc).isoformat(), rid),
                )

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
                cur.execute(
                    "UPDATE jobs SET position_link=? WHERE id=?", (norm, keep_id)
                )

        cur.execute("SELECT id, position_link, source FROM jobs")
        for rid, link, source in cur.fetchall():
            provider = _provider_from_link(link or "")
            if provider and (not source or source.strip() != provider):
                cur.execute("UPDATE jobs SET source=? WHERE id=?", (provider, rid))

        conn.commit()
    finally:
        conn.close()


def _normalize_skill_name_key(name: str) -> str:
    return " ".join((name or "").strip().lower().split())


def get_skill_patterns(db_path: str, enabled_only: bool = True) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT name, pattern, source, occurrences, weight, enabled, last_seen_at "
            "FROM skill_patterns"
        )
        params: list = []
        if enabled_only:
            q += " WHERE enabled=1"
        q += " ORDER BY weight DESC, occurrences DESC, name ASC"
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "name": r[0] or "",
                "pattern": r[1] or "",
                "source": r[2] or "",
                "occurrences": int(r[3] or 0),
                "weight": float(r[4] or 0),
                "enabled": int(r[5] or 0),
                "last_seen_at": r[6] or "",
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_skill_pattern(
    db_path: str,
    name: str,
    pattern: str,
    source: str = "learned",
    occurrences_inc: int = 0,
    weight_inc: float = 0.0,
    enabled: bool = True,
) -> bool:
    name_clean = (name or "").strip()
    pattern_clean = (pattern or "").strip()
    name_key = _normalize_skill_name_key(name_clean)
    if not name_clean or not name_key or not pattern_clean:
        return False

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO skill_patterns
                (name, name_key, pattern, source, occurrences, weight, enabled, created_at, updated_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name_key) DO UPDATE SET
                name=excluded.name,
                pattern=CASE
                    WHEN excluded.pattern IS NOT NULL AND TRIM(excluded.pattern)<>'' THEN excluded.pattern
                    ELSE skill_patterns.pattern
                END,
                source=CASE
                    WHEN skill_patterns.source IS NULL OR TRIM(skill_patterns.source)='' THEN excluded.source
                    ELSE skill_patterns.source
                END,
                occurrences=skill_patterns.occurrences + excluded.occurrences,
                weight=skill_patterns.weight + excluded.weight,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at,
                last_seen_at=excluded.last_seen_at
            """,
            (
                name_clean,
                name_key,
                pattern_clean,
                source,
                max(0, int(occurrences_inc)),
                max(0.0, float(weight_inc)),
                1 if enabled else 0,
                now,
                now,
                now,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def migrate_profile_skill_patterns_to_db(
    db_path: str, profile_path: str
) -> dict[str, int]:
    ensure_db(db_path)

    profile: dict = {}
    if profile_path and os.path.exists(profile_path):
        try:
            with open(profile_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                profile = loaded
        except Exception:
            profile = {}

    raw = profile.get("known_skill_patterns")
    if not isinstance(raw, list) or not raw:
        raw = DEFAULT_PROFILE.get("known_skill_patterns") or []

    inserted = 0
    seed_count = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not name or not pattern:
            continue
        seed_count += 1
        if upsert_skill_pattern(
            db_path,
            name=name,
            pattern=pattern,
            source="profile_seed",
            occurrences_inc=0,
            weight_inc=0.0,
            enabled=True,
        ):
            inserted += 1

    return {"seed_count": int(seed_count), "inserted": int(inserted)}


def load_profile(profile_path: Optional[str]) -> dict:
    if not profile_path:
        return DEFAULT_PROFILE.copy()
    with open(profile_path, encoding="utf-8") as f:
        data = json.load(f)
    profile = DEFAULT_PROFILE.copy()
    profile.update(data)

    base_include = profile.get("include_keywords", []) or []
    base_exclude = profile.get("exclude_keywords", []) or []
    learned_include = profile.get("learned_include_keywords", []) or []
    learned_exclude = profile.get("learned_exclude_keywords", []) or []

    profile["include_keywords"] = _unique_keywords(
        list(base_include) + list(learned_include)
    )
    profile["exclude_keywords"] = _unique_keywords(
        list(base_exclude) + list(learned_exclude)
    )
    profile["learned_include_keywords"] = _unique_keywords(list(learned_include))
    profile["learned_exclude_keywords"] = _unique_keywords(list(learned_exclude))
    profile["user_skills"] = _unique_keywords(
        list(profile.get("user_skills", []) or [])
    )
    profile["blocked_skills"] = _unique_keywords(
        list(profile.get("blocked_skills", []) or [])
    )
    profile["missing_skills_suggestions"] = _unique_keywords(
        list(profile.get("missing_skills_suggestions", []) or [])
    )
    return profile


def update_profile_from_db_signals(
    db_path: str, profile_path: str, max_keywords: int = 20
) -> dict[str, int]:
    profile = DEFAULT_PROFILE.copy()
    if profile_path and os.path.exists(profile_path):
        with open(profile_path, encoding="utf-8") as f:
            existing = json.load(f)
        profile.update(existing)

    learned_include, learned_exclude, labeled_count = (
        _suggest_keywords_from_labeled_jobs(db_path, max_keywords=max_keywords)
    )
    profile["learned_include_keywords"] = learned_include
    profile["learned_exclude_keywords"] = learned_exclude

    max_missing_items = int(profile.get("missing_skills_max_items", 25) or 25)
    missing_skills = _suggest_missing_skills_from_applied_jobs(
        db_path, profile, max_items=max_missing_items
    )
    profile["missing_skills_suggestions"] = missing_skills

    os.makedirs(os.path.dirname(os.path.abspath(profile_path)), exist_ok=True)
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)

    return {
        "labeled_count": int(labeled_count),
        "learned_include_count": len(learned_include),
        "learned_exclude_count": len(learned_exclude),
        "missing_skills_count": len(missing_skills),
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
            nearest_distance = min(
                abs(token_pos - link_pos) for token_pos in token_positions
            )
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


def _parse_card_text_fields(card_text: str) -> dict[str, str]:
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
        if wt_low == "onsite" or wt_low == "on-site":
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


def _parse_anchor_fragments(fragments: list[str]) -> dict[str, str]:
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

        wt_match = re.search(
            r"\((Hybrid|Remote|On-site|Onsite)\)", right, flags=re.IGNORECASE
        )
        if wt_match:
            wt = wt_match.group(1).lower()
            if wt in ("on-site", "onsite"):
                work_type = "On-site"
            elif wt == "hybrid":
                work_type = "Hybrid"
            elif wt == "remote":
                work_type = "Remote"
            right = re.sub(
                r"\((Hybrid|Remote|On-site|Onsite)\)", "", right, flags=re.IGNORECASE
            ).strip()

        place = right.strip(" -|:")[:180]

    if not place and " - " in title:
        maybe_title, maybe_place = title.rsplit(" - ", 1)
        if maybe_title and maybe_place:
            maybe_place_low = maybe_place.lower()
            if (
                "," in maybe_place
                or maybe_place_low.endswith(" dk")
                or "denmark" in maybe_place_low
            ):
                title = maybe_title.strip(" -|:")[:180]
                place = maybe_place.strip(" -|:")[:180]

    return {
        "title": title,
        "company": company,
        "place": place,
        "work_type": work_type,
    }


def _extract_html_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}
    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    def field_score(fields: dict[str, str], has_detail: bool) -> tuple[int, int, int]:
        count = sum(
            1 for key in ["title", "company", "place", "work_type"] if fields.get(key)
        )
        richness = (
            len(fields.get("title", ""))
            + len(fields.get("company", ""))
            + len(fields.get("place", ""))
        )
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
            if (
                node.name in ("tr", "table", "li", "div", "td")
                and 30 <= len(txt) <= 900
            ):
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
        if not current or field_score(fields, has_detail) > field_score(
            current, current_has_detail
        ):
            by_link[normalized] = fields

    for value in by_link.values():
        value.pop("_has_detail", None)

    return by_link


def first_non_empty(lines: list[str]) -> str:
    for line in lines:
        cleaned = line.strip()
        if cleaned:
            return cleaned
    return ""


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

    return company[:180], title[:180]


def _is_job_link(link: str) -> bool:
    low = link.lower()
    if "linkedin.com/comm/jobs/view/" in low or "linkedin.com/jobs/view/" in low:
        return True
    if re.search(
        r"(?:careers\.google\.com|google\.com)/.+/jobs/results/\d+",
        low,
    ):
        return True
    if "jobindex.dk" in low and (
        "jobid=" in low
        or re.search(r"/jobannonce/[hr]\d+", low)
        or re.search(r"/bruger/dine-job/[hr]\d+", low)
    ):
        return True
    if "careers.demant.com" in low and "/job/" in low:
        return True
    if "jobs.danfoss.com" in low and "/job/" in low:
        return True
    if "jobs.teradyne.com" in low and "/job/" in low:
        return True
    if "careers.nttdata-solutions.com" in low and "/job/" in low:
        return True
    if "careers.getinge.com" in low and "/job/" in low:
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

    base = (
        f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.scheme and parsed.netloc
        else link
    )
    return base.rstrip("/")


def _provider_from_link(link: str) -> str:
    low = (link or "").lower()
    if "linkedin.com" in low:
        return "LinkedIn"
    if "careers.google.com" in low or "google.com/about/careers/applications/jobs/results/" in low:
        return "Google Careers"
    if "jobindex.dk" in low:
        return "Jobindex"
    if "careers.demant.com" in low:
        return "Demant"
    if "jobs.danfoss.com" in low:
        return "Danfoss"
    if "jobs.teradyne.com" in low:
        return "Teradyne"
    if "careers.nttdata-solutions.com" in low:
        return "NTT DATA Business Solutions"
    if "careers.getinge.com" in low:
        return "Getinge"

    parsed = urlparse(link)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "Unknown"


def _has_easy_apply_signal(text: str) -> bool:
    compact = " ".join((text or "").split())
    return bool(compact and EASY_APPLY_PATTERN.search(compact))


def _has_linkedin_public_easy_apply(
    position_link: str, easy_apply_cache: Optional[dict[str, bool]] = None
) -> bool:
    link = (position_link or "").strip()
    if not link or "linkedin.com/" not in link.lower():
        return False
    if easy_apply_cache is not None and link in easy_apply_cache:
        return bool(easy_apply_cache[link])

    req = Request(
        link,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
    )

    has_easy_apply = False
    try:
        with urlopen(req, timeout=8) as response:
            ctype = (response.headers.get("Content-Type") or "").lower()
            if not ctype or "html" in ctype or "text" in ctype:
                payload = response.read()
                charset = response.headers.get_content_charset() or "utf-8"
                html_text = payload.decode(charset, errors="ignore")
                has_easy_apply = "public_jobs_apply-link-onsite" in html_text.lower()
    except (HTTPError, URLError, TimeoutError, ValueError):
        has_easy_apply = False
    except Exception:
        has_easy_apply = False

    if easy_apply_cache is not None:
        easy_apply_cache[link] = has_easy_apply
    return has_easy_apply


def _is_linkedin_reference_position_link(raw_link: str, normalized_link: str) -> bool:
    low = (raw_link or "").lower()
    if "linkedin.com" not in low:
        return False

    parsed = urlparse(raw_link)
    q = parse_qs(parsed.query)
    reference_id = (
        q.get("referenceJobId", [""])[0] or q.get("referencejobid", [""])[0] or ""
    ).strip()
    if not reference_id or not reference_id.isdigit():
        return False

    m = re.search(
        r"linkedin\.com/(?:comm/)?jobs/view/(\d+)", (normalized_link or "").lower()
    )
    if not m:
        return False

    return m.group(1) == reference_id


def _is_linkedin_boilerplate_entry(entry: dict) -> bool:
    source = (
        entry.get("source") or _provider_from_link(entry.get("position_link", ""))
    ).lower()
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

    return any(
        phrase in value
        for phrase in boilerplate_phrases
        for value in [title, company, place]
    )


def _extract_entries_from_text(text: str) -> list[dict]:
    lines = [ln.strip() for ln in text.splitlines()]
    entries = []

    def score_title(line: str) -> int:
        low = line.lower()
        keys = [
            "developer",
            "engineer",
            "specialist",
            "manager",
            "scientist",
            "lead",
            "architect",
            "analyst",
            "consultant",
            ".net",
            "software",
        ]
        return sum(1 for k in keys if k in low)

    def score_company(line: str) -> int:
        low = line.lower()
        keys = ["group", "inc", "aps", "a/s", "ltd", "gmbh", "company", "danmark"]
        return sum(1 for k in keys if k in low)

    def score_place(line: str) -> int:
        low = line.lower()
        keys = [
            "aarhus",
            "copenhagen",
            "odense",
            "lystrup",
            "humleb",
            "denmark",
            "municipality",
        ]
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
            if (
                candidate
                and "http" not in clow
                and not any(sp in clow for sp in stop_phrases)
                and "----" not in candidate
            ):
                candidates.append(candidate)

        title = ""
        company = ""
        place = ""
        if candidates:
            by_title = sorted(
                candidates, key=lambda s: (score_title(s), len(s)), reverse=True
            )
            by_company = sorted(
                candidates, key=lambda s: (score_company(s), len(s)), reverse=True
            )
            by_place = sorted(
                candidates, key=lambda s: (score_place(s), -len(s)), reverse=True
            )

            title = (
                by_title[0]
                if score_title(by_title[0]) > 0
                else (candidates[-3] if len(candidates) >= 3 else candidates[0])
            )
            company = (
                by_company[0]
                if score_company(by_company[0]) > 0
                else (candidates[-2] if len(candidates) >= 2 else "")
            )
            place = (
                by_place[0]
                if score_place(by_place[0]) > 0
                else (candidates[-1] if len(candidates) >= 3 else "")
            )

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

        local_chunk = " ".join(
            lines[max(0, idx - 20) : min(len(lines), idx + 5)]
        ).lower()
        if "remote" in local_chunk:
            work_type = "Remote"
        elif "hybrid" in local_chunk:
            work_type = "Hybrid"
        elif "on-site" in local_chunk or "onsite" in local_chunk or place:
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
            }
        )

    return entries


def _extract_jobindex_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

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
            if (
                t2 == job_id
                and txt2
                and txt2.lower()
                not in {
                    "view job",
                    "apply",
                    "about the company",
                    "save job",
                    "settings",
                }
            ):
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
            m_place = re.search(
                re.escape(title) + r"\s+(.{2,80}?)\s+\d+\s+min\b",
                compact,
                flags=re.IGNORECASE,
            )
            if m_place:
                place = m_place.group(1).strip(" -|:")[:180]

        m_desc = re.search(
            r"settings\s*\)\s*(.*?)\s*PUBLISHED\s*:", compact, flags=re.IGNORECASE
        )
        extracted = ""
        if m_desc:
            extracted = m_desc.group(1).strip()
        else:
            m_desc2 = re.search(
                r"\d+\s+min\s*\(.*?\)\s*(.*?)\s*PUBLISHED\s*:",
                compact,
                flags=re.IGNORECASE,
            )
            if m_desc2:
                extracted = m_desc2.group(1).strip()

        raw_text = compact[:2500]
        if extracted:
            merged = f"{extracted}\n\n{raw_text}".strip()
            raw_text = merged[:2500]

        by_link[normalized] = {
            "title": title,
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": raw_text,
            "source": "Jobindex",
        }

    return by_link


def _extract_demant_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

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
            "source": "Demant",
        }

    return by_link


def _extract_danfoss_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        parsed = urlparse(href)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if "jobs.danfoss.com" not in host or "/job/" not in path:
            continue

        normalized = _normalize_position_link(href)
        if not normalized:
            continue

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        by_link[normalized] = {
            "title": title[:180],
            "company": "Danfoss",
            "place": "",
            "work_type": "Unknown",
            "raw_text": title[:2500],
            "source": "Danfoss",
        }

    return by_link


def _extract_google_entries_by_link(html_text: str) -> dict[str, dict[str, str]]:
    if not html_text:
        return {}

    soup = BeautifulSoup(html_text, "html.parser")
    by_link: dict[str, dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href") or ""
        normalized = _normalize_position_link(href)
        if not normalized or _provider_from_link(normalized) != "Google Careers":
            continue

        title = " ".join(anchor.get_text(" ", strip=True).split())
        if not title:
            continue

        block = anchor.find_parent("td") or anchor.find_parent("tr") or anchor.parent
        compact = " ".join(block.get_text(" ", strip=True).split()) if block else title
        compact = compact[:2500]

        company = "Google"
        place = ""

        suffix = compact[len(title) :].strip() if compact.startswith(title) else compact
        location_match = re.match(
            r"^(?P<company>[^–-]{2,80}?)\s*[–-]\s*(?P<place>.+?)(?:\s+\d+\s+(?:minute|minutes|hour|hours|day|days|week|weeks)\s+ago\b|$)",
            suffix,
            flags=re.IGNORECASE,
        )
        if location_match:
            company = (location_match.group("company") or company).strip(" -|:")[:180]
            place = (location_match.group("place") or "").strip(" -|:")[:180]

        by_link[normalized] = {
            "title": title[:180],
            "company": company,
            "place": place,
            "work_type": "Unknown",
            "raw_text": compact,
            "source": "Google Careers",
        }

    return by_link


def extract_job_entries(doc: dict) -> list[dict]:
    text = doc.get("text", "") or ""
    html_text = doc.get("html", "") or ""
    title_hint = doc.get("title", "") or ""
    links = doc.get("links", []) or []
    html_by_link = _extract_html_entries_by_link(html_text)
    jobindex_by_link = _extract_jobindex_entries_by_link(html_text)
    demant_by_link = _extract_demant_entries_by_link(html_text)
    danfoss_by_link = _extract_danfoss_entries_by_link(html_text)
    google_by_link = _extract_google_entries_by_link(html_text)

    by_text = _extract_entries_from_text(text)
    by_link = {}
    for entry in by_text:
        by_link[entry["position_link"]] = entry

    for lnk, entry in by_link.items():
        html_fields = html_by_link.get(lnk, {})
        ji_fields = jobindex_by_link.get(lnk, {})
        demant_fields = demant_by_link.get(lnk, {})
        danfoss_fields = danfoss_by_link.get(lnk, {})
        google_fields = google_by_link.get(lnk, {})

        if google_fields.get("title"):
            entry["title"] = google_fields["title"]
        if google_fields.get("company"):
            entry["company"] = google_fields["company"]
        if google_fields.get("place"):
            entry["place"] = google_fields["place"]
        if google_fields.get("work_type"):
            entry["work_type"] = google_fields["work_type"]
        if google_fields.get("raw_text"):
            entry["raw_text"] = google_fields["raw_text"]
        if google_fields.get("source"):
            entry["source"] = google_fields["source"]

        if danfoss_fields.get("title"):
            entry["title"] = danfoss_fields["title"]
        if danfoss_fields.get("company"):
            entry["company"] = danfoss_fields["company"]
        if danfoss_fields.get("place"):
            entry["place"] = danfoss_fields["place"]
        if danfoss_fields.get("work_type"):
            entry["work_type"] = danfoss_fields["work_type"]
        if danfoss_fields.get("raw_text"):
            entry["raw_text"] = danfoss_fields["raw_text"]
        if danfoss_fields.get("source"):
            entry["source"] = danfoss_fields["source"]

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
        if demant_fields.get("source"):
            entry["source"] = demant_fields["source"]

        if ji_fields.get("title"):
            entry["title"] = ji_fields["title"]
        if ji_fields.get("company"):
            entry["company"] = ji_fields["company"]
        if ji_fields.get("place"):
            entry["place"] = ji_fields["place"]
        if ji_fields.get("raw_text"):
            entry["raw_text"] = ji_fields["raw_text"]

        if html_fields.get("title"):
            entry["title"] = html_fields["title"]
        if html_fields.get("company"):
            entry["company"] = html_fields["company"]
        if html_fields.get("place"):
            entry["place"] = html_fields["place"]

        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, lnk
        )
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
        danfoss_fields = danfoss_by_link.get(normalized, {})
        google_fields = google_by_link.get(normalized, {})
        company, title = extract_company_title(text, title_hint)
        wt = html_fields.get("work_type") or _work_type_from_html_for_link(
            html_text, normalized
        )
        by_link[normalized] = {
            "company": google_fields.get("company")
            or danfoss_fields.get("company")
            or demant_fields.get("company")
            or ji_fields.get("company")
            or html_fields.get("company")
            or company,
            "title": google_fields.get("title")
            or danfoss_fields.get("title")
            or demant_fields.get("title")
            or ji_fields.get("title")
            or html_fields.get("title")
            or title,
            "place": google_fields.get("place")
            or danfoss_fields.get("place")
            or demant_fields.get("place")
            or ji_fields.get("place")
            or html_fields.get("place")
            or "",
            "work_type": google_fields.get("work_type")
            or danfoss_fields.get("work_type")
            or demant_fields.get("work_type")
            or (wt if wt else "Unknown"),
            "position_link": normalized,
            "raw_text": google_fields.get("raw_text")
            or danfoss_fields.get("raw_text")
            or demant_fields.get("raw_text")
            or ji_fields.get("raw_text")
            or html_fields.get("raw_text")
            or text[:2500],
            "source": google_fields.get("source")
            or danfoss_fields.get("source")
            or demant_fields.get("source")
            or _provider_from_link(normalized),
        }

    filtered_entries: list[dict] = []
    for entry in by_link.values():
        if "source" not in entry:
            entry["source"] = _provider_from_link(entry.get("position_link", ""))
        if entry.get("source") == "Getinge":
            entry["company"] = "Getinge"
        if _is_linkedin_boilerplate_entry(entry):
            continue
        filtered_entries.append(entry)

    return filtered_entries


def upsert_job(db_path: str, job: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        position_link = job.get("position_link", "")
        title = sanitize_job_title(job.get("title", ""))
        cur.execute(
            "SELECT 1 FROM jobs WHERE position_link=? LIMIT 1", (position_link,)
        )
        is_new_record = cur.fetchone() is None
        if is_new_record:
            cur.execute(
                """
                            INSERT INTO jobs (source, company, title, place, work_type, position_link, raw_text, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.get("source") or _provider_from_link(position_link),
                    job.get("company", ""),
                    title,
                    job.get("place", ""),
                    job.get("work_type", "Unknown"),
                    position_link,
                    job.get("raw_text", ""),
                    now,
                    now,
                ),
            )
            conn.commit()
        return is_new_record
    finally:
        conn.close()


def score_relevance(
    text: str,
    profile: dict,
    skill_patterns: Optional[list[tuple[str, str]]] = None,
    source: str = "",
    position_link: str = "",
    easy_apply_cache: Optional[dict[str, bool]] = None,
) -> tuple[float, str, int, str]:
    include = [
        k.lower().strip() for k in profile.get("include_keywords", []) if k.strip()
    ]
    exclude = [
        k.lower().strip() for k in profile.get("exclude_keywords", []) if k.strip()
    ]
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

    user_skills = {
        _normalize_skill_key(s)
        for s in (profile.get("user_skills") or [])
        if _normalize_skill_key(str(s))
    }
    extracted_required = _extract_required_skills_from_text(text, skill_patterns or [])
    required_keys = {_normalize_skill_key(s) for s in extracted_required}

    matched = sorted(
        [s for s in extracted_required if _normalize_skill_key(s) in user_skills]
    )
    missing = sorted(
        [s for s in extracted_required if _normalize_skill_key(s) not in user_skills]
    )

    skill_match_weight = float(profile.get("skill_match_weight", 1.2) or 1.2)
    skill_missing_penalty = float(profile.get("skill_missing_penalty", 0.15) or 0.15)

    if user_skills:
        score += float(len(matched)) * skill_match_weight
        score -= float(len(missing)) * skill_missing_penalty

    easy_apply_bonus = float(profile.get("easy_apply_bonus", 0.75) or 0.75)
    source_low = (source or "").strip().lower()
    link_low = (position_link or "").strip().lower()
    is_linkedin = source_low == "linkedin" or "linkedin.com/" in link_low
    has_easy_apply = bool(is_linkedin and _has_easy_apply_signal(text))
    if is_linkedin and not has_easy_apply:
        has_easy_apply = _has_linkedin_public_easy_apply(
            position_link, easy_apply_cache=easy_apply_cache
        )
    if has_easy_apply and easy_apply_bonus:
        score += easy_apply_bonus

    relevant = 1 if score >= min_score else 0
    category = "relevant" if score >= min_score else "not relevant"

    reason = (
        f"score={score:.1f}; include={hit_inc[:6]}; exclude={hit_exc[:6]}; "
        f"required_skills={list(required_keys)[:8]}; matched_skills={matched[:8]}; missing_skills={missing[:8]}; "
        f"easy_apply={has_easy_apply}; easy_apply_bonus={easy_apply_bonus if has_easy_apply else 0}"
    )
    return score, reason, relevant, category


def apply_relevance(
    db_path: str, profile: dict, prune_irrelevant: bool = False
) -> tuple[int, int]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, source, title, company, position_link, raw_text, relevance_reason FROM jobs"
        )
        rows = cur.fetchall()
        relevant_count = 0

        skill_pattern_rows = get_skill_patterns(db_path, enabled_only=True)
        if skill_pattern_rows:
            skill_patterns = [
                (
                    str(item.get("name", "")).strip(),
                    str(item.get("pattern", "")).strip(),
                )
                for item in skill_pattern_rows
                if str(item.get("name", "")).strip()
                and str(item.get("pattern", "")).strip()
            ]
        else:
            skill_patterns = _profile_skill_patterns(profile)

        easy_apply_cache: dict[str, bool] = {}
        pending_updates: list[tuple[int, float, str, int, str]] = []

        for rid, source, title, company, position_link, raw_text, relevance_reason in rows:
            manual_reason = (relevance_reason or "").strip().lower()
            if manual_reason == "manual_feedback=relevant":
                relevant_count += 1
                continue
            if manual_reason == "manual_feedback=not relevant":
                continue

            composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
            score, reason, relevant, category = score_relevance(
                composed,
                profile,
                skill_patterns=skill_patterns,
                source=source or "",
                position_link=position_link or "",
                easy_apply_cache=easy_apply_cache,
            )
            pending_updates.append((rid, score, reason, relevant, category))
            if relevant:
                relevant_count += 1

        now = datetime.now(timezone.utc).isoformat()
        for rid, score, reason, relevant, category in pending_updates:
            cur.execute(
                "UPDATE jobs SET relevance_score=?, relevance_reason=?, relevant=?, category=?, updated_at=? WHERE id=?",
                (score, reason, relevant, category, now, rid),
            )

        if prune_irrelevant:
            cur.execute("DELETE FROM jobs WHERE category='not relevant'")

        conn.commit()
        return len(rows), relevant_count
    finally:
        conn.close()


def rescore_job_by_id(db_path: str, profile: dict, job_id: int) -> bool:
    """Re-score one job and persist relevance score/reason.

    For applied jobs, keeps category/relevant as relevant while updating score/reason.
    """
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, title, company, position_link, raw_text, applied
            FROM jobs
            WHERE id=?
            """,
            (int(job_id),),
        )
        row = cur.fetchone()
        if not row:
            return False

        rid, source, title, company, position_link, raw_text, applied = row

        skill_pattern_rows = get_skill_patterns(db_path, enabled_only=True)
        if skill_pattern_rows:
            skill_patterns = [
                (
                    str(item.get("name", "")).strip(),
                    str(item.get("pattern", "")).strip(),
                )
                for item in skill_pattern_rows
                if str(item.get("name", "")).strip()
                and str(item.get("pattern", "")).strip()
            ]
        else:
            skill_patterns = _profile_skill_patterns(profile)

        composed = f"{title or ''}\n{company or ''}\n{raw_text or ''}"
        score, reason, relevant, category = score_relevance(
            composed,
            profile,
            skill_patterns=skill_patterns,
            source=source or "",
            position_link=position_link or "",
            easy_apply_cache={},
        )

        if int(applied or 0) == 1:
            relevant = 1
            category = "relevant"

        cur.execute(
            "UPDATE jobs SET relevance_score=?, relevance_reason=?, relevant=?, category=?, updated_at=? WHERE id=?",
            (score, reason, int(relevant), category, datetime.now(timezone.utc).isoformat(), int(rid)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_relevant_jobs(db_path: str, limit: int = 0) -> list[dict]:
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


def get_jobs_by_category(
    db_path: str, category: str, limit: int = 0, unviewed_only: bool = False
) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description "
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
                "source": (r[1] or _provider_from_link(r[6] or ""))
                if len(r) > 1
                else "Unknown",
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
                "description": r[13] or "",
                "category": category,
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_applied_jobs(db_path: str, limit: int = 0) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, relevance_score, relevance_reason, summary, viewed, applied, description, category "
            "FROM jobs WHERE applied=1 ORDER BY updated_at DESC"
        )
        params: list = []
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[6] or ""))
                if len(r) > 1
                else "Unknown",
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
                "description": r[13] or "",
                "category": r[14] or "relevant",
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_viewed_jobs_count(db_path: str) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM jobs WHERE viewed=1")
        row = cur.fetchone()
        return int((row[0] if row else 0) or 0)
    finally:
        conn.close()


def get_jobs_for_description_refresh(
    db_path: str,
    category: str = "",
    source: str = "",
    links: list[str] = None,
    job_ids: list[int] = None,
    limit: int = 0,
    missing_only: bool = True,
    unviewed_only: bool = False,
) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT id, source, company, title, place, work_type, position_link, raw_text, category, description, summary "
            "FROM jobs WHERE 1=1"
        )
        params: list = []

        q += " AND (applied IS NULL OR applied=0)"

        if category:
            q += " AND category=?"
            params.append(category)

        if source:
            q += " AND LOWER(source)=LOWER(?)"
            params.append(source)

        if links:
            normalized_links = [
                _normalize_position_link(link) for link in links if (link or "").strip()
            ]
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

        if unviewed_only:
            q += " AND (viewed IS NULL OR viewed=0)"

        q += " ORDER BY updated_at DESC"
        if limit and limit > 0:
            q += " LIMIT ?"
            params.append(int(limit))

        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "source": (r[1] or _provider_from_link(r[6] or ""))
                if len(r) > 1
                else "Unknown",
                "company": r[2] or "",
                "title": r[3] or "",
                "place": r[4] or "",
                "work_type": r[5] or "Unknown",
                "position_link": r[6] or "",
                "raw_text": r[7] or "",
                "category": r[8] or "",
                "description": r[9] or "",
                "summary": r[10] or "",
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
            (summary, datetime.now(timezone.utc).isoformat(), job_id),
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
            (description, datetime.now(timezone.utc).isoformat(), job_id),
        )
        conn.commit()
    finally:
        conn.close()


def append_applied_job_raw_text(
    db_path: str,
    job_id: int,
    manual_text: str,
    marker: str = "[MANUAL_APPLIED_DESCRIPTION]",
    max_total_chars: int = 120000,
) -> bool:
    """Append manual description text to raw_text for an applied job only."""
    cleaned = (manual_text or "").strip()
    if not cleaned:
        return False

    block = f"{marker}\n{cleaned}".strip()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT raw_text, applied FROM jobs WHERE id=?", (int(job_id),))
        row = cur.fetchone()
        if not row:
            return False

        raw_text = (row[0] or "").strip()
        applied = int(row[1] or 0)
        if applied != 1:
            return False

        if block in raw_text:
            return True

        merged = f"{raw_text}\n\n{block}".strip() if raw_text else block
        merged = merged[-max_total_chars:]
        cur.execute(
            "UPDATE jobs SET raw_text=?, updated_at=? WHERE id=?",
            (merged, datetime.now(timezone.utc).isoformat(), int(job_id)),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_skills(db_path: str, job_id: int, skill_names: list[str]) -> None:
    """Persist the extracted skill list for a job as links to skill_patterns rows."""
    if not job_id or not skill_names:
        return
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for name in skill_names:
            name = (name or "").strip()
            if not name:
                continue
            key = _normalize_skill_key(name)
            # Ensure the skill_pattern row exists (source='detected', no weight bump here)
            cur.execute(
                """
                INSERT INTO skill_patterns (name, name_key, pattern, source, occurrences, weight, enabled, created_at, updated_at)
                VALUES (?, ?, ?, 'detected', 0, 0, 1, ?, ?)
                ON CONFLICT(name_key) DO NOTHING
                """,
                (name, key, _skill_to_regex_simple(name), now, now),
            )
            cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", (key,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "INSERT OR IGNORE INTO job_skills (job_id, skill_id) VALUES (?, ?)",
                    (job_id, row[0]),
                )
        conn.commit()
    finally:
        conn.close()


def get_job_skills(db_path: str, job_id: int) -> list[str]:
    """Return the cached skill names for a job, or empty list if not yet stored."""
    if not job_id:
        return []
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT sp.name FROM job_skills js
            JOIN skill_patterns sp ON sp.id = js.skill_id
            WHERE js.job_id = ?
            ORDER BY sp.name
            """,
            (job_id,),
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def clear_job_skills_for_unviewed_jobs(db_path: str) -> int:
    """Clear cached job->skill links for unviewed jobs so skills can be re-extracted."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            DELETE FROM job_skills
            WHERE job_id IN (
                SELECT id FROM jobs WHERE COALESCE(viewed, 0)=0
            )
            """
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def clear_job_skills_for_job(db_path: str, job_id: int) -> int:
    """Clear cached job->skill links for one job so skills can be re-extracted."""
    if not job_id:
        return 0
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM job_skills WHERE job_id=?", (int(job_id),))
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def delete_skill_from_db(db_path: str, skill_name: str) -> dict[str, int]:
    """Delete a skill from skill_patterns and all job links by normalized name key."""
    key = _normalize_skill_key(skill_name)
    if not key:
        return {"skill_rows_deleted": 0, "job_skill_links_deleted": 0}

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM skill_patterns WHERE name_key=?", (key,))
        skill_ids = [int(r[0]) for r in cur.fetchall()]
        if not skill_ids:
            return {"skill_rows_deleted": 0, "job_skill_links_deleted": 0}

        links_deleted = 0
        for skill_id in skill_ids:
            cur.execute("DELETE FROM job_skills WHERE skill_id=?", (skill_id,))
            links_deleted += int(cur.rowcount or 0)

        skill_rows_deleted = 0
        for skill_id in skill_ids:
            cur.execute("DELETE FROM skill_patterns WHERE id=?", (skill_id,))
            skill_rows_deleted += int(cur.rowcount or 0)

        conn.commit()
        return {
            "skill_rows_deleted": int(skill_rows_deleted),
            "job_skill_links_deleted": int(links_deleted),
        }
    finally:
        conn.close()


def _skill_to_regex_simple(name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(r"[A-Za-z0-9+#.]+", name or "") if t]
    if not tokens:
        return name
    return r"\b" + r"\s+".join(tokens) + r"\b"


def set_job_feedback(db_path: str, job_id: int, signal: str) -> bool:
    normalized = (signal or "").strip().lower()
    if normalized not in {"relevant", "not relevant"}:
        raise ValueError(f"Unsupported signal: {signal}")

    relevant = 1 if normalized == "relevant" else 0
    now = datetime.now(timezone.utc).isoformat()

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
            (
                relevant,
                normalized,
                f"manual_feedback={normalized}",
                applied,
                now,
                int(job_id),
            ),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def set_job_viewed(db_path: str, job_id: int, viewed: bool) -> bool:
    now = datetime.now(timezone.utc).isoformat()
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
    now = datetime.now(timezone.utc).isoformat()
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
    docs: list[dict],
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    positions_by_file: list[dict[str, object]] = []
    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        entries = extract_job_entries(doc)
        file_found = 0
        file_inserted = 0
        file_skipped = 0
        for entry in entries:
            if not entry.get("position_link"):
                continue
            file_found += 1
            is_new_record = upsert_job(db_path, entry)
            if is_new_record and on_new_record:
                on_new_record()
            if is_new_record:
                inserted_new += 1
                file_inserted += 1
            else:
                skipped_existing += 1
                file_skipped += 1
            processed += 1
            if on_progress:
                on_progress(processed, inserted_new, skipped_existing)
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(file_found),
                "inserted_new": int(file_inserted),
                "skipped_existing": int(file_skipped),
            }
        )
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }
