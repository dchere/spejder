import os
import sqlite3
import re
import time
import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qs, unquote, urlparse
from spejder.db.utils import SQLITE_TIMEOUT_SECONDS, SQLITE_BUSY_TIMEOUT_MS, sanitize_job_title, _normalize_position_link, _provider_from_link

JOB_RETENTION_DAYS = 90

def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_job_link(db_path: str, job_id: int):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT position_link FROM jobs WHERE id=?", (job_id,))
        return cur.fetchone()
    finally:
        conn.close()


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
                        title_english TEXT,
                        place TEXT,
                        work_type TEXT,
                        position_link TEXT UNIQUE NOT NULL,
                        raw_text TEXT,
                        description TEXT,
                        viewed INTEGER DEFAULT 0,
                        applied INTEGER DEFAULT 0,
                        on_interview INTEGER DEFAULT 0,
                        interview_stopped INTEGER DEFAULT 0,
                        company_feedback TEXT,
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
                        (source, company, title, title_english, place, work_type, position_link, raw_text, description, viewed, applied, relevance_score, relevant, category, relevance_reason, summary, created_at, updated_at)
                    SELECT
                        '',
                        company,
                        title,
                        '',
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
            if "on_interview" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN on_interview INTEGER DEFAULT 0")
            if "interview_stopped" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN interview_stopped INTEGER DEFAULT 0")
            if "company_feedback" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN company_feedback TEXT")
            if "source" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN source TEXT")
            if "description" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN description TEXT")
            if "title_english" not in cols:
                cur.execute("ALTER TABLE jobs ADD COLUMN title_english TEXT")

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
                        title_english TEXT,
                        place TEXT,
                        work_type TEXT,
                        position_link TEXT UNIQUE NOT NULL,
                        raw_text TEXT,
                        description TEXT,
                        viewed INTEGER DEFAULT 0,
                        applied INTEGER DEFAULT 0,
                        on_interview INTEGER DEFAULT 0,
                        interview_stopped INTEGER DEFAULT 0,
                        company_feedback TEXT,
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
                on_interview_sel = "on_interview" if "on_interview" in cols else "0"
                interview_stopped_sel = "interview_stopped" if "interview_stopped" in cols else "0"
                company_feedback_sel = "company_feedback" if "company_feedback" in cols else "NULL"
                cur.execute(
                    f"""
                    INSERT OR IGNORE INTO jobs_new
                        (id, source, company, title, title_english, place, work_type, position_link, raw_text, description, viewed, applied, on_interview, interview_stopped, company_feedback, relevance_score, relevant, category, relevance_reason, summary, created_at, updated_at)
                    SELECT
                        id,
                        source,
                        company,
                        title,
                        COALESCE(title_english, ''),
                        place,
                        work_type,
                        position_link,
                        raw_text,
                        description,
                        viewed,
                        applied,
                        {on_interview_sel},
                        {interview_stopped_sel},
                        {company_feedback_sel},
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
                title_english TEXT,
                place TEXT,
                work_type TEXT,
                position_link TEXT UNIQUE NOT NULL,
                raw_text TEXT,
                description TEXT,
                viewed INTEGER DEFAULT 0,
                applied INTEGER DEFAULT 0,
                on_interview INTEGER DEFAULT 0,
                interview_stopped INTEGER DEFAULT 0,
                company_feedback TEXT,
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

        cur.execute("PRAGMA table_info(jobs)")
        cols = {row[1] for row in cur.fetchall()}
        if "on_interview" not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN on_interview INTEGER DEFAULT 0")
        if "interview_stopped" not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN interview_stopped INTEGER DEFAULT 0")
        if "company_feedback" not in cols:
            cur.execute("ALTER TABLE jobs ADD COLUMN company_feedback TEXT")

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
        cur.execute("UPDATE jobs SET on_interview=0 WHERE on_interview IS NULL")
        cur.execute("UPDATE jobs SET interview_stopped=0 WHERE interview_stopped IS NULL")
        cur.execute("UPDATE jobs SET source='' WHERE source IS NULL")
        cur.execute("UPDATE jobs SET description='' WHERE description IS NULL")

        cur.execute("SELECT id, title, title_english FROM jobs")
        for rid, title, title_english in cur.fetchall():
            cleaned_title = sanitize_job_title(title or "")
            if cleaned_title and cleaned_title != (title or ""):
                cur.execute(
                    "UPDATE jobs SET title=?, title_english=?, updated_at=? WHERE id=?",
                    (cleaned_title, "" if (title_english or "") else title_english, datetime.now(timezone.utc).isoformat(), rid),
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

        cur.execute(
            """
            UPDATE jobs SET source = 'Emerson Career Site'
            WHERE lower(position_link) LIKE '%hdjq.fa.us2.oraclecloud.com%'
              AND lower(position_link) LIKE '%/candidateexperience/%'
              AND (source IS NULL OR trim(source) = '' OR source = 'Oracle CX')
            """
        )
        cur.execute(
            """
            UPDATE jobs SET company = 'Emerson'
            WHERE lower(position_link) LIKE '%hdjq.fa.us2.oraclecloud.com%'
              AND lower(position_link) LIKE '%/candidateexperience/%'
              AND (company IS NULL OR trim(company) = '' OR company = 'Emerson Career Site')
            """
        )

        conn.commit()
    finally:
        conn.close()


