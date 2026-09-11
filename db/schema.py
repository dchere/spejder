"""CREATE/ALTER jobs schema, jobs_new migrations, skill tables, applied_at backfill."""

def apply_schema(cur) -> None:
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
                    hidden INTEGER DEFAULT 0,
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
                    hidden INTEGER DEFAULT 0,
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
            hidden INTEGER DEFAULT 0,
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
    if "cover_letter" not in cols:
        cur.execute("ALTER TABLE jobs ADD COLUMN cover_letter TEXT")
    if "cover_letter_requested" not in cols:
        cur.execute("ALTER TABLE jobs ADD COLUMN cover_letter_requested INTEGER DEFAULT 0")
    if "applied_at" not in cols:
        cur.execute("ALTER TABLE jobs ADD COLUMN applied_at TEXT")
    if "hidden" not in cols:
        cur.execute("ALTER TABLE jobs ADD COLUMN hidden INTEGER DEFAULT 0")

    cur.execute(
        "UPDATE jobs SET applied_at = updated_at WHERE applied = 1 AND applied_at IS NULL"
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
        CREATE TABLE IF NOT EXISTS bad_ngram_weights (
            ngram TEXT NOT NULL,
            gram_size INTEGER NOT NULL,
            weight INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (ngram, gram_size)
        )
        """
    )
