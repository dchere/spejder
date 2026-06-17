from .connection import _connect


def get_jobs_merge_candidates(db_path: str) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, position_link, source, title, company FROM jobs")
        return [{"id": r[0], "position_link": r[1], "source": r[2], "title": r[3], "company": r[4]} for r in cur.fetchall()]
    finally:
        conn.close()



def get_titles_for_labeled_jobs(db_path: str) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT title, description FROM jobs WHERE category IN ('relevant', 'not relevant') AND applied=0"
        )
        return [(r[0] or "") + " " + (r[1] or "") for r in cur.fetchall()]
    finally:
        conn.close()


def get_titles_for_missing_skills(db_path: str) -> list[str]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT title, description FROM jobs WHERE applied=1"
        )
        return [(r[0] or "") + " " + (r[1] or "") for r in cur.fetchall()]
    finally:
        conn.close()



def get_all_jobs_for_dedupe(db_path: str) -> list[tuple]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source, company, title, place, work_type, position_link, raw_text, viewed, applied, created_at
            FROM jobs
            """
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_jobs_for_keyword_suggestions(db_path: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT category, title, company, place, work_type, raw_text FROM jobs WHERE category IN ('relevant', 'not relevant')"
        )
        return cur.fetchall()
    finally:
        conn.close()


def get_jobs_for_skill_suggestions(db_path: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id FROM jobs WHERE applied=1"
        )
        return cur.fetchall()
    finally:
        conn.close()

