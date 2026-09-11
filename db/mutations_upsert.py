"""Job upsert, relevance, delete, source, and batch merge writes."""
from datetime import datetime, timezone

from .connection import _connect
from .utils import sanitize_job_title, _provider_from_link
from .deduplication_utils import (
    _keeper_sort_key,
    _merge_duplicate_into_keeper,
    _position_dedupe_key,
)
from .mutations_pipeline import (
    _HIDDEN_CLEAR_IF_VIEWED_OR_APPLIED,
    _INTERVIEW_FIELDS_CLEAR,
)

def upsert_job(db_path: str, job: dict) -> bool:
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        position_link = job.get("position_link", "")
        source = job.get("source") or _provider_from_link(position_link)
        company = job.get("company", "")
        title = sanitize_job_title(job.get("title", ""))
        place = job.get("place", "")
        work_type = job.get("work_type", "Unknown")
        raw_text = job.get("raw_text", "")
        cur.execute(
            "SELECT 1 FROM jobs WHERE position_link=? LIMIT 1", (position_link,)
        )
        is_new_record = cur.fetchone() is None

        if is_new_record:
            dedupe_key = _position_dedupe_key(company, title, place)
            if dedupe_key:
                cur.execute(
                    """
                    SELECT id, source, company, title, title_english, place, work_type, raw_text, viewed, applied, position_link, created_at
                    FROM jobs
                    """
                )
                matches = []
                for row in cur.fetchall():
                    existing_key = _position_dedupe_key(
                        str(row[2] or ""), str(row[3] or ""), str(row[5] or "")
                    )
                    if existing_key != dedupe_key:
                        continue
                    matches.append({
                        "id": int(row[0] or 0),
                        "source": str(row[1] or "").strip() or _provider_from_link(str(row[10] or "")),
                        "company": str(row[2] or ""),
                        "title": str(row[3] or ""),
                        "place": str(row[5] or ""),
                        "work_type": str(row[6] or ""),
                        "raw_text": str(row[7] or ""),
                        "viewed": int(row[8] or 0),
                        "applied": int(row[9] or 0),
                        "position_link": str(row[10] or ""),
                        "created_at": str(row[11] or ""),
                    })

                if matches:
                    matches.sort(key=_keeper_sort_key)
                    keeper = dict(matches[0])
                    incoming = {
                        "company": company,
                        "title": title,
                        "place": place,
                        "work_type": work_type,
                        "raw_text": raw_text,
                        "viewed": 0,
                        "applied": 0,
                    }
                    _merge_duplicate_into_keeper(keeper, incoming)

                    cur.execute(
                        f"""
                        UPDATE jobs
                        SET company=?, title=?, title_english=?, place=?, work_type=?, raw_text=?,
                            viewed=?, applied=?, {_HIDDEN_CLEAR_IF_VIEWED_OR_APPLIED}, updated_at=?
                        WHERE id=?
                        """,
                        (
                            keeper["company"],
                            sanitize_job_title(keeper["title"]),
                            "",
                            keeper["place"],
                            keeper["work_type"] or "Unknown",
                            keeper["raw_text"],
                            keeper["viewed"],
                            keeper["applied"],
                            keeper["viewed"],
                            keeper["applied"],
                            now,
                            keeper["id"],
                        ),
                    )
                    conn.commit()
                    return False

        if is_new_record:
            cur.execute(
                """
                            INSERT INTO jobs (source, company, title, place, work_type, position_link, raw_text, created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source,
                    company,
                    title,
                    place,
                    work_type,
                    position_link,
                    raw_text,
                    now,
                    now,
                ),
            )
            conn.commit()
            return is_new_record

        cur.execute(
            """
            SELECT id, company, title, place, work_type, raw_text
            FROM jobs
            WHERE position_link=?
            LIMIT 1
            """,
            (position_link,),
        )
        existing = cur.fetchone()
        if existing:
            job_id = int(existing[0] or 0)
            merged_company = str(existing[1] or "") or company
            merged_title = sanitize_job_title(str(existing[2] or "") or title)
            merged_place = str(existing[3] or "")
            merged_work_type = str(existing[4] or "") or work_type
            merged_raw = str(existing[5] or "")

            provider_company = _provider_from_link(position_link)
            if company and (
                not merged_company
                or (provider_company and provider_company == company and merged_company != company)
            ):
                merged_company = company
            if title and (not merged_title or len(title) > len(merged_title)):
                merged_title = sanitize_job_title(title)
            if place and (not merged_place or merged_place.lower() == "unknown"):
                merged_place = place
            if work_type and (
                not merged_work_type or merged_work_type.lower() == "unknown"
            ):
                merged_work_type = work_type
            if len(raw_text) > len(merged_raw):
                merged_raw = raw_text

            if (
                merged_company != str(existing[1] or "")
                or merged_title != sanitize_job_title(str(existing[2] or ""))
                or merged_place != str(existing[3] or "")
                or merged_work_type != str(existing[4] or "")
                or merged_raw != str(existing[5] or "")
            ):
                cur.execute(
                    """
                    UPDATE jobs
                    SET source=?, company=?, title=?, place=?, work_type=?, raw_text=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        source,
                        merged_company,
                        merged_title,
                        merged_place,
                        merged_work_type or "Unknown",
                        merged_raw,
                        now,
                        job_id,
                    ),
                )
                conn.commit()
        return False
    finally:
        conn.close()



def update_jobs_relevance(db_path: str, updates: list[tuple[int, float, str, int, str]], prune_irrelevant: bool = False):
    from datetime import datetime, timezone
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        for rid, score, reason, relevant, category in updates:
            cur.execute(
                "UPDATE jobs SET relevance_score=?, relevance_reason=?, relevant=?, category=?, updated_at=? WHERE id=?",
                (score, reason, relevant, category, now, rid),
            )

        if prune_irrelevant:
            cur.execute("DELETE FROM jobs WHERE category='not relevant'")

        conn.commit()
    finally:
        conn.close()


def delete_jobs(db_path: str, rids: list[int]):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for rid in rids:
            cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
            cur.execute("DELETE FROM job_skills WHERE job_id=?", (rid,))
        conn.commit()
    finally:
        conn.close()


def update_job_source(db_path: str, job_id: int, source: str):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET source=? WHERE id=?", (source, job_id))
        conn.commit()
    finally:
        conn.close()



def batch_update_and_delete_jobs(db_path: str, updates: list[tuple], deletes: list[int]):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for u in updates:
            # (company, title, place, work_type, raw_text, viewed, applied, hidden, updated_at, id)
            company, title, place, work_type, raw_text, viewed, applied, hidden, updated_at, job_id = u
            viewed = int(viewed or 0)
            applied = int(applied or 0)
            hidden = 0 if viewed == 1 or applied == 1 else int(hidden or 0)
            params = (
                company,
                title,
                place,
                work_type,
                raw_text,
                viewed,
                applied,
                hidden,
                updated_at,
                job_id,
            )
            if applied == 0:
                cur.execute(
                    "UPDATE jobs SET company=?, title=?, place=?, work_type=?, raw_text=?, "
                    f"viewed=?, applied=?, hidden=?, {_INTERVIEW_FIELDS_CLEAR}, updated_at=? WHERE id=?",
                    params,
                )
            else:
                cur.execute(
                    "UPDATE jobs SET company=?, title=?, place=?, work_type=?, raw_text=?, "
                    "viewed=?, applied=?, hidden=?, updated_at=? WHERE id=?",
                    params,
                )
        for rid in deletes:
            cur.execute("DELETE FROM jobs WHERE id=?", (rid,))
        conn.commit()
    finally:
        conn.close()
