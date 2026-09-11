"""Title sanitize, link canonicalize/dedupe, source backfill, Emerson migrate, prune."""
from datetime import datetime, timezone

from spejder.db.utils import sanitize_job_title, _normalize_position_link, _provider_from_link
from spejder.parsers.itday_portal import ITDAY_PORTAL_SOURCE

JOB_RETENTION_DAYS = 90


def apply_maintenance(cur) -> None:
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
            OR lower(trim(source)) = ?
        )
        """,
        (ITDAY_PORTAL_SOURCE.strip().lower(),),
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
    # Interview/stopped applied jobs are retained; plain applied rows still age out.
    cur.execute(
        """
        DELETE FROM jobs
        WHERE created_at IS NOT NULL
          AND TRIM(created_at) <> ''
          AND datetime(replace(created_at, 'T', ' ')) < datetime('now', ?)
          AND NOT (applied = 1 AND (on_interview = 1 OR interview_stopped = 1))
        """,
        (f"-{int(JOB_RETENTION_DAYS)} days",),
    )

    cur.execute(
        "UPDATE jobs SET work_type='Unknown' WHERE work_type IS NULL OR work_type='' "
    )
    cur.execute("UPDATE jobs SET viewed=0 WHERE viewed IS NULL")
    cur.execute("UPDATE jobs SET applied=0 WHERE applied IS NULL")
    cur.execute("UPDATE jobs SET hidden=0 WHERE hidden IS NULL")
    cur.execute("UPDATE jobs SET on_interview=0 WHERE on_interview IS NULL")
    cur.execute("UPDATE jobs SET interview_stopped=0 WHERE interview_stopped IS NULL")
    cur.execute("UPDATE jobs SET source='' WHERE source IS NULL")
    cur.execute("UPDATE jobs SET description='' WHERE description IS NULL")
    cur.execute("UPDATE jobs SET cover_letter_requested=0 WHERE cover_letter_requested IS NULL")

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

    portal_source_key = ITDAY_PORTAL_SOURCE.strip().lower()
    cur.execute("SELECT id, position_link, source FROM jobs")
    for rid, link, source in cur.fetchall():
        provider = _provider_from_link(link or "")
        if (
            provider
            and (source or "").strip().lower() != portal_source_key
            and (not source or source.strip() != provider)
        ):
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
