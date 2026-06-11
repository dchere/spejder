"""Tests for interview stage DB queries and mutations."""

import os
import tempfile
import unittest

from spejder.db import (
    ensure_db,
    get_all_applied_jobs,
    get_applied_jobs,
    get_interview_jobs,
    get_stopped_interview_jobs,
    set_job_applied,
    set_job_company_feedback,
    set_job_feedback,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.workflows.dashboard import build_dashboard_record
from spejder.config import AppConfig


def _insert_job(db_path: str, link: str, *, applied: bool = False, title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    if applied:
        set_job_applied(db_path, job_id, True)
    return job_id


class InterviewDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_get_applied_jobs_excludes_interview_stages(self):
        applied_id = _insert_job(self.db_path, "https://example.com/applied", applied=True, title="Applied Role")
        interview_id = _insert_job(self.db_path, "https://example.com/interview", applied=True, title="Interview Role")
        stopped_id = _insert_job(self.db_path, "https://example.com/stopped", applied=True, title="Stopped Role")
        self.assertTrue(set_job_on_interview(self.db_path, interview_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, stopped_id, True))

        applied_ids = {row["id"] for row in get_applied_jobs(self.db_path, limit=0)}
        interview_ids = {row["id"] for row in get_interview_jobs(self.db_path, limit=0)}
        stopped_ids = {row["id"] for row in get_stopped_interview_jobs(self.db_path, limit=0)}
        all_ids = {row["id"] for row in get_all_applied_jobs(self.db_path, limit=0)}

        self.assertEqual(applied_ids, {applied_id})
        self.assertEqual(interview_ids, {interview_id})
        self.assertEqual(stopped_ids, {stopped_id})
        self.assertEqual(all_ids, {applied_id, interview_id, stopped_id})

    def test_interview_mutations_require_applied_and_are_mutually_exclusive(self):
        job_id = _insert_job(self.db_path, "https://example.com/pending", applied=False)
        self.assertFalse(set_job_on_interview(self.db_path, job_id, True))
        self.assertFalse(set_job_interview_stopped(self.db_path, job_id, True))

        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        row = get_interview_jobs(self.db_path, limit=0)[0]
        self.assertEqual(row["on_interview"], 1)
        self.assertEqual(row["interview_stopped"], 0)

        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        row = get_stopped_interview_jobs(self.db_path, limit=0)[0]
        self.assertEqual(row["on_interview"], 0)
        self.assertEqual(row["interview_stopped"], 1)

        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        row = get_interview_jobs(self.db_path, limit=0)[0]
        self.assertEqual(row["on_interview"], 1)
        self.assertEqual(row["interview_stopped"], 0)

    def test_unapply_clears_interview_fields(self):
        job_id = _insert_job(self.db_path, "https://example.com/clear", applied=True)
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "No hire"))
        self.assertTrue(set_job_applied(self.db_path, job_id, False))

        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual(applied, 0)
        self.assertEqual(on_interview, 0)
        self.assertEqual(interview_stopped, 0)
        self.assertIsNone(company_feedback)

    def test_unstop_preserves_company_feedback(self):
        job_id = _insert_job(self.db_path, "https://example.com/unstop", applied=True)
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "No hire"))

        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, False))
        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual(interview_stopped, 0)
        self.assertEqual(company_feedback, "No hire")
        self.assertEqual(applied, 1)
        self.assertEqual(on_interview, 0)

    def test_build_dashboard_record_includes_interview_fields(self):
        record = build_dashboard_record(
            db_path=self.db_path,
            runtime_profile=AppConfig(),
            title_translation_cache={},
            row={
                "id": 1,
                "on_interview": 1,
                "interview_stopped": 0,
                "company_feedback": "Follow up later",
            },
            default_category="relevant",
            translate_title=False,
        )
        self.assertEqual(record["on_interview"], 1)
        self.assertEqual(record["interview_stopped"], 0)
        self.assertEqual(record["company_feedback"], "Follow up later")

    def test_company_feedback_requires_stopped(self):
        job_id = _insert_job(self.db_path, "https://example.com/feedback", applied=True)
        self.assertFalse(set_job_company_feedback(self.db_path, job_id, "Too early"))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "No hire"))
        row = get_stopped_interview_jobs(self.db_path, limit=0)[0]
        self.assertEqual(row["company_feedback"], "No hire")

    def _interview_row(self, job_id: int):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT applied, on_interview, interview_stopped, company_feedback FROM jobs WHERE id=?",
                (job_id,),
            )
            return cur.fetchone()
        finally:
            conn.close()

    def test_viewed_false_clears_interview_then_reapply_lands_in_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/viewed-clear", applied=True)
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "Ghosted"))

        self.assertTrue(set_job_viewed(self.db_path, job_id, False))
        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual((applied, on_interview, interview_stopped, company_feedback), (0, 0, 0, None))

        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        applied_ids = {row["id"] for row in get_applied_jobs(self.db_path, limit=0)}
        interview_ids = {row["id"] for row in get_interview_jobs(self.db_path, limit=0)}
        stopped_ids = {row["id"] for row in get_stopped_interview_jobs(self.db_path, limit=0)}
        self.assertEqual(applied_ids, {job_id})
        self.assertEqual(interview_ids, set())
        self.assertEqual(stopped_ids, set())

    def test_not_relevant_feedback_clears_interview_then_reapply_lands_in_applied(self):
        job_id = _insert_job(self.db_path, "https://example.com/feedback-clear", applied=True)
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        self.assertTrue(set_job_company_feedback(self.db_path, job_id, "Rejected"))

        self.assertTrue(set_job_feedback(self.db_path, job_id, "not relevant"))
        applied, on_interview, interview_stopped, company_feedback = self._interview_row(job_id)
        self.assertEqual((applied, on_interview, interview_stopped, company_feedback), (0, 0, 0, None))

        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        applied_ids = {row["id"] for row in get_applied_jobs(self.db_path, limit=0)}
        interview_ids = {row["id"] for row in get_interview_jobs(self.db_path, limit=0)}
        stopped_ids = {row["id"] for row in get_stopped_interview_jobs(self.db_path, limit=0)}
        self.assertEqual(applied_ids, {job_id})
        self.assertEqual(interview_ids, set())
        self.assertEqual(stopped_ids, set())


if __name__ == "__main__":
    unittest.main()
