"""Tests for user portrait workflow."""

import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import (
    ensure_db,
    get_job_skills_for_jobs,
    replace_job_skills,
    set_job_applied,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_summary,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.workflows.user_portrait import (
    build_portrait_prompt,
    collect_portrait_context,
    embed_portrait_for_textarea,
    generate_portrait_draft,
    load_portrait,
    portrait_has_context,
    render_portrait_diff_html,
    save_portrait,
)


def _insert_applied_job(db_path: str, link: str, company: str = "Acme") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": company,
            "title": "Engineer",
            "position_link": link,
            "raw_text": "Python backend role",
            "summary": "Build APIs",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        job_id = int(cur.fetchone()[0])
    finally:
        conn.close()
    set_job_applied(db_path, job_id, True)
    return job_id


class UserPortraitWorkflowTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.portrait_path = os.path.join(self._tmpdir.name, "portrait.txt")
        self.cv_path = os.path.join(self._tmpdir.name, "CV.txt")
        ensure_db(self.db_path)
        with open(self.cv_path, "w", encoding="utf-8") as handle:
            handle.write("Senior Python engineer with API experience.")
        self.profile = AppConfig(
            user_skills=["python", "apis"],
            missing_skills_suggestions=["docker"],
            default_portrait_path=self.portrait_path,
            default_cv_path=self.cv_path,
            max_input_chars=8000,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_load_save_roundtrip(self):
        self.assertEqual(load_portrait(self.portrait_path), "")
        save_portrait(self.portrait_path, "Hello portrait")
        self.assertEqual(load_portrait(self.portrait_path), "Hello portrait")

    def test_render_portrait_diff_html_marks_changes(self):
        html = render_portrait_diff_html("line one\nold line", "line one\nnew line")
        self.assertIn("diff-line-del", html)
        self.assertIn("diff-line-add", html)
        self.assertIn("old line", html)
        self.assertIn("new line", html)

    def test_collect_portrait_context_includes_skills_jobs_cv(self):
        _insert_applied_job(self.db_path, "https://example.com/job-1")
        context = collect_portrait_context(self.db_path, self.profile, cv_path=self.cv_path)
        self.assertIn("SKILLS I HAVE", context)
        self.assertIn("python", context)
        self.assertIn("SKILLS TO LEARN", context)
        self.assertIn("docker", context)
        self.assertIn("CV TEXT", context)
        self.assertIn("APPLIED JOBS", context)
        self.assertIn("Acme", context)

    def test_build_portrait_prompt_requests_minimal_change(self):
        prompt = build_portrait_prompt("Existing summary", "NEW DATA block")
        self.assertIn("Existing summary", prompt)
        self.assertIn("minimal edits", prompt.lower())
        self.assertIn("NEW DATA block", prompt)

    def test_portrait_has_context(self):
        self.assertTrue(portrait_has_context(self.db_path, self.profile, cv_path=self.cv_path))
        empty_profile = AppConfig(default_cv_path=self.cv_path)
        os.remove(self.cv_path)
        self.assertFalse(portrait_has_context(self.db_path, empty_profile, cv_path=self.cv_path))

    def test_collect_portrait_context_truncates_long_fields(self):
        long_text = "x" * 5000
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "BigCo",
                "title": "Lead",
                "position_link": "https://example.com/long",
                "raw_text": "short raw",
                "summary": long_text,
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM jobs WHERE position_link=?", ("https://example.com/long",))
            job_id = int(cur.fetchone()[0])
        finally:
            conn.close()
        set_job_applied(self.db_path, job_id, True)
        set_job_summary(self.db_path, job_id, long_text)
        context = collect_portrait_context(self.db_path, self.profile, cv_path=self.cv_path)
        self.assertLess(len(context), 9000)
        self.assertIn("[summary truncated]", context)

    def test_collect_portrait_context_uses_batch_job_skills(self):
        job_id = _insert_applied_job(self.db_path, "https://example.com/skilled")
        replace_job_skills(self.db_path, job_id, ["python", "docker"])
        context = collect_portrait_context(self.db_path, self.profile, cv_path=self.cv_path)
        self.assertIn("skills: docker, python", context)

    def test_get_job_skills_for_jobs_batch(self):
        job_a = _insert_applied_job(self.db_path, "https://example.com/a")
        job_b = _insert_applied_job(self.db_path, "https://example.com/b", company="Beta")
        replace_job_skills(self.db_path, job_a, ["python"])
        replace_job_skills(self.db_path, job_b, ["rust", "sql"])
        skills = get_job_skills_for_jobs(self.db_path, [job_a, job_b, 999])
        self.assertEqual(skills[job_a], ["python"])
        self.assertEqual(skills[job_b], ["rust", "sql"])
        self.assertEqual(skills[999], [])

    def test_embed_portrait_for_textarea_preserves_plain_text(self):
        raw = "A & B <tag>\nLine two"
        self.assertEqual(embed_portrait_for_textarea(raw), raw)

    def test_embed_portrait_for_textarea_neutralizes_breakout(self):
        raw = "Before </textarea> after"
        embedded = embed_portrait_for_textarea(raw)
        self.assertIn("&lt;/textarea", embedded)
        self.assertNotIn("</textarea>", embedded.lower())

    def test_portrait_has_context_rejects_whitespace_only_skills(self):
        profile = AppConfig(
            user_skills=["  ", ""],
            missing_skills_suggestions=["\t"],
            default_cv_path=self.cv_path,
        )
        os.remove(self.cv_path)
        self.assertFalse(portrait_has_context(self.db_path, profile, cv_path=self.cv_path))

    def test_collect_portrait_context_includes_interview_stages(self):
        interview_id = _insert_applied_job(self.db_path, "https://example.com/interview")
        stopped_id = _insert_applied_job(self.db_path, "https://example.com/stopped", company="StopCo")
        set_job_on_interview(self.db_path, interview_id, True)
        set_job_interview_stopped(self.db_path, stopped_id, True)
        context = collect_portrait_context(self.db_path, self.profile, cv_path=self.cv_path)
        self.assertIn("(stage: interview)", context)
        self.assertIn("(stage: stopped)", context)

    def test_generate_portrait_draft_raises_without_context(self):
        profile = AppConfig(default_cv_path=os.path.join(self._tmpdir.name, "missing.txt"))
        with self.assertRaises(ValueError):
            generate_portrait_draft(None, self.db_path, profile)  # type: ignore[arg-type]
