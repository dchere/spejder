"""Company promote flag bits, automation, merge, scoring, and API."""

import os
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    COMPANY_FLAG_PROMOTE,
    COMPANY_FLAG_PROMOTE_TOUCHED,
    company_has_flag,
    ensure_companies_from_jobs,
    ensure_company_for_raw,
    ensure_db,
    get_company_flags,
    get_promoted_company_keys,
    list_company_catalog_rows,
    merge_company_rows,
    set_company_promote,
    set_job_applied,
    set_job_feedback,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
    upsert_job,
)
from spejder.db.companies_flag_bits import (
    companies_flags_column_missing,
    merge_promote_flags,
)
from spejder.db.connection import _connect
from spejder.db.deduplication_utils import (
    _canonicalize_company_for_dedupe,
    _normalize_company_key,
)
from spejder.jobs.scoring import apply_relevance
from spejder.managers.dashboard_companies_table import _render_companies_table_html
from spejder.server import create_app
from spejder.tests.test_dashboard_templates import _extract_js_function_body, _read_template


def _company_key(company: str) -> str:
    return _normalize_company_key(_canonicalize_company_for_dedupe(company))


def _insert_job(
    db_path: str,
    link: str,
    *,
    company: str = "Acme",
    title: str = "Engineer",
) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": company,
            "title": title,
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _company_id(db_path: str, name: str) -> int:
    ensure_companies_from_jobs(db_path)
    for row in list_company_catalog_rows(db_path):
        if row["display_name"] == name:
            return int(row["id"])
    raise AssertionError(name)


class CompanyPromoteEnsureDbImportTest(unittest.TestCase):
    def test_ensure_db_without_prior_flag_imports(self):
        """Regression: bare ensure_db must not circular-import COMPANY_FLAG_*."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            script = textwrap.dedent(
                f"""
                import sys
                # Prove we did not pre-import flag constants / companies.
                banned = (
                    "spejder.db.companies",
                    "spejder.db.companies_flags",
                    "spejder.db.companies_flag_bits",
                )
                for name in banned:
                    assert name not in sys.modules, name
                from spejder.db import ensure_db
                ensure_db({db_path!r})
                from spejder.db import (
                    get_jobs_for_description_triage,
                    get_relevant_jobs,
                )
                assert callable(get_jobs_for_description_triage)
                assert callable(get_relevant_jobs)
                print("ok")
                """
            )
            result = subprocess.run(
                [sys.executable, "-c", script],
                cwd=os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                result.returncode,
                0,
                msg=f"stdout={result.stdout!r} stderr={result.stderr!r}",
            )
            self.assertIn("ok", result.stdout)


class CompanyPromoteFlagBitsTest(unittest.TestCase):
    def test_flag_helpers(self):
        flags = 0
        flags = flags | COMPANY_FLAG_PROMOTE
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))
        flags = flags | COMPANY_FLAG_PROMOTE_TOUCHED
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))
        self.assertEqual(COMPANY_FLAG_PROMOTE, 1)
        self.assertEqual(COMPANY_FLAG_PROMOTE_TOUCHED, 2)

    def test_merge_promote_or_and_disagree_touched(self):
        self.assertEqual(
            merge_promote_flags([COMPANY_FLAG_PROMOTE, 0]),
            COMPANY_FLAG_PROMOTE | COMPANY_FLAG_PROMOTE_TOUCHED,
        )
        self.assertEqual(
            merge_promote_flags([COMPANY_FLAG_PROMOTE, COMPANY_FLAG_PROMOTE]),
            COMPANY_FLAG_PROMOTE,
        )
        self.assertEqual(
            merge_promote_flags(
                [COMPANY_FLAG_PROMOTE_TOUCHED, COMPANY_FLAG_PROMOTE]
            ),
            COMPANY_FLAG_PROMOTE | COMPANY_FLAG_PROMOTE_TOUCHED,
        )


class CompanyPromoteAutomationTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_apply_sets_promote_untouched(self):
        job_id = _insert_job(self.db_path, "https://example.com/a", company="Foo")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "Foo")
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))
        self.assertEqual(get_promoted_company_keys(self.db_path), {_company_key("Foo")})

    def test_apply_blank_company_no_promote(self):
        job_id = _insert_job(self.db_path, "https://example.com/blank", company="  ")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertEqual(get_promoted_company_keys(self.db_path), set())

    def test_touched_blocks_auto_promote(self):
        job_id = _insert_job(self.db_path, "https://example.com/t", company="Bar")
        ensure_companies_from_jobs(self.db_path)
        cid = _company_id(self.db_path, "Bar")
        set_company_promote(self.db_path, cid, False, touch=True)
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

    def test_interview_true_sets_promote(self):
        job_id = _insert_job(self.db_path, "https://example.com/i", company="Baz")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "Baz")
        set_company_promote(self.db_path, cid, False, touch=False)
        # Clear promote without touch via raw update
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("UPDATE companies SET flags=0 WHERE id=?", (cid,))
            conn.commit()
        finally:
            conn.close()
        self.assertTrue(set_job_on_interview(self.db_path, job_id, True))
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))

    def test_unapply_clears_promote_when_idle_untouched(self):
        job_id = _insert_job(self.db_path, "https://example.com/u", company="Qux")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "Qux")
        self.assertTrue(company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE))
        self.assertTrue(set_job_applied(self.db_path, job_id, False))
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))

    def test_unapply_leaves_touched_promote(self):
        job_id = _insert_job(self.db_path, "https://example.com/ut", company="Kept")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "Kept")
        set_company_promote(self.db_path, cid, True, touch=True)
        self.assertTrue(set_job_applied(self.db_path, job_id, False))
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

    def test_unapply_one_of_two_leaves_promote(self):
        job_a = _insert_job(
            self.db_path, "https://example.com/mja", company="Multi", title="Role A"
        )
        job_b = _insert_job(
            self.db_path, "https://example.com/mjb", company="Multi", title="Role B"
        )
        self.assertTrue(set_job_applied(self.db_path, job_a, True))
        self.assertTrue(set_job_applied(self.db_path, job_b, True))
        cid = _company_id(self.db_path, "Multi")
        self.assertTrue(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE)
        )
        self.assertTrue(set_job_applied(self.db_path, job_a, False))
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(set_job_applied(self.db_path, job_b, False))
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))

    def test_viewed_false_clears_when_idle_leaves_when_not(self):
        job_a = _insert_job(
            self.db_path, "https://example.com/va", company="ViewIdle", title="Role A"
        )
        job_b = _insert_job(
            self.db_path, "https://example.com/vb", company="ViewIdle", title="Role B"
        )
        self.assertTrue(set_job_applied(self.db_path, job_a, True))
        self.assertTrue(set_job_applied(self.db_path, job_b, True))
        cid = _company_id(self.db_path, "ViewIdle")
        self.assertTrue(set_job_viewed(self.db_path, job_a, False))
        self.assertTrue(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE)
        )
        self.assertTrue(set_job_viewed(self.db_path, job_b, False))
        self.assertFalse(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE)
        )

    def test_not_relevant_clears_when_idle_leaves_when_not(self):
        job_a = _insert_job(
            self.db_path, "https://example.com/nra", company="NrIdle", title="Role A"
        )
        job_b = _insert_job(
            self.db_path, "https://example.com/nrb", company="NrIdle", title="Role B"
        )
        self.assertTrue(set_job_applied(self.db_path, job_a, True))
        self.assertTrue(set_job_applied(self.db_path, job_b, True))
        cid = _company_id(self.db_path, "NrIdle")
        self.assertTrue(set_job_feedback(self.db_path, job_a, "not relevant"))
        self.assertTrue(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE)
        )
        self.assertTrue(set_job_feedback(self.db_path, job_b, "not relevant"))
        self.assertFalse(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_PROMOTE)
        )

    def test_stop_does_not_auto_clear_promote(self):
        job_id = _insert_job(self.db_path, "https://example.com/s", company="StopCo")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "StopCo")
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

    def test_ensure_company_for_raw_creates(self):
        self.assertIsNone(ensure_company_for_raw(self.db_path, "  "))
        cid = ensure_company_for_raw(self.db_path, "Ephemeral New")
        self.assertIsNotNone(cid)
        self.assertEqual(ensure_company_for_raw(self.db_path, "Ephemeral New"), cid)

    def test_backfill_one_shot_does_not_rerun(self):
        job_id = _insert_job(
            self.db_path, "https://example.com/bf", company="BackfillCo"
        )
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "BackfillCo")
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT id, display_name, created_at, updated_at FROM companies"
            )
            company_rows = cur.fetchall()
            cur.execute(
                "SELECT company_id, alias_raw, alias_key, created_at "
                "FROM company_aliases"
            )
            alias_rows = cur.fetchall()
            cur.execute("DROP TABLE company_aliases")
            cur.execute("DROP TABLE companies")
            cur.execute(
                """
                CREATE TABLE companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    display_name TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE company_aliases (
                    company_id INTEGER NOT NULL,
                    alias_raw TEXT NOT NULL,
                    alias_key TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.executemany(
                "INSERT INTO companies (id, display_name, created_at, updated_at) "
                "VALUES (?, ?, ?, ?)",
                company_rows,
            )
            cur.executemany(
                "INSERT INTO company_aliases "
                "(company_id, alias_raw, alias_key, created_at) "
                "VALUES (?, ?, ?, ?)",
                alias_rows,
            )
            self.assertTrue(companies_flags_column_missing(cur))
            conn.commit()
        finally:
            conn.close()

        ensure_db(self.db_path)
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

        set_company_promote(self.db_path, cid, False, touch=True)
        ensure_db(self.db_path)
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))


class CompanyPromoteMergeTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_merge_or_disagree_sets_touched(self):
        _insert_job(self.db_path, "https://example.com/m1", company="Alpha")
        _insert_job(self.db_path, "https://example.com/m2", company="Beta")
        id_a = _company_id(self.db_path, "Alpha")
        id_b = _company_id(self.db_path, "Beta")
        set_company_promote(self.db_path, id_a, True, touch=False)
        # Beta stays 0
        result = merge_company_rows(self.db_path, [id_a, id_b], "Merged")
        survivor = int(result["survivor_id"])
        flags = get_company_flags(self.db_path, survivor)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))


class CompanyPromoteScoringTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def _profile(self) -> AppConfig:
        profile = MagicMock(spec=AppConfig)
        profile.include_keywords = []
        profile.exclude_keywords = []
        profile.learned_include_keywords = []
        profile.learned_exclude_keywords = []
        profile.min_score = 0.0
        profile.user_skills = []
        profile.unwanted_skills = []
        profile.blocked_skills = []
        profile.known_skill_patterns = []
        profile.skill_match_weight = 0.0
        profile.skill_missing_penalty = 0.0
        profile.skill_unwanted_penalty = 0.0
        profile.easy_apply_bonus = 0.0
        profile.applied_company_bonus = 0.75
        return profile

    def test_scoring_uses_promoted_keys(self):
        applied_id = _insert_job(
            self.db_path,
            "https://example.com/applied-foo-as",
            company="Foo A/S",
            title="Applied Role",
        )
        sibling_id = _insert_job(
            self.db_path,
            "https://example.com/sibling-foo",
            company="Foo",
            title="Sibling Role",
        )
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))
        scored, _ = apply_relevance(self.db_path, self._profile())
        self.assertGreaterEqual(scored, 1)
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT relevance_score, relevance_reason FROM jobs WHERE id=?",
                (sibling_id,),
            )
            score, reason = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(float(score or 0), 0.75)
        self.assertIn("applied_company=True", str(reason or ""))

    def test_cleared_promote_yields_no_bonus_with_active_applied(self):
        """Touched clear → no bonus even when applied∧¬stopped jobs remain."""
        applied_id = _insert_job(
            self.db_path,
            "https://example.com/applied-bar-as",
            company="Bar A/S",
            title="Applied Role",
        )
        sibling_id = _insert_job(
            self.db_path,
            "https://example.com/sibling-bar",
            company="Bar",
            title="Sibling Role",
        )
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))
        cid = ensure_company_for_raw(self.db_path, "Bar A/S")
        self.assertIsNotNone(cid)
        set_company_promote(self.db_path, int(cid), False, touch=True)
        scored, _ = apply_relevance(self.db_path, self._profile())
        self.assertGreaterEqual(scored, 1)
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT relevance_score, relevance_reason FROM jobs WHERE id=?",
                (sibling_id,),
            )
            score, reason = cur.fetchone()
        finally:
            conn.close()
        self.assertEqual(float(score or 0), 0.0)
        self.assertNotIn("applied_company=True", str(reason or ""))


class CompanyPromoteUiTest(unittest.TestCase):
    def test_companies_table_promote_checkbox(self):
        table = _render_companies_table_html(
            [
                {
                    "id": 1,
                    "display_name": "Acme",
                    "created_at": "2024-01-01T00:00:00+00:00",
                    "promote": True,
                    "job_count": 1,
                    "not_viewed_count": 0,
                    "relevant_count": 0,
                    "not_relevant_count": 0,
                    "applied_count": 0,
                    "interview_count": 0,
                    "stopped_count": 0,
                    "alias_count": 1,
                }
            ]
        )
        self.assertIn(">Promote<", table)
        self.assertIn("company-promote-checkbox", table)
        self.assertIn("setCompanyPromote(1, this.checked, this)", table)
        self.assertIn('data-sort-promote="1"', table)
        self.assertIn("checked", table)
        header = table[table.index("<thead>") : table.index("</thead>")]
        self.assertLess(header.index(">Promote<"), header.index(">Action<"))
        self.assertLess(header.index(">Action<"), header.index(">Company<"))
        self.assertLess(header.index(">Company<"), header.index(">Added<"))
        first_row = re.search(r"<tbody>\s*<tr\b[^>]*>(.*?)</tr>", table, re.DOTALL)
        self.assertIsNotNone(first_row)
        row_html = first_row.group(1)
        td_opens = re.findall(r"<td\b[^>]*>", row_html)
        tds = re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.DOTALL)
        self.assertGreaterEqual(len(tds), 4)
        self.assertIn('class="company-row-select"', tds[0])
        self.assertIn('class="company-promote-checkbox"', tds[1])
        self.assertIn('class="companies-action"', td_opens[2])
        self.assertIn("company-name-link", tds[3])

    def test_stop_dialog_only_when_promote_on(self):
        body = _extract_js_function_body(
            _read_template("partials/dashboard_card_actions.js"),
            "setInterviewStopped",
        )
        self.assertIn("/api/company/promote-status", body)
        self.assertIn("Mark this job as stopped?", body)
        self.assertIn("Keep promoting this company", body)
        self.assertIn("/api/company/promote", body)
        self.assertIn("promoteOn && companyId", body)
        self.assertIn("promoteChoice === false", body)
        self.assertIn("Promote clear failed", body)
        stop_idx = body.index("/api/interview/stopped")
        promote_post_idx = body.index("'/api/company/promote'")
        self.assertLess(stop_idx, promote_post_idx)

    def test_set_company_promote_waits_for_rebuild(self):
        body = _extract_js_function_body(
            _read_template("partials/dashboard_companies.js"),
            "setCompanyPromote",
        )
        self.assertIn("data.changed", body)
        self.assertIn("waitForRebuildThenReload('companies')", body)


class CompanyPromoteApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.rebuild = MagicMock()
        profile_path = os.path.join(self._tmpdir.name, "profile.json")
        runtime_profile = AppConfig()
        runtime_profile.save(profile_path)
        self.app = create_app(
            db_path=self.db_path,
            profile_path=profile_path,
            runtime_profile=runtime_profile,
            model_path="",
            report_dir=self.report_dir,
            get_title_translation_llm=lambda: None,
            persist_runtime_profile=lambda: None,
            reload_runtime_profile=lambda: None,
            queue_dashboard_rebuild=self.rebuild,
            cli_verbose=False,
        )
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.server.routers.companies.rescore_active_jobs", return_value=0)
    def test_promote_endpoint(self, _mock_rescore):
        _insert_job(self.db_path, "https://example.com/p", company="PromoCo")
        cid = _company_id(self.db_path, "PromoCo")
        resp = self.client.post(
            "/api/company/promote",
            json={"company_id": cid, "promote": True},
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["promote"])
        self.assertTrue(payload["touched"])
        self.rebuild.assert_called()
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

    def test_promote_status_by_job(self):
        job_id = _insert_job(self.db_path, "https://example.com/ps", company="StatCo")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        cid = _company_id(self.db_path, "StatCo")
        resp = self.client.get(f"/api/company/promote-status?job_id={job_id}")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["company_id"], cid)
        self.assertTrue(payload["promote"])


if __name__ == "__main__":
    unittest.main()
