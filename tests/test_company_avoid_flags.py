"""Company Avoid flag: bits, merge OR, bulk viewed, upsert, API, UI wiring.

Scenario: An operator toggles Avoid on a catalog company (on or off), answers
Yes/No/Cancel, and later ingests jobs for that company; pipeline membership
jobs stay viewed; Promote/Touched stay independent.

Behavior: Avoid bit 4 persists; Yes bulk on views not-yet-viewed membership;
Yes bulk off unviews safe non-pipeline viewed membership without clearing
Promote/Touched; No flips flag only; Cancel posts nothing; upsert auto-views
when company is Avoid; merge Keep Avoid as OR; Companies chrome shows Avoid
between Promote and Action; Promote stays independently toggleable.
"""

import os
import re
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import (
    COMPANY_FLAG_AVOID,
    COMPANY_FLAG_PROMOTE,
    COMPANY_FLAG_PROMOTE_TOUCHED,
    company_has_flag,
    ensure_companies_from_jobs,
    ensure_db,
    get_company_flags,
    list_company_catalog_rows,
    merge_company_rows,
    set_company_avoid,
    set_company_promote,
    set_job_applied,
    set_job_hidden,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
    upsert_job,
)
from spejder.db.companies_flag_bits import merge_promote_flags
from spejder.db.connection import _connect
from spejder.managers.dashboard_companies_table import _render_companies_table_html
from spejder.server import create_app
from spejder.tests.test_dashboard_templates import _extract_js_function_body, _read_template


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


def _job_viewed(db_path: str, job_id: int) -> int:
    return int(_job_pipeline_state(db_path, job_id)["viewed"])


def _job_pipeline_state(db_path: str, job_id: int) -> dict:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(viewed, 0), COALESCE(hidden, 0),
                   COALESCE(applied, 0), COALESCE(on_interview, 0),
                   COALESCE(interview_stopped, 0), COALESCE(category, '')
            FROM jobs WHERE id=?
            """,
            (job_id,),
        )
        row = cur.fetchone()
        return {
            "viewed": int(row[0]),
            "hidden": int(row[1]),
            "applied": int(row[2]),
            "on_interview": int(row[3]),
            "interview_stopped": int(row[4]),
            "category": str(row[5] or ""),
        }
    finally:
        conn.close()


class CompanyAvoidFlagBitsTest(unittest.TestCase):
    def test_avoid_bit_and_merge_or(self):
        self.assertEqual(COMPANY_FLAG_AVOID, 4)
        flags = COMPANY_FLAG_AVOID
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_AVOID))
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertEqual(
            merge_promote_flags([COMPANY_FLAG_AVOID, 0]),
            COMPANY_FLAG_AVOID,
        )
        self.assertEqual(
            merge_promote_flags([COMPANY_FLAG_PROMOTE, COMPANY_FLAG_AVOID]),
            COMPANY_FLAG_PROMOTE | COMPANY_FLAG_AVOID | COMPANY_FLAG_PROMOTE_TOUCHED,
        )
        self.assertEqual(
            merge_promote_flags(
                [COMPANY_FLAG_PROMOTE | COMPANY_FLAG_AVOID, COMPANY_FLAG_PROMOTE]
            ),
            COMPANY_FLAG_PROMOTE | COMPANY_FLAG_AVOID,
        )


class CompanyAvoidBulkTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_no_flips_flag_only(self):
        job_id = _insert_job(self.db_path, "https://example.com/no", company="NoCo")
        cid = _company_id(self.db_path, "NoCo")
        self.assertEqual(_job_viewed(self.db_path, job_id), 0)
        result = set_company_avoid(self.db_path, cid, True, bulk=False)
        self.assertTrue(result["avoid"])
        self.assertTrue(result["changed"])
        self.assertEqual(result["jobs_updated"], 0)
        self.assertTrue(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_AVOID)
        )
        self.assertEqual(_job_viewed(self.db_path, job_id), 0)

        result_off = set_company_avoid(self.db_path, cid, False, bulk=False)
        self.assertFalse(result_off["avoid"])
        self.assertEqual(_job_viewed(self.db_path, job_id), 0)

    def test_yes_on_views_unviewed_membership(self):
        j1 = _insert_job(self.db_path, "https://example.com/y1", company="YesOn")
        j2 = _insert_job(
            self.db_path, "https://example.com/y2", company="YesOn", title="Dev"
        )
        set_job_viewed(self.db_path, j2, True)
        set_job_hidden(self.db_path, j1, True)
        before_j1 = _job_pipeline_state(self.db_path, j1)
        before_j2 = _job_pipeline_state(self.db_path, j2)
        self.assertEqual(before_j1["hidden"], 1)
        cid = _company_id(self.db_path, "YesOn")
        result = set_company_avoid(self.db_path, cid, True, bulk=True)
        self.assertTrue(result["avoid"])
        self.assertEqual(result["jobs_updated"], 1)
        after_j1 = _job_pipeline_state(self.db_path, j1)
        after_j2 = _job_pipeline_state(self.db_path, j2)
        self.assertEqual(after_j1["viewed"], 1)
        self.assertEqual(after_j1["hidden"], 0)
        self.assertEqual(after_j1["applied"], before_j1["applied"])
        self.assertEqual(after_j1["on_interview"], before_j1["on_interview"])
        self.assertEqual(after_j1["interview_stopped"], before_j1["interview_stopped"])
        self.assertEqual(after_j1["category"], before_j1["category"])
        self.assertEqual(after_j2["viewed"], 1)
        self.assertEqual(after_j2["hidden"], 0)
        self.assertEqual(after_j2["applied"], before_j2["applied"])
        self.assertEqual(after_j2["on_interview"], before_j2["on_interview"])
        self.assertEqual(after_j2["interview_stopped"], before_j2["interview_stopped"])
        self.assertEqual(after_j2["category"], before_j2["category"])

    def test_yes_off_unviews_safe_preserves_pipeline_and_promote(self):
        plain = _insert_job(self.db_path, "https://example.com/off1", company="YesOff")
        applied = _insert_job(
            self.db_path, "https://example.com/off2", company="YesOff", title="A"
        )
        interview = _insert_job(
            self.db_path, "https://example.com/off3", company="YesOff", title="I"
        )
        stopped = _insert_job(
            self.db_path, "https://example.com/off4", company="YesOff", title="S"
        )
        cid = _company_id(self.db_path, "YesOff")
        set_company_promote(self.db_path, cid, True, touch=True)
        set_company_avoid(self.db_path, cid, True, bulk=True)
        set_job_applied(self.db_path, applied, True)
        set_job_applied(self.db_path, interview, True)
        set_job_on_interview(self.db_path, interview, True)
        set_job_applied(self.db_path, stopped, True)
        set_job_interview_stopped(self.db_path, stopped, True)
        # Re-assert promote+touched after pipeline mutations (auto may already have set promote).
        set_company_promote(self.db_path, cid, True, touch=True)
        flags_before = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags_before, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags_before, COMPANY_FLAG_PROMOTE_TOUCHED))
        self.assertTrue(company_has_flag(flags_before, COMPANY_FLAG_AVOID))
        before = {
            jid: _job_pipeline_state(self.db_path, jid)
            for jid in (plain, applied, interview, stopped)
        }

        result = set_company_avoid(self.db_path, cid, False, bulk=True)
        self.assertFalse(result["avoid"])
        self.assertGreaterEqual(result["jobs_updated"], 1)
        after_plain = _job_pipeline_state(self.db_path, plain)
        self.assertEqual(after_plain["viewed"], 0)
        self.assertEqual(after_plain["hidden"], before[plain]["hidden"])
        self.assertEqual(after_plain["applied"], before[plain]["applied"])
        self.assertEqual(after_plain["on_interview"], before[plain]["on_interview"])
        self.assertEqual(
            after_plain["interview_stopped"], before[plain]["interview_stopped"]
        )
        self.assertEqual(after_plain["category"], before[plain]["category"])
        for jid in (applied, interview, stopped):
            after = _job_pipeline_state(self.db_path, jid)
            self.assertEqual(after["viewed"], 1)
            self.assertEqual(after["hidden"], before[jid]["hidden"])
            self.assertEqual(after["applied"], before[jid]["applied"])
            self.assertEqual(after["on_interview"], before[jid]["on_interview"])
            self.assertEqual(
                after["interview_stopped"], before[jid]["interview_stopped"]
            )
            self.assertEqual(after["category"], before[jid]["category"])
        flags_after = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags_after, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags_after, COMPANY_FLAG_PROMOTE_TOUCHED))
        self.assertFalse(company_has_flag(flags_after, COMPANY_FLAG_AVOID))

    def test_promote_and_avoid_independent(self):
        _insert_job(self.db_path, "https://example.com/ind", company="Indep")
        cid = _company_id(self.db_path, "Indep")
        set_company_promote(self.db_path, cid, True, touch=True)
        set_company_avoid(self.db_path, cid, True, bulk=False)
        flags = get_company_flags(self.db_path, cid)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_AVOID))
        set_company_promote(self.db_path, cid, False, touch=True)
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_PROMOTE))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_AVOID))
        set_company_avoid(self.db_path, cid, False, bulk=False)
        flags = get_company_flags(self.db_path, cid)
        self.assertFalse(company_has_flag(flags, COMPANY_FLAG_AVOID))
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_PROMOTE_TOUCHED))

    def test_merge_avoid_or(self):
        _insert_job(self.db_path, "https://example.com/m1", company="MergeA")
        _insert_job(self.db_path, "https://example.com/m2", company="MergeB")
        id_a = _company_id(self.db_path, "MergeA")
        id_b = _company_id(self.db_path, "MergeB")
        set_company_avoid(self.db_path, id_a, True, bulk=False)
        result = merge_company_rows(self.db_path, [id_a, id_b], "Merged")
        survivor = int(result["survivor_id"])
        flags = get_company_flags(self.db_path, survivor)
        self.assertTrue(company_has_flag(flags, COMPANY_FLAG_AVOID))

    def test_upsert_auto_views_when_avoid(self):
        _insert_job(self.db_path, "https://example.com/seed", company="UpsertAvoid")
        cid = _company_id(self.db_path, "UpsertAvoid")
        set_company_avoid(self.db_path, cid, True, bulk=False)
        job_id = _insert_job(
            self.db_path,
            "https://example.com/new-avoid",
            company="UpsertAvoid",
            title="New Role",
        )
        self.assertEqual(_job_viewed(self.db_path, job_id), 1)

    def test_catalog_row_includes_avoid(self):
        _insert_job(self.db_path, "https://example.com/cat", company="CatAvoid")
        cid = _company_id(self.db_path, "CatAvoid")
        set_company_avoid(self.db_path, cid, True, bulk=False)
        row = next(r for r in list_company_catalog_rows(self.db_path) if r["id"] == cid)
        self.assertTrue(row["avoid"])


class CompanyAvoidUiTest(unittest.TestCase):
    def test_companies_table_avoid_between_promote_and_action(self):
        table = _render_companies_table_html(
            [
                {
                    "id": 1,
                    "display_name": "AvoidCo",
                    "created_at": "2026-01-01T00:00:00+00:00",
                    "promote": False,
                    "avoid": True,
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
        header = table[table.index("<thead>") : table.index("</thead>")]
        self.assertLess(header.index(">Promote<"), header.index(">Avoid<"))
        self.assertLess(header.index(">Avoid<"), header.index(">Action<"))
        self.assertIn("company-avoid-checkbox", table)
        self.assertIn("setCompanyAvoid(1, this.checked, this)", table)
        self.assertIn('data-sort-avoid="1"', table)
        self.assertIn("checked", table)
        first_row = re.search(r"<tbody>\s*<tr\b[^>]*>(.*?)</tr>", table, re.DOTALL)
        self.assertIsNotNone(first_row)
        row_html = first_row.group(1)
        td_opens = re.findall(r"<td\b[^>]*>", row_html)
        tds = re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.DOTALL)
        self.assertIn('class="company-row-select"', tds[0])
        self.assertIn('class="company-promote-checkbox"', tds[1])
        self.assertIn('class="company-avoid-checkbox"', tds[2])
        self.assertIn('class="companies-action"', td_opens[3])
        self.assertIn("company-name-link", tds[4])

    def test_set_company_avoid_dialog_and_reload(self):
        avoid_js = _read_template("partials/dashboard_companies_avoid.js")
        body = _extract_js_function_body(avoid_js, "setCompanyAvoid")
        self.assertIn("openCompanyAvoidDialog", body)
        self.assertIn("/api/company/avoid", body)
        self.assertIn("bulk: choice === 'yes'", body)
        self.assertIn("choice === 'cancel'", body)
        self.assertIn("waitForRebuildThenReload('companies')", body)
        companies_js = _read_template("partials/dashboard_companies.js")
        self.assertNotIn("function setCompanyAvoid", companies_js)
        self.assertNotIn("openCompanyAvoidDialog", companies_js)
        # Promote stays independently toggleable (not disabled when Avoid on).
        promote_body = _extract_js_function_body(companies_js, "setCompanyPromote")
        self.assertNotIn("avoid", promote_body.lower())
        modal = _read_template("partials/dashboard_companies_avoid_modal.html")
        self.assertIn('id="company-avoid-modal"', modal)
        self.assertIn('id="company-avoid-yes"', modal)
        self.assertIn('id="company-avoid-no"', modal)
        self.assertIn('id="company-avoid-cancel"', modal)
        dashboard = _read_template("dashboard.html")
        self.assertIn(
            '{% include "partials/dashboard_companies_avoid_modal.html" %}',
            dashboard,
        )
        self.assertIn(
            '{% include "partials/dashboard_companies_avoid.js" %}',
            dashboard,
        )
        sort_js = _read_template("partials/dashboard_companies_sort.js")
        self.assertIn("sortAvoid", sort_js)


class CompanyAvoidApiTest(unittest.TestCase):
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

    def test_avoid_endpoint_rebuild_no_rescore(self):
        _insert_job(self.db_path, "https://example.com/api-a", company="ApiAvoid")
        cid = _company_id(self.db_path, "ApiAvoid")
        with patch(
            "spejder.server.routers.companies.rescore_active_jobs"
        ) as mock_rescore:
            resp = self.client.post(
                "/api/company/avoid",
                json={"company_id": cid, "avoid": True, "bulk": True},
            )
            mock_rescore.assert_not_called()
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["avoid"])
        self.assertTrue(payload["changed"])
        self.rebuild.assert_called()
        self.assertTrue(
            company_has_flag(get_company_flags(self.db_path, cid), COMPANY_FLAG_AVOID)
        )

    def test_avoid_endpoint_missing_company_400(self):
        resp = self.client.post(
            "/api/company/avoid",
            json={"company_id": 99999, "avoid": True, "bulk": False},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertFalse(resp.json().get("ok"))


if __name__ == "__main__":
    unittest.main()
