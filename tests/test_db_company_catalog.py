"""Integration tests for company catalog schema, bootstrap, and resolve."""

import os
import tempfile
import unittest

from unittest.mock import patch

from spejder.db import (
    CompanyRef,
    delete_company_rows,
    ensure_companies_from_jobs,
    ensure_db,
    get_job_skills,
    get_jobs_by_company,
    list_company_catalog_rows,
    resolve_company,
    set_job_applied,
    set_job_feedback,
    set_job_hidden,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_skills,
    set_job_viewed,
    upsert_job,
)
from spejder.db.companies import company_alias_key
from spejder.db.connection import _connect


def _insert(db_path: str, link: str, company: str, title: str = "Engineer") -> int:
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


class CompanyCatalogDbTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_schema_creates_companies_tables(self):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('companies', 'company_aliases')"
            )
            names = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
        self.assertEqual(names, {"companies", "company_aliases"})

    def test_bootstrap_from_distinct_jobs_skips_blank(self):
        _insert(self.db_path, "https://example.com/a", "Acme")
        _insert(self.db_path, "https://example.com/b", "Beta")
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO jobs (source, company, title, place, work_type, position_link, "
                "raw_text, created_at, updated_at) VALUES "
                "('Test', '  ', 'Blank', '', 'Unknown', 'https://example.com/blank', "
                "'raw', datetime('now'), datetime('now'))"
            )
            conn.commit()
        finally:
            conn.close()

        created = ensure_companies_from_jobs(self.db_path)
        self.assertGreaterEqual(created, 0)
        rows = list_company_catalog_rows(self.db_path)
        names = {row["display_name"] for row in rows}
        self.assertIn("Acme", names)
        self.assertIn("Beta", names)
        self.assertNotIn("", names)
        self.assertNotIn("  ", names)

    def test_resolve_catalogued_and_ephemeral(self):
        _insert(self.db_path, "https://example.com/a", "Acme Corp")
        ensure_companies_from_jobs(self.db_path)

        ref = resolve_company(self.db_path, "Acme Corp")
        self.assertIsInstance(ref, CompanyRef)
        self.assertTrue(ref.catalogued)
        self.assertIsNotNone(ref.id)
        self.assertEqual(ref.display_name, "Acme Corp")
        self.assertIn(company_alias_key("Acme Corp"), ref.keys)

        ephemeral = resolve_company(self.db_path, "Never Seen LLC")
        self.assertFalse(ephemeral.catalogued)
        self.assertIsNone(ephemeral.id)
        self.assertEqual(ephemeral.display_name, "Never Seen LLC")
        self.assertEqual(ephemeral.keys, frozenset({company_alias_key("Never Seen LLC")}))

        blank = resolve_company(self.db_path, "  ")
        self.assertFalse(blank.catalogued)
        self.assertEqual(blank.keys, frozenset())

    def test_get_jobs_by_company_via_alias_key(self):
        _insert(self.db_path, "https://example.com/a", "Acme")
        _insert(self.db_path, "https://example.com/b", "Other", title="Other role")
        ensure_companies_from_jobs(self.db_path)

        jobs = get_jobs_by_company(self.db_path, "acme")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company"], "Acme")

    def test_upsert_does_not_auto_create_catalog(self):
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM company_aliases")
            cur.execute("DELETE FROM companies")
            conn.commit()
        finally:
            conn.close()

        # upsert_job must not create catalog rows (bootstrap is separate).
        from spejder.db.mutations_upsert import upsert_job as raw_upsert

        # Bypass ensure_db side-effects by writing via existing path after wipe.
        raw_upsert(
            self.db_path,
            {
                "source": "Test",
                "company": "Fresh Co",
                "title": "Dev",
                "position_link": "https://example.com/fresh",
                "raw_text": "raw",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM companies")
            count = int(cur.fetchone()[0])
        finally:
            conn.close()
        self.assertEqual(count, 0)
        self.assertFalse(resolve_company(self.db_path, "Fresh Co").catalogued)

    def test_list_company_catalog_rows_order_added_desc(self):
        """SSR default: newest catalog created_at first (name ASC only as tie-break)."""
        _insert(self.db_path, "https://example.com/older", "Older Co")
        _insert(self.db_path, "https://example.com/newer", "Newer Co")
        # Name ASC would put Newer before Older; Added DESC must reverse that.
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE companies SET created_at=? WHERE display_name=?",
                ("2024-01-01T00:00:00+00:00", "Older Co"),
            )
            cur.execute(
                "UPDATE companies SET created_at=? WHERE display_name=?",
                ("2024-06-15T12:00:00+00:00", "Newer Co"),
            )
            conn.commit()
        finally:
            conn.close()

        names = [row["display_name"] for row in list_company_catalog_rows(self.db_path)]
        self.assertEqual(names[:2], ["Newer Co", "Older Co"])

    def test_list_company_catalog_rows_stage_counts(self):
        relevant_id = _insert(self.db_path, "https://example.com/rel", "Acme", "Rel")
        not_rel_id = _insert(self.db_path, "https://example.com/nr", "Acme", "NR")
        applied_id = _insert(self.db_path, "https://example.com/app", "Acme", "App")
        interview_id = _insert(self.db_path, "https://example.com/int", "Acme", "Int")
        stopped_id = _insert(self.db_path, "https://example.com/stop", "Acme", "Stop")
        soft_id = _insert(self.db_path, "https://example.com/soft", "Acme", "Soft")

        self.assertTrue(set_job_feedback(self.db_path, relevant_id, "relevant"))
        self.assertTrue(set_job_feedback(self.db_path, not_rel_id, "not relevant"))
        self.assertTrue(set_job_feedback(self.db_path, soft_id, "relevant"))
        self.assertTrue(set_job_hidden(self.db_path, soft_id, True))
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))
        self.assertTrue(set_job_applied(self.db_path, interview_id, True))
        self.assertTrue(set_job_on_interview(self.db_path, interview_id, True))
        self.assertTrue(set_job_applied(self.db_path, stopped_id, True))
        self.assertTrue(set_job_interview_stopped(self.db_path, stopped_id, True))

        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        acme = rows["Acme"]
        self.assertEqual(acme["job_count"], 6)
        self.assertEqual(acme["relevant_count"], 2)  # relevant + soft-hidden relevant
        self.assertEqual(acme["not_relevant_count"], 1)
        self.assertEqual(acme["applied_count"], 1)
        self.assertEqual(acme["interview_count"], 1)
        self.assertEqual(acme["stopped_count"], 1)
        self.assertTrue(acme.get("created_at"))
        # Applied/interview/stopped auto-mark viewed; remaining three stay unviewed
        # (relevant + not relevant + soft-hidden relevant).
        self.assertEqual(acme["not_viewed_count"], 3)

    def test_list_company_catalog_rows_not_viewed_count(self):
        """not_viewed_count = viewed=0 over membership; soft-hidden included; viewed jobs excluded."""
        unviewed_id = _insert(self.db_path, "https://example.com/uv", "ViewCo", "Unviewed")
        soft_id = _insert(self.db_path, "https://example.com/soft-uv", "ViewCo", "SoftUnviewed")
        viewed_rel_id = _insert(self.db_path, "https://example.com/vr", "ViewCo", "ViewedRel")
        applied_id = _insert(self.db_path, "https://example.com/va", "ViewCo", "ViewedApp")

        self.assertTrue(set_job_feedback(self.db_path, unviewed_id, "relevant"))
        self.assertTrue(set_job_feedback(self.db_path, soft_id, "relevant"))
        self.assertTrue(set_job_hidden(self.db_path, soft_id, True))
        self.assertTrue(set_job_feedback(self.db_path, viewed_rel_id, "relevant"))
        self.assertTrue(set_job_viewed(self.db_path, viewed_rel_id, True))
        self.assertTrue(set_job_applied(self.db_path, applied_id, True))
        self.assertTrue(set_job_viewed(self.db_path, applied_id, True))

        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        row = rows["ViewCo"]
        self.assertEqual(row["job_count"], 4)
        self.assertEqual(row["not_viewed_count"], 2)  # unviewed + soft-hidden unviewed
        self.assertEqual(row["relevant_count"], 3)  # unviewed + soft + viewed relevant
        self.assertEqual(row["applied_count"], 1)
        self.assertTrue(row.get("created_at"))

    def test_list_company_catalog_rows_applied_excludes_relevant(self):
        """Applied + category relevant counts Applied only (exclusive buckets)."""
        job_id = _insert(self.db_path, "https://example.com/app-rel", "BucketCo", "AppRel")
        self.assertTrue(set_job_applied(self.db_path, job_id, True))
        # set_job_applied also sets category='relevant'; must not double-count.
        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        row = rows["BucketCo"]
        self.assertEqual(row["job_count"], 1)
        self.assertEqual(row["applied_count"], 1)
        self.assertEqual(row["relevant_count"], 0)

    def test_list_company_catalog_rows_viewed_still_relevant(self):
        """Viewed non-applied Relevant still increments relevant_count (no Edited-today carve-out)."""
        job_id = _insert(self.db_path, "https://example.com/view-rel", "ViewCo", "ViewRel")
        self.assertTrue(set_job_feedback(self.db_path, job_id, "relevant"))
        self.assertTrue(set_job_viewed(self.db_path, job_id, True))
        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        row = rows["ViewCo"]
        self.assertEqual(row["job_count"], 1)
        self.assertEqual(row["relevant_count"], 1)
        self.assertEqual(row["applied_count"], 0)

    def test_list_company_catalog_rows_alias_membership(self):
        _insert(self.db_path, "https://example.com/acme", "Acme")
        ensure_companies_from_jobs(self.db_path)
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM companies WHERE display_name=?", ("Acme",))
            cid = int(cur.fetchone()[0])
            alias_raw = "Acme Subsidiary"
            cur.execute(
                "INSERT INTO company_aliases (company_id, alias_raw, alias_key, created_at) "
                "VALUES (?, ?, ?, datetime('now'))",
                (cid, alias_raw, company_alias_key(alias_raw)),
            )
            conn.commit()
        finally:
            conn.close()

        _insert(self.db_path, "https://example.com/sub", alias_raw, title="Sub role")
        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        acme = rows["Acme"]
        self.assertEqual(acme["job_count"], 2)
        self.assertGreaterEqual(acme["alias_count"], 2)
        # Default category is not relevant; both membership jobs land there.
        self.assertEqual(acme["not_relevant_count"], 2)

    def test_delete_company_rows_batch_and_already_gone(self):
        job_a = _insert(self.db_path, "https://example.com/a", "Alpha")
        job_b = _insert(self.db_path, "https://example.com/b", "Beta")
        job_c = _insert(self.db_path, "https://example.com/c", "Gamma")
        set_job_skills(self.db_path, job_a, ["Python"])
        set_job_skills(self.db_path, job_b, ["SQL"])
        set_job_skills(self.db_path, job_c, ["Go"])
        self.assertTrue(get_job_skills(self.db_path, job_a))
        self.assertTrue(get_job_skills(self.db_path, job_b))
        self.assertTrue(get_job_skills(self.db_path, job_c))
        ensure_companies_from_jobs(self.db_path)
        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        id_a = int(rows["Alpha"]["id"])
        id_b = int(rows["Beta"]["id"])
        id_c = int(rows["Gamma"]["id"])
        gone_id = 999_999

        result = delete_company_rows(
            self.db_path, [id_a, id_a, -1, 0, id_b, gone_id]
        )
        self.assertEqual(result["company_ids"], [id_a, id_b, gone_id])
        self.assertEqual(result["deleted_ids"], [id_a, id_b])
        self.assertEqual(result["already_gone_ids"], [gone_id])
        self.assertEqual(result["jobs_deleted"], 2)
        self.assertEqual(result["count"], 2)
        remaining = {row["display_name"] for row in list_company_catalog_rows(self.db_path)}
        self.assertNotIn("Alpha", remaining)
        self.assertNotIn("Beta", remaining)
        self.assertIn("Gamma", remaining)
        self.assertEqual(resolve_company(self.db_path, "Gamma").id, id_c)
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            for job_id in (job_a, job_b):
                cur.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,))
                self.assertIsNone(cur.fetchone())
                cur.execute("SELECT 1 FROM job_skills WHERE job_id=?", (job_id,))
                self.assertIsNone(cur.fetchone())
            cur.execute("SELECT 1 FROM jobs WHERE id=?", (job_c,))
            self.assertIsNotNone(cur.fetchone())
        finally:
            conn.close()
        self.assertEqual(get_job_skills(self.db_path, job_c), ["Go"])
        self.assertEqual(get_job_skills(self.db_path, job_a), [])
        self.assertEqual(get_job_skills(self.db_path, job_b), [])

    def test_delete_company_rows_rolls_back_on_error(self):
        from spejder.db.companies_mutations import _cascade_delete_company_cur

        _insert(self.db_path, "https://example.com/a", "KeepMe")
        _insert(self.db_path, "https://example.com/b", "AlsoKeep")
        ensure_companies_from_jobs(self.db_path)
        rows = {row["display_name"]: row for row in list_company_catalog_rows(self.db_path)}
        id_a = int(rows["KeepMe"]["id"])
        id_b = int(rows["AlsoKeep"]["id"])
        calls = {"n": 0}
        real_cascade = _cascade_delete_company_cur

        def flaky(cur, company_id):
            calls["n"] += 1
            if calls["n"] == 1:
                return real_cascade(cur, company_id)
            raise RuntimeError("boom")

        with patch(
            "spejder.db.companies_mutations._cascade_delete_company_cur",
            side_effect=flaky,
        ):
            with self.assertRaises(RuntimeError):
                delete_company_rows(self.db_path, [id_a, id_b])

        remaining = {
            row["display_name"]: int(row["id"])
            for row in list_company_catalog_rows(self.db_path)
        }
        self.assertEqual(remaining["KeepMe"], id_a)
        self.assertEqual(remaining["AlsoKeep"], id_b)


if __name__ == "__main__":
    unittest.main()
