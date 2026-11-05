"""Integration tests for company catalog merge / rename / delete."""

import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.db import (
    ensure_companies_from_jobs,
    ensure_db,
    get_jobs_by_company,
    list_company_catalog_rows,
    resolve_company,
    upsert_job,
)
from spejder.db.companies import company_alias_key
from spejder.db.connection import _connect
from spejder.workflows.company_catalog import (
    delete_company,
    merge_companies,
    rename_company,
)


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


def _company_id(db_path: str, display_name: str) -> int:
    ensure_companies_from_jobs(db_path)
    for row in list_company_catalog_rows(db_path):
        if row["display_name"] == display_name:
            return int(row["id"])
    raise AssertionError(f"company not found: {display_name}")


class CompanyCatalogWorkflowTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_merge_rewrites_jobs_aliases_and_calls_dedupe(self):
        # Distinct alias keys (noise tokens like "inc" would collapse "Acme"/"Acme Inc").
        _insert(self.db_path, "https://example.com/a", "AlphaSoft", title="Role A")
        _insert(self.db_path, "https://example.com/b", "BetaWare", title="Role B")
        id_a = _company_id(self.db_path, "AlphaSoft")
        id_b = _company_id(self.db_path, "BetaWare")

        with patch(
            "spejder.workflows.company_catalog.merge_duplicate_positions",
            return_value={"rows_deleted": 0, "rows_updated": 0, "groups_merged": 0},
        ) as dedupe, patch(
            "spejder.workflows.company_catalog.prune_empty_companies",
            return_value=[],
        ) as prune:
            result = merge_companies(
                self.db_path, [id_a, id_b], "Merged Soft"
            )
            dedupe.assert_called_once_with(self.db_path)
            prune.assert_called_once_with(self.db_path, [id_b])

        self.assertEqual(result["survivor_id"], id_a)
        self.assertEqual(result["display_name"], "Merged Soft")
        self.assertEqual(result["deleted_ids"], [id_b])
        self.assertEqual(result["pruned_empty_ids"], [])

        ref = resolve_company(self.db_path, "AlphaSoft")
        self.assertTrue(ref.catalogued)
        self.assertEqual(ref.display_name, "Merged Soft")
        self.assertIn(company_alias_key("AlphaSoft"), ref.keys)
        self.assertIn(company_alias_key("BetaWare"), ref.keys)
        self.assertIn(company_alias_key("Merged Soft"), ref.keys)

        jobs = get_jobs_by_company(self.db_path, "BetaWare")
        self.assertEqual(len(jobs), 2)
        self.assertTrue(all(job["company"] == "Merged Soft" for job in jobs))
        # Loser id is gone
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM companies WHERE id=?", (id_b,))
            self.assertIsNone(cur.fetchone())
        finally:
            conn.close()

    def test_rename_updates_display_and_jobs(self):
        _insert(self.db_path, "https://example.com/a", "Old Name")
        cid = _company_id(self.db_path, "Old Name")
        result = rename_company(self.db_path, cid, "New Name")
        self.assertEqual(result["display_name"], "New Name")

        ref = resolve_company(self.db_path, "Old Name")
        self.assertEqual(ref.display_name, "New Name")
        jobs = get_jobs_by_company(self.db_path, "Old Name")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company"], "New Name")

    def test_merge_preserves_renamed_loser_aliases(self):
        # Rename rewrites jobs.company; old spelling must still resolve after merge.
        _insert(self.db_path, "https://example.com/keep", "KeepCo", title="Role Keep")
        _insert(self.db_path, "https://example.com/lose", "LoseInc", title="Role Lose")
        id_keep = _company_id(self.db_path, "KeepCo")
        id_lose = _company_id(self.db_path, "LoseInc")
        loseinc_key = company_alias_key("LoseInc")

        rename_company(self.db_path, id_lose, "LoseNew")
        self.assertTrue(resolve_company(self.db_path, "LoseInc").catalogued)
        self.assertIn(loseinc_key, resolve_company(self.db_path, "LoseNew").keys)

        with patch(
            "spejder.workflows.company_catalog.merge_duplicate_positions",
            return_value={"rows_deleted": 0, "rows_updated": 0, "groups_merged": 0},
        ):
            merge_companies(self.db_path, [id_keep, id_lose], "KeepCo")

        survivor = resolve_company(self.db_path, "KeepCo")
        self.assertTrue(survivor.catalogued)
        self.assertEqual(survivor.id, id_keep)
        self.assertIn(loseinc_key, survivor.keys)
        self.assertIn(company_alias_key("LoseNew"), survivor.keys)

        old = resolve_company(self.db_path, "LoseInc")
        self.assertTrue(old.catalogued)
        self.assertEqual(old.id, id_keep)
        self.assertEqual(old.display_name, "KeepCo")

    def test_rename_into_other_company_name_raises_and_leaves_state(self):
        _insert(self.db_path, "https://example.com/a", "AlphaCo", title="Role A")
        _insert(self.db_path, "https://example.com/b", "BetaCo", title="Role B")
        id_a = _company_id(self.db_path, "AlphaCo")
        id_b = _company_id(self.db_path, "BetaCo")
        beta_key = company_alias_key("BetaCo")

        with self.assertRaises(ValueError) as ctx:
            rename_company(self.db_path, id_a, "BetaCo")
        self.assertIn("already used", str(ctx.exception))
        self.assertIn("merge that company instead", str(ctx.exception))

        ref_a = resolve_company(self.db_path, "AlphaCo")
        ref_b = resolve_company(self.db_path, "BetaCo")
        self.assertEqual(ref_a.id, id_a)
        self.assertEqual(ref_a.display_name, "AlphaCo")
        self.assertEqual(ref_b.id, id_b)
        self.assertEqual(ref_b.display_name, "BetaCo")
        self.assertIn(beta_key, ref_b.keys)
        self.assertNotIn(beta_key, ref_a.keys)

        jobs_a = get_jobs_by_company(self.db_path, "AlphaCo")
        jobs_b = get_jobs_by_company(self.db_path, "BetaCo")
        self.assertEqual(len(jobs_a), 1)
        self.assertEqual(jobs_a[0]["company"], "AlphaCo")
        self.assertEqual(len(jobs_b), 1)
        self.assertEqual(jobs_b[0]["company"], "BetaCo")

    def test_merge_into_third_company_name_raises_and_leaves_state(self):
        _insert(self.db_path, "https://example.com/a", "AlphaCo", title="Role A")
        _insert(self.db_path, "https://example.com/b", "BetaCo", title="Role B")
        _insert(self.db_path, "https://example.com/c", "GammaCo", title="Role C")
        id_a = _company_id(self.db_path, "AlphaCo")
        id_b = _company_id(self.db_path, "BetaCo")
        id_c = _company_id(self.db_path, "GammaCo")
        gamma_key = company_alias_key("GammaCo")

        with patch(
            "spejder.workflows.company_catalog.merge_duplicate_positions",
            return_value={"rows_deleted": 0, "rows_updated": 0, "groups_merged": 0},
        ) as dedupe:
            with self.assertRaises(ValueError) as ctx:
                merge_companies(self.db_path, [id_a, id_b], "GammaCo")
            dedupe.assert_not_called()
        self.assertIn("already used", str(ctx.exception))
        self.assertIn("merge that company too", str(ctx.exception))

        for display, cid in (
            ("AlphaCo", id_a),
            ("BetaCo", id_b),
            ("GammaCo", id_c),
        ):
            ref = resolve_company(self.db_path, display)
            self.assertEqual(ref.id, cid)
            self.assertEqual(ref.display_name, display)
            jobs = get_jobs_by_company(self.db_path, display)
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["company"], display)

        self.assertIn(gamma_key, resolve_company(self.db_path, "GammaCo").keys)
        self.assertNotIn(gamma_key, resolve_company(self.db_path, "AlphaCo").keys)

    def test_delete_cascades_jobs_and_is_idempotent(self):
        job_id = _insert(self.db_path, "https://example.com/a", "Doomed")
        cid = _company_id(self.db_path, "Doomed")
        result = delete_company(self.db_path, cid)
        self.assertFalse(result["already_gone"])
        self.assertEqual(result["jobs_deleted"], 1)

        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM jobs WHERE id=?", (job_id,))
            self.assertIsNone(cur.fetchone())
            cur.execute("SELECT 1 FROM companies WHERE id=?", (cid,))
            self.assertIsNone(cur.fetchone())
        finally:
            conn.close()

        again = delete_company(self.db_path, cid)
        self.assertTrue(again["already_gone"])
        self.assertEqual(again["jobs_deleted"], 0)


if __name__ == "__main__":
    unittest.main()
