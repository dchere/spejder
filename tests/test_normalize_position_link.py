"""Tests for _normalize_position_link provider-specific rules (HR-Manager)."""

import os
import sqlite3
import tempfile
import unittest

from spejder.db import ensure_db, upsert_job
from spejder.db.utils import _normalize_position_link


HR_INIT_BASE = "https://candidate.hr-manager.net/ApplicationInit.aspx"
HR_FORM_BASE = (
    "https://candidate.hr-manager.net/ApplicationForm/SinglePageApplicationForm.aspx"
)


class HrManagerNormalizeTest(unittest.TestCase):
    def test_different_project_ids_differ(self):
        a = (
            f"{HR_INIT_BASE}?cid=316&ProjectId=188792&DepartmentId=21678"
            "&MediaId=5&SkipAdvertisement=False"
        )
        b = (
            f"{HR_INIT_BASE}?cid=316&ProjectId=188793&DepartmentId=21678"
            "&MediaId=5&SkipAdvertisement=False"
        )
        na = _normalize_position_link(a)
        nb = _normalize_position_link(b)
        self.assertNotEqual(na, nb)
        self.assertIn("ProjectId=188792", na)
        self.assertIn("ProjectId=188793", nb)

    def test_reorder_and_tracking_params_same_norm(self):
        left = (
            f"{HR_INIT_BASE}?MediaId=5&SkipAdvertisement=False"
            "&DepartmentId=21678&cid=316&ProjectId=188792"
        )
        right = (
            f"{HR_INIT_BASE}?ProjectId=188792&cid=316&DepartmentId=21678"
            "&MediaId=99&SkipAdvertisement=True"
        )
        expected = (
            f"{HR_INIT_BASE}?DepartmentId=21678&ProjectId=188792&cid=316"
        )
        self.assertEqual(_normalize_position_link(left), expected)
        self.assertEqual(_normalize_position_link(right), expected)

    def test_with_project_id_not_bare_path(self):
        link = f"{HR_INIT_BASE}?ProjectId=188792&MediaId=5"
        normalized = _normalize_position_link(link)
        self.assertNotEqual(normalized, HR_INIT_BASE)
        self.assertIn("ProjectId=188792", normalized)
        self.assertNotIn("MediaId", normalized)

    def test_application_form_same_rule(self):
        link = (
            f"{HR_FORM_BASE}?cid=2771&departmentId=18957&ProjectId=143569&MediaId=5"
        )
        expected = (
            f"{HR_FORM_BASE}?DepartmentId=18957&ProjectId=143569&cid=2771"
        )
        self.assertEqual(_normalize_position_link(link), expected)

    def test_no_project_id_passthrough_keeps_query(self):
        link = f"{HR_INIT_BASE}?cid=316&MediaId=5#frag"
        normalized = _normalize_position_link(link)
        self.assertEqual(normalized, f"{HR_INIT_BASE}?cid=316&MediaId=5")
        self.assertNotIn("#", normalized)

    def test_blank_project_id_passthrough(self):
        link = f"{HR_INIT_BASE}?ProjectId=&cid=316&MediaId=5"
        normalized = _normalize_position_link(link)
        self.assertEqual(normalized, f"{HR_INIT_BASE}?ProjectId=&cid=316&MediaId=5")
        self.assertIn("MediaId=5", normalized)

    def test_path_only_stays_path_only(self):
        self.assertEqual(_normalize_position_link(HR_INIT_BASE), HR_INIT_BASE)

    def test_fragment_stripped_with_project_id(self):
        link = f"{HR_INIT_BASE}?ProjectId=188792&cid=316#section"
        expected = f"{HR_INIT_BASE}?ProjectId=188792&cid=316"
        self.assertEqual(_normalize_position_link(link), expected)

    def test_apex_and_subdomain_hr_manager_hosts(self):
        apex = "https://hr-manager.net/ApplicationInit.aspx?ProjectId=1"
        sub = "https://candidate.hr-manager.net/ApplicationInit.aspx?ProjectId=1"
        self.assertEqual(
            _normalize_position_link(apex),
            "https://hr-manager.net/ApplicationInit.aspx?ProjectId=1",
        )
        self.assertEqual(
            _normalize_position_link(sub),
            "https://candidate.hr-manager.net/ApplicationInit.aspx?ProjectId=1",
        )

    def test_ensure_db_keeps_two_hr_manager_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            link_a = (
                f"{HR_INIT_BASE}?cid=316&ProjectId=188792&DepartmentId=21678"
                "&MediaId=5&SkipAdvertisement=False"
            )
            link_b = (
                f"{HR_INIT_BASE}?cid=316&ProjectId=188793&DepartmentId=21678"
                "&MediaId=5&SkipAdvertisement=False"
            )
            upsert_job(
                db_path,
                {
                    "title": "Job A",
                    "company": "Energinet",
                    "place": "",
                    "work_type": "Unknown",
                    "position_link": link_a,
                    "raw_text": "a",
                    "source": "IT-DAY Job Portal",
                },
            )
            upsert_job(
                db_path,
                {
                    "title": "Job B",
                    "company": "Energinet",
                    "place": "",
                    "work_type": "Unknown",
                    "position_link": link_b,
                    "raw_text": "b",
                    "source": "IT-DAY Job Portal",
                },
            )
            ensure_db(db_path)

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT position_link FROM jobs ORDER BY id"
                ).fetchall()
            self.assertEqual(len(rows), 2)
            norms = {_normalize_position_link(r[0]) for r in rows}
            self.assertEqual(len(norms), 2)
            self.assertTrue(all("ProjectId=" in n for n in norms))

    def test_ensure_db_merges_same_identity_rewrites_link(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            link_a = (
                f"{HR_INIT_BASE}?cid=316&ProjectId=188792&DepartmentId=21678"
                "&MediaId=5&SkipAdvertisement=False"
            )
            link_b = (
                f"{HR_INIT_BASE}?cid=316&ProjectId=188792&DepartmentId=21678"
                "&MediaId=99&SkipAdvertisement=True"
            )
            expected = (
                f"{HR_INIT_BASE}?DepartmentId=21678&ProjectId=188792&cid=316"
            )
            upsert_job(
                db_path,
                {
                    "title": "Job Same A",
                    "company": "Energinet",
                    "place": "",
                    "work_type": "Unknown",
                    "position_link": link_a,
                    "raw_text": "a",
                    "source": "IT-DAY Job Portal",
                },
            )
            upsert_job(
                db_path,
                {
                    "title": "Job Same B",
                    "company": "Energinet",
                    "place": "",
                    "work_type": "Unknown",
                    "position_link": link_b,
                    "raw_text": "b",
                    "source": "IT-DAY Job Portal",
                },
            )
            ensure_db(db_path)

            with sqlite3.connect(db_path) as conn:
                rows = conn.execute(
                    "SELECT position_link FROM jobs ORDER BY id"
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], expected)
            self.assertNotIn("MediaId", rows[0][0])


if __name__ == "__main__":
    unittest.main()
