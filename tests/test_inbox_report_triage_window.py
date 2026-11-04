"""Inbox report panel fetch uses triage-window, not uncapped category listing."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.workflows.inbox_report import write_inbox_dashboard_report


class InboxReportTriageWindowTest(unittest.TestCase):
    @patch("spejder.workflows.inbox_report._render_html_dashboard")
    @patch("spejder.workflows.inbox_report._build_skills_tab_items", return_value=[])
    @patch("spejder.workflows.inbox_report.build_viewed_today_dashboard_records", return_value=[])
    @patch("spejder.workflows.inbox_report.get_all_applied_jobs", return_value=[])
    @patch("spejder.workflows.inbox_report.get_viewed_jobs_count", return_value=0)
    @patch("spejder.workflows.inbox_report.get_jobs_count_by_category", return_value=9)
    @patch("spejder.workflows.inbox_report.get_jobs_by_category_triage_window", return_value=[])
    def test_panel_fetch_uses_triage_window(
        self,
        mock_triage,
        mock_count,
        _mock_viewed_count,
        _mock_applied,
        _mock_viewed_today,
        _mock_skills,
        _mock_render,
    ):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            report_dir = os.path.join(tmp, "outbox")
            profile = AppConfig(
                report_max_relevant_positions=3,
                report_max_not_relevant_positions=11,
            )
            write_inbox_dashboard_report(
                db_path,
                profile,
                MagicMock(),
                report_dir,
            )

            self.assertEqual(mock_triage.call_count, 2)
            by_cat = {c.args[1]: c for c in mock_triage.call_args_list}
            self.assertEqual(by_cat["relevant"].args[0], db_path)
            self.assertEqual(by_cat["relevant"].kwargs["non_hidden_limit"], 3)
            self.assertTrue(by_cat["relevant"].kwargs["unviewed_only"])
            self.assertEqual(by_cat["not relevant"].kwargs["non_hidden_limit"], 11)
            self.assertTrue(by_cat["not relevant"].kwargs["unviewed_only"])
            for call in mock_triage.call_args_list:
                self.assertNotIn("exclude_hidden", call.kwargs)
                self.assertNotIn("limit", call.kwargs)
            self.assertEqual(mock_count.call_count, 2)
            for call in mock_count.call_args_list:
                self.assertEqual(call.kwargs.get("exclude_hidden"), False)
                self.assertTrue(call.kwargs.get("unviewed_only"))


if __name__ == "__main__":
    unittest.main()
