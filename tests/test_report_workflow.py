"""Tests for report_workflow helpers."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.workflows.report_workflow import (
    _report_limit_value,
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
    report_links,
)


class ReportWorkflowTest(unittest.TestCase):
    def test_report_limit_value_parses_positive_int(self):
        self.assertEqual(_report_limit_value("12", 7), 12)

    def test_report_limit_value_falls_back_on_invalid(self):
        self.assertEqual(_report_limit_value("nope", 7), 7)
        self.assertEqual(_report_limit_value(0, 7), 7)

    def test_report_max_relevant_positions_uses_profile_field(self):
        profile = AppConfig(report_max_relevant_positions=9)
        self.assertEqual(_report_max_relevant_positions(profile), 9)

    def test_report_max_not_relevant_positions_uses_legacy_fallback(self):
        profile = AppConfig(
            report_max_relevant_positions=11,
            report_max_not_relevant_positions=0,
        )
        self.assertEqual(_report_max_not_relevant_positions(profile), 11)

    @patch("spejder.workflows.report_workflow.email_parser.load_files")
    def test_report_links_prints_counts(self, mock_load):
        mock_load.return_value = [
            {"links": ["https://example.com/a", "https://example.com/b"]},
            {"links": ["https://example.com/a"]},
        ]
        with patch("builtins.print") as mock_print:
            report_links("./inbox")
        printed = " ".join(str(call.args[0]) for call in mock_print.call_args_list)
        self.assertIn("https://example.com/a", printed)
        self.assertIn("2", printed)


if __name__ == "__main__":
    unittest.main()
