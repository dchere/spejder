"""Tests for dashboard record shaping."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.workflows.dashboard import build_dashboard_record


class BuildDashboardRecordTest(unittest.TestCase):
    @patch("spejder.workflows.dashboard._format_skills")
    @patch("spejder.workflows.dashboard._fallback_description_text")
    @patch("spejder.workflows.dashboard.get_job_skills")
    @patch("spejder.workflows.dashboard._summary_for_display")
    @patch("spejder.workflows.dashboard._build_title_fields")
    def test_builds_record_from_minimal_row(
        self,
        mock_build_title_fields,
        mock_summary_for_display,
        mock_get_job_skills,
        mock_fallback_description_text,
        mock_format_skills,
    ):
        mock_build_title_fields.return_value = {
            "title": "Engineer",
            "title_english": "Engineer",
        }
        mock_summary_for_display.return_value = "Summary"
        mock_get_job_skills.return_value = ["python"]
        mock_fallback_description_text.return_value = "Description"
        mock_format_skills.return_value = "python"
        row = {"id": 42}

        result = build_dashboard_record(
            db_path="/tmp/jobs.db",
            runtime_profile=AppConfig(),
            title_translation_cache={},
            row=row,
            default_category="relevant",
        )

        self.assertEqual(result["id"], 42)
        self.assertEqual(result["title"], "Engineer")
        self.assertEqual(result["title_english"], "Engineer")
        self.assertEqual(result["summary"], "Summary")
        self.assertEqual(result["description"], "Description")
        self.assertEqual(result["skills"], "python")
        self.assertEqual(result["source"], "Unknown")
        self.assertEqual(result["work_type"], "Unknown")
        self.assertEqual(result["category"], "relevant")
        self.assertEqual(result["viewed"], 0)
        self.assertEqual(result["applied"], 0)

    @patch("spejder.workflows.dashboard._format_skills")
    @patch("spejder.workflows.dashboard._fallback_description_text")
    @patch("spejder.workflows.dashboard.get_job_skills")
    @patch("spejder.workflows.dashboard._summary_for_display")
    @patch("spejder.workflows.dashboard._build_title_fields")
    def test_translate_title_false_uses_raw_title_fields(
        self,
        mock_build_title_fields,
        mock_summary_for_display,
        mock_get_job_skills,
        mock_fallback_description_text,
        mock_format_skills,
    ):
        mock_summary_for_display.return_value = "Summary"
        mock_get_job_skills.return_value = ["python"]
        mock_fallback_description_text.return_value = "Description"
        mock_format_skills.return_value = "python"
        row = {
            "id": 7,
            "title": "Ingenior",
            "title_english": "Engineer",
        }

        result = build_dashboard_record(
            db_path="/tmp/jobs.db",
            runtime_profile=AppConfig(),
            title_translation_cache={},
            row=row,
            default_category="relevant",
            translate_title=False,
        )

        mock_build_title_fields.assert_not_called()
        self.assertEqual(result["title"], "Ingenior")
        self.assertEqual(result["title_english"], "Engineer")


if __name__ == "__main__":
    unittest.main()
