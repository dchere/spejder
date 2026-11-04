"""Enrichment report panel fetch uses triage-window, not uncapped category listing."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.workflows.enrichment import refresh_descriptions


class EnrichmentReportTriageWindowTest(unittest.TestCase):
    @patch("spejder.workflows.enrichment._render_html_dashboard")
    @patch("spejder.workflows.enrichment._build_skills_tab_items", return_value=[])
    @patch("spejder.workflows.enrichment.build_viewed_today_dashboard_records", return_value=[])
    @patch("spejder.workflows.enrichment.get_all_applied_jobs", return_value=[])
    @patch("spejder.workflows.enrichment.get_viewed_jobs_count", return_value=0)
    @patch("spejder.workflows.enrichment.get_jobs_count_by_category", return_value=4)
    @patch("spejder.workflows.enrichment.get_jobs_by_category_triage_window", return_value=[])
    @patch("spejder.workflows.enrichment.set_job_description")
    @patch("spejder.workflows.enrichment._get_title_english_for_row", return_value="Engineer")
    @patch("spejder.workflows.enrichment._is_low_quality_description", return_value=False)
    @patch("spejder.workflows.enrichment._has_invalid_description_marker", return_value=False)
    @patch("spejder.workflows.enrichment._enrich_raw_text_with_position_page", return_value="raw")
    @patch("spejder.workflows.enrichment._build_description_summary", return_value="summary")
    @patch("spejder.workflows.enrichment._learn_skill_patterns_from_positions")
    @patch("spejder.workflows.enrichment.materialize_relevant_and_applied_skills")
    @patch(
        "spejder.workflows.enrichment.get_jobs_for_description_refresh",
        return_value=[
            {
                "id": 1,
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/job",
                "raw_text": "raw",
                "description": "",
                "category": "relevant",
            }
        ],
    )
    @patch("spejder.workflows.enrichment.LocalLLM")
    @patch("spejder.workflows.enrichment.load_runtime_profile")
    @patch("spejder.workflows.enrichment.ensure_db")
    @patch("spejder.workflows.enrichment._ensure_skill_pattern_seed_migration")
    def test_panel_fetch_uses_triage_window(
        self,
        _mock_seed,
        _mock_ensure_db,
        mock_load_profile,
        mock_llm,
        _mock_get_rows,
        _mock_materialize_batch,
        mock_learn,
        _mock_summary,
        _mock_enrich,
        _mock_invalid,
        _mock_low_quality,
        _mock_title,
        _mock_set_desc,
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
                report_max_relevant_positions=2,
                report_max_not_relevant_positions=8,
            )
            mock_load_profile.return_value = profile
            mock_llm.return_value = MagicMock()
            mock_learn.return_value = {
                "considered_positions": 0,
                "new_skill_patterns": 0,
                "total_known_skill_patterns": 0,
            }

            refresh_descriptions(
                profile=os.path.join(tmp, "profile.json"),
                db=db_path,
                model="fake-model",
                quiet_model=True,
                report_dir=report_dir,
                allow_empty=True,
            )

        self.assertEqual(mock_triage.call_count, 2)
        by_cat = {c.args[1]: c for c in mock_triage.call_args_list}
        self.assertEqual(by_cat["relevant"].kwargs["non_hidden_limit"], 2)
        self.assertTrue(by_cat["relevant"].kwargs["unviewed_only"])
        self.assertEqual(by_cat["not relevant"].kwargs["non_hidden_limit"], 8)
        self.assertTrue(by_cat["not relevant"].kwargs["unviewed_only"])
        for call in mock_triage.call_args_list:
            self.assertNotIn("exclude_hidden", call.kwargs)
            self.assertNotIn("limit", call.kwargs)
        self.assertEqual(mock_count.call_count, 2)
        for call in mock_count.call_args_list:
            self.assertEqual(call.kwargs.get("exclude_hidden"), False)


if __name__ == "__main__":
    unittest.main()
