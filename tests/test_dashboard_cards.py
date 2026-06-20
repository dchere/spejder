"""Tests for dashboard job card HTML builders."""

import unittest

from spejder.managers.dashboard_cards import _build_job_cards, _format_applied_date


def _applied_card_item(**overrides):
    item = {
        "id": 1,
        "source": "Test",
        "company": "Acme",
        "title": "Engineer",
        "place": "",
        "work_type": "Unknown",
        "description": "",
        "skills": "",
        "position_link": "https://example.com/job",
        "relevance_score": 0.5,
        "category": "relevant",
        "viewed": 1,
        "applied": 1,
        "on_interview": 0,
        "interview_stopped": 0,
        "applied_at": "2024-05-10T14:30:00+00:00",
    }
    item.update(overrides)
    return item


class DashboardCardsTest(unittest.TestCase):
    def test_format_applied_date_empty(self):
        self.assertEqual(_format_applied_date(""), "")
        self.assertEqual(_format_applied_date("   "), "")

    def test_format_applied_date_iso_with_offset(self):
        self.assertEqual(
            _format_applied_date("2024-05-10T14:30:00+02:00"),
            "2024-05-10",
        )

    def test_format_applied_date_z_suffix(self):
        self.assertEqual(
            _format_applied_date("2024-05-10T14:30:00Z"),
            "2024-05-10",
        )

    def test_format_applied_date_malformed_t_fallback(self):
        self.assertEqual(
            _format_applied_date("2024-05-10Tnot-a-time"),
            "2024-05-10",
        )

    def test_format_applied_date_malformed_space_fallback(self):
        self.assertEqual(
            _format_applied_date("2024-05-10 not-a-time"),
            "2024-05-10",
        )

    def test_build_job_cards_shows_formatted_applied_date(self):
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        self.assertIn("Applied:</strong> 2024-05-10", html)

    def test_build_job_cards_omits_applied_line_when_empty(self):
        html = _build_job_cards(
            [_applied_card_item(applied_at="")],
            card_panel="applied",
        )
        self.assertNotIn("Applied:", html)


if __name__ == "__main__":
    unittest.main()
