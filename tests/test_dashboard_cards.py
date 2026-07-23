"""Tests for dashboard job card HTML builders."""

import unittest

from spejder.managers.dashboard_cards import (
    APPLIED_DATE_CLASS,
    APPLIED_DATE_TITLE,
    RELEVANCE_SCORE_CLASS,
    RELEVANCE_SCORE_TITLE,
    _build_job_cards,
    _format_applied_date,
)
from spejder.managers.dashboard_manager import _render_company_dashboard_html


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

    def test_build_job_cards_includes_hidden_checkbox(self):
        html = _build_job_cards([_applied_card_item(applied=0, viewed=0, hidden=0)])
        self.assertIn('class="hidden-wrap"', html)
        self.assertIn("setHidden(1, this.checked, this)", html)
        self.assertIn("Hidden</label>", html)

    def test_build_job_cards_hidden_panel_checks_hidden(self):
        html = _build_job_cards(
            [_applied_card_item(applied=0, viewed=0, hidden=1)],
            card_panel="hidden",
        )
        self.assertIn('class="hidden-wrap"><input type="checkbox" checked', html)

    def test_build_job_cards_hidden_checkbox_from_item_only(self):
        html = _build_job_cards(
            [_applied_card_item(applied=0, viewed=0, hidden=0)],
            card_panel="hidden",
        )
        self.assertIn('class="hidden-wrap"><input type="checkbox"  onchange="setHidden', html)

    def test_company_dashboard_partitions_hidden_only_to_hidden_tab(self):
        hidden_item = _applied_card_item(
            id=10,
            title="Parked Role",
            applied=0,
            viewed=0,
            hidden=1,
            category="relevant",
            position_link="https://example.com/parked",
        )
        visible_item = _applied_card_item(
            id=11,
            title="Visible Role",
            applied=0,
            viewed=1,
            hidden=0,
            category="relevant",
            position_link="https://example.com/visible",
        )
        html = _render_company_dashboard_html("Acme", [hidden_item, visible_item])

        self.assertIn(">Hidden (1)<", html)
        self.assertIn(">Relevant (1)<", html)

        hidden_panel_start = html.index('id="panel-hidden"')
        hidden_panel_end = html.index("</section>", hidden_panel_start)
        hidden_panel = html[hidden_panel_start:hidden_panel_end]
        self.assertIn("Parked Role", hidden_panel)
        self.assertNotIn("Visible Role", hidden_panel)

        relevant_panel_start = html.index('id="panel-relevant"')
        relevant_panel_end = html.index("</section>", relevant_panel_start)
        relevant_panel = html[relevant_panel_start:relevant_panel_end]
        self.assertIn("Visible Role", relevant_panel)
        self.assertNotIn("Parked Role", relevant_panel)

    def test_build_job_cards_shows_formatted_applied_date(self):
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        self.assertIn(
            f'class="{APPLIED_DATE_CLASS}" title="{APPLIED_DATE_TITLE}">Applied: 2024-05-10</span>',
            html,
        )
        self.assertIn(
            f'class="{RELEVANCE_SCORE_CLASS}" title="{RELEVANCE_SCORE_TITLE}">0.50</span>',
            html,
        )
        article_start = html.find("<article")
        article_end = html.find("</article>")
        card_html = html[article_start:article_end]
        self.assertIn(f'class="{RELEVANCE_SCORE_CLASS}"', card_html)
        self.assertIn(f'class="{APPLIED_DATE_CLASS}"', card_html)
        self.assertIn(f'title="{RELEVANCE_SCORE_TITLE}"', card_html)
        self.assertIn(f'title="{APPLIED_DATE_TITLE}"', card_html)

    def test_build_job_cards_applied_date_outside_feedback_row(self):
        # Applied date is a sibling above .feedback, not nested inside the checkbox row.
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        applied_idx = html.find(f'class="{APPLIED_DATE_CLASS}"')
        feedback_idx = html.find('<div class="feedback">')
        self.assertNotEqual(applied_idx, -1)
        self.assertNotEqual(feedback_idx, -1)
        self.assertLess(applied_idx, feedback_idx)

    def test_build_job_cards_shows_applied_date_for_applied_stage_panels(self):
        expected = (
            f'class="{APPLIED_DATE_CLASS}" title="{APPLIED_DATE_TITLE}">Applied: 2024-05-10</span>'
        )
        for panel in ("applied", "interview", "stopped"):
            with self.subTest(panel=panel):
                html = _build_job_cards([_applied_card_item()], card_panel=panel)
                self.assertIn(expected, html)

    def test_build_job_cards_adds_has_applied_date_class_when_date_present(self):
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        article_start = html.find("<article")
        article_end = html.find(">", article_start)
        article_tag = html[article_start:article_end + 1]
        self.assertIn("has-applied-date", article_tag)

    def test_build_job_cards_omits_has_applied_date_class_when_date_absent(self):
        html = _build_job_cards(
            [_applied_card_item(applied_at="")],
            card_panel="applied",
        )
        article_start = html.find("<article")
        article_end = html.find(">", article_start)
        article_tag = html[article_start:article_end + 1]
        self.assertNotIn("has-applied-date", article_tag)

    def test_build_job_cards_omits_applied_line_when_empty(self):
        html = _build_job_cards(
            [_applied_card_item(applied_at="")],
            card_panel="applied",
        )
        self.assertNotIn("Applied:", html)


if __name__ == "__main__":
    unittest.main()
