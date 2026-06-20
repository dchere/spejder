"""Tests for dashboard Jinja templates."""

import os
import unittest

from spejder.managers.dashboard_templates import (
    jinja_env,
    load_dashboard_card_corners_css,
)


def _minimal_dashboard_context():
    return {
        "title": "Test dashboard",
        "relevant_total_count": 0,
        "not_relevant_total_count": 0,
        "viewed_total": 0,
        "len_relevant_items": 0,
        "len_not_relevant_items": 0,
        "len_applied_items": 0,
        "len_interview_items": 0,
        "len_stopped_items": 0,
        "len_skills_items": 0,
        "relevant_cards": "",
        "not_relevant_cards": "",
        "applied_cards": "",
        "interview_cards": "",
        "stopped_cards": "",
        "skills_table_html": '<p class="empty">No skills found.</p>',
        "portrait_text": "",
        "has_portrait": False,
    }


class DashboardTemplatesTest(unittest.TestCase):
    def test_load_dashboard_card_corners_css_returns_file_content(self):
        css_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "templates",
            "partials",
            "dashboard_card_corners.css",
        )
        with open(css_path, encoding="utf-8") as f:
            expected = f.read()

        self.assertEqual(load_dashboard_card_corners_css(), expected)

    def test_dashboard_html_includes_corner_css_from_partial(self):
        template = jinja_env.get_template("dashboard.html")
        html = template.render(**_minimal_dashboard_context())

        self.assertIn(".relevance-score, .applied-date { position: absolute;", html)
        self.assertIn(".relevance-score { top: 8px; }", html)
        self.assertIn(".applied-date { bottom: 8px; }", html)
        self.assertIn(".applied-date { bottom: 8px; }", html)
        self.assertIn(".card.has-applied-date .feedback", html)
        self.assertIn("padding-bottom: 1.5rem", html)
        self.assertIn(".card.has-applied-date .company-feedback-input", html)
        self.assertIn("text-overflow: ellipsis", html)

    def test_company_dashboard_html_includes_corner_css_from_partial(self):
        template = jinja_env.get_template("company_dashboard.html")
        html = template.render(
            company_label="Acme",
            safe_company_label="Acme",
            len_company_items=0,
            len_relevant_items=0,
            len_not_relevant_items=0,
            len_applied_items=0,
            len_interview_items=0,
            len_stopped_items=0,
            relevant_cards="",
            not_relevant_cards="",
            applied_cards="",
            interview_cards="",
            stopped_cards="",
            skills_table_html="",
            len_skills_items=0,
        )
        self.assertIn(".applied-date { bottom: 8px; }", html)
        self.assertIn(".card.has-applied-date .feedback", html)


if __name__ == "__main__":
    unittest.main()
