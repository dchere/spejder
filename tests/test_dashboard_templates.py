"""Tests for dashboard Jinja templates."""

import os
import re
import unittest

from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT
from spejder.managers.dashboard_templates import (
    jinja_env,
    load_dashboard_card_corners_css,
)


_CORNER_CSS_START = ".relevance-score, .applied-date { position: absolute"
_CORNER_CSS_END = (
    ".card.has-applied-date .company-feedback-input { margin-bottom: 1.25rem; }"
)


def _normalize_css_whitespace(css: str) -> str:
    return re.sub(r"\s+", " ", css.strip())


def _extract_inlined_corner_css(template_text: str) -> str:
    start = template_text.index(_CORNER_CSS_START)
    end = template_text.index(_CORNER_CSS_END, start) + len(_CORNER_CSS_END)
    return template_text[start:end]


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
        "skills_empty_added_at_sort": SKILLS_EMPTY_ADDED_AT_SORT,
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

    def test_inlined_corner_css_matches_partial(self):
        canonical = _normalize_css_whitespace(load_dashboard_card_corners_css())
        templates_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "templates",
        )
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                path = os.path.join(templates_dir, name)
                with open(path, encoding="utf-8") as f:
                    template_text = f.read()
                extracted = _normalize_css_whitespace(
                    _extract_inlined_corner_css(template_text)
                )
                self.assertEqual(extracted, canonical)

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
