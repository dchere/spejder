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


def _brace_balanced_slice(text: str, open_brace: int) -> str:
    depth = 0
    for i in range(open_brace, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[open_brace + 1 : i]
    raise ValueError("Unbalanced braces")


def _extract_js_function_body(text: str, name: str) -> str:
    pattern = re.compile(
        rf"(?:async\s+)?function\s+{re.escape(name)}\s*\([^)]*\)\s*\{{",
        re.MULTILINE,
    )
    match = pattern.search(text)
    if not match:
        raise ValueError(f"Function {name!r} not found")
    return _brace_balanced_slice(text, match.end() - 1)


def _extract_if_block(body: str, condition_keyword: str) -> str:
    pattern = re.compile(
        rf"if\s*\([^)]*\b{re.escape(condition_keyword)}\b[^)]*\)\s*\{{",
        re.MULTILINE,
    )
    match = pattern.search(body)
    if not match:
        raise ValueError(f"if block for {condition_keyword!r} not found")
    return _brace_balanced_slice(body, match.end() - 1)


def _read_template(name: str) -> str:
    templates_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "templates",
    )
    path = os.path.join(templates_dir, name)
    with open(path, encoding="utf-8") as f:
        return f.read()


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
        self.assertNotIn('id="btn-block-selected"', html)

    def test_dashboard_html_shows_skills_bulk_bar_when_skills_present(self):
        template = jinja_env.get_template("dashboard.html")
        context = _minimal_dashboard_context()
        context["len_skills_items"] = 2
        context["skills_table_html"] = (
            '<table class="skills-table" id="skills-table"><tbody><tr></tr></tbody></table>'
        )
        html = template.render(**context)

        self.assertIn('id="btn-block-selected"', html)
        self.assertIn('id="btn-delete-selected"', html)

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

    def test_set_applied_true_branch_does_not_call_set_mode(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                body = _extract_js_function_body(text, "setApplied")
                applied_branch = _extract_if_block(body, "applied")
                self.assertNotIn("setMode", applied_branch)

    def test_interview_handlers_still_call_set_mode(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                for fn in ("setOnInterview", "setInterviewStopped"):
                    with self.subTest(function=fn):
                        body = _extract_js_function_body(text, fn)
                        self.assertIn("setMode", body)


if __name__ == "__main__":
    unittest.main()
