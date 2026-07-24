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
        "len_hidden_items": 0,
        "len_skills_items": 0,
        "relevant_cards": "",
        "not_relevant_cards": "",
        "applied_cards": "",
        "interview_cards": "",
        "stopped_cards": "",
        "hidden_cards": "",
        "skills_table_html": '<p class="empty">No skills found.</p>',
        "skills_empty_added_at_sort": SKILLS_EMPTY_ADDED_AT_SORT,
        "portrait_text": "",
        "has_portrait": False,
        "report_mtime": "Wed, 01 Jan 2025 00:00:00 GMT",
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
            len_hidden_items=0,
            relevant_cards="",
            not_relevant_cards="",
            applied_cards="",
            interview_cards="",
            stopped_cards="",
            hidden_cards="",
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

    def test_set_hidden_does_not_call_set_mode(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                body = _extract_js_function_body(text, "setHidden")
                self.assertNotIn("setMode", body)
                hidden_branch = _extract_if_block(body, "hidden")
                self.assertIn("removeAppliedOnlyUI(card)", hidden_branch)
                self.assertIn("/api/hidden", body)
                else_branch = body.split("} else {", 1)[1]
                self.assertIn("panelRelevant", else_branch)
                self.assertIn("panelNotRelevant", else_branch)
                self.assertIn("relevantCheckbox.checked", else_branch)

    def test_set_relevant_keeps_still_hidden_cards(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                body = _extract_js_function_body(text, "setRelevant")
                self.assertIn("stillHidden", body)
                self.assertIn("hiddenCheckbox.checked", body)
                self.assertIn("!stillHidden", body)

    def test_company_set_viewed_true_moves_off_hidden(self):
        body = _extract_js_function_body(
            _read_template("company_dashboard.html"), "setViewed"
        )
        viewed_true = body.split("} else {", 1)[1]
        self.assertIn("panelHidden", viewed_true)
        self.assertIn("panelRelevant", viewed_true)
        self.assertIn("panelNotRelevant", viewed_true)
        self.assertIn("hiddenCheckbox.checked = false", viewed_true)

    def test_templates_include_hidden_tab(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                self.assertIn('id="btn-hidden"', text)
                self.assertIn('id="panel-hidden"', text)
                self.assertIn("function setHidden", text)
                self.assertIn("function removeAppliedOnlyUI", text)

    def test_remove_applied_only_ui_strips_applied_chrome(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                body = _extract_js_function_body(
                    _read_template(name), "removeAppliedOnlyUI"
                )
                self.assertIn(".applied-date", body)
                self.assertIn(".interview-wrap", body)
                self.assertIn(".stopped-wrap", body)
                self.assertIn("has-applied-date", body)

    def test_interview_handlers_still_call_set_mode(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                for fn in ("setOnInterview", "setInterviewStopped"):
                    with self.subTest(function=fn):
                        body = _extract_js_function_body(text, fn)
                        self.assertIn("setMode", body)

    def test_dashboard_html_embeds_report_mtime_meta(self):
        template = jinja_env.get_template("dashboard.html")
        html = template.render(**_minimal_dashboard_context())

        self.assertIn(
            '<meta name="spejder-report-mtime" content="Wed, 01 Jan 2025 00:00:00 GMT" />',
            html,
        )

    def test_dashboard_tab_buttons_use_switch_tab(self):
        text = _read_template("dashboard.html")
        for mode in ("relevant", "not relevant", "applied", "interview", "stopped", "hidden", "skills"):
            with self.subTest(mode=mode):
                self.assertIn(f"switchTab('{mode}')", text)
        self.assertIn("btnPortrait.addEventListener('click', () => setMode('portrait'))", text)

    def test_dashboard_switch_tab_checks_stale_report(self):
        body = _extract_js_function_body(_read_template("dashboard.html"), "switchTab")
        self.assertIn("fetchReportStatus", body)
        self.assertIn("pageReportMtime", body)
        self.assertIn("reloadWithTab", body)
        self.assertIn("Refreshing…", body)
        self.assertIn("tabRefreshStatus.textContent = ''", body)

    def test_dashboard_restores_tab_from_query_param(self):
        text = _read_template("dashboard.html")
        self.assertIn("function initTabFromUrl()", text)
        self.assertIn("params.get('tab')", text)
        self.assertIn("history.replaceState", text)
        self.assertIn("DOMContentLoaded", text)


if __name__ == "__main__":
    unittest.main()
