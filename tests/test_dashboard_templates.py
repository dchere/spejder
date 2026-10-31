"""Tests for dashboard Jinja templates."""

import os
import re
import unittest

from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT
from spejder.managers.dashboard_templates import (
    jinja_env,
    load_dashboard_card_corners_css,
)



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
        "len_viewed_today_items": 0,
        "len_applied_items": 0,
        "len_interview_items": 0,
        "len_stopped_items": 0,
        "len_skills_items": 0,
        "relevant_cards": "",
        "not_relevant_cards": "",
        "viewed_today_cards": "",
        "applied_cards": "",
        "interview_cards": "",
        "stopped_cards": "",
        "skills_table_html": '<p class="empty">No skills found.</p>',
        "skills_cloud_status_html": "",
        "skills_empty_added_at_sort": SKILLS_EMPTY_ADDED_AT_SORT,
        "portrait_text": "",
        "has_portrait": False,
        "report_mtime": "Wed, 01 Jan 2025 00:00:00 GMT",
    }


def _minimal_company_context():
    return {
        "company_label": "Acme",
        "safe_company_label": "Acme",
        "len_company_items": 0,
        "len_relevant_items": 0,
        "len_not_relevant_items": 0,
        "len_viewed_today_items": 0,
        "len_applied_items": 0,
        "len_interview_items": 0,
        "len_stopped_items": 0,
        "relevant_cards": "",
        "not_relevant_cards": "",
        "viewed_today_cards": "",
        "applied_cards": "",
        "interview_cards": "",
        "stopped_cards": "",
        "skills_table_html": "",
        "len_skills_items": 0,
    }


def _render_template(name: str) -> str:
    context = (
        _minimal_dashboard_context()
        if name == "dashboard.html"
        else _minimal_company_context()
    )
    return jinja_env.get_template(name).render(**context)


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
        self.assertIn(
            ".card:has(.applied-date) .feedback,\n.card.has-applied-date .feedback",
            html,
        )
        self.assertIn(
            ".card:has(.applied-date) .company-feedback-input,\n.card.has-applied-date .company-feedback-input",
            html,
        )
        self.assertIn("text-overflow: ellipsis", html)
        self.assertNotIn('id="btn-block-selected"', html)

    def test_templates_include_chrome_and_skills_css(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                source = _read_template(name)
                self.assertIn(
                    '{% include "partials/dashboard_chrome.css" %}', source
                )
                self.assertIn(
                    '{% include "partials/dashboard_card_corners.css" %}', source
                )
                html = _render_template(name)
                if name == "dashboard.html":
                    self.assertIn(
                        '{% include "partials/dashboard_skills.css" %}', source
                    )
                    self.assertIn(
                        ".skills-table th.skills-action, .skills-table td.skills-action { white-space: nowrap; }",
                        html,
                    )
                else:
                    self.assertNotIn(
                        '{% include "partials/dashboard_skills.css" %}', source
                    )
                    self.assertNotIn(".skills-table", html)
                    self.assertNotIn(".skills-action", html)

    def test_dashboard_html_shows_skills_bulk_bar_when_skills_present(self):
        template = jinja_env.get_template("dashboard.html")
        context = _minimal_dashboard_context()
        context["len_skills_items"] = 2
        context["skills_table_html"] = (
            '<table class="skills-table" id="skills-table"><tbody><tr></tr></tbody></table>'
        )
        context["skills_cloud_status_html"] = (
            '<div class="skills-cloud-status" id="skills-cloud-status">Bad cloud</div>'
        )
        html = template.render(**context)

        self.assertIn('id="btn-block-selected"', html)
        self.assertIn('id="btn-delete-selected"', html)
        self.assertIn('id="btn-sync-from-cv"', html)
        self.assertIn("skills-bulk-bar-end", html)
        self.assertNotIn("skills-legend", html)
        panel = html[html.index('id="panel-skills"') : html.index('id="panel-edited-today"')]
        self.assertLess(panel.index('id="btn-block-selected"'), panel.index('id="btn-sync-from-cv"'))
        self.assertLess(panel.index('id="btn-sync-from-cv"'), panel.index('class="skills-table"'))
        self.assertLess(panel.index('class="skills-table"'), panel.index('id="skills-cloud-status"'))

    def test_dashboard_html_keeps_sync_when_skills_empty(self):
        template = jinja_env.get_template("dashboard.html")
        context = _minimal_dashboard_context()
        context["len_skills_items"] = 0
        html = template.render(**context)

        self.assertIn('id="btn-sync-from-cv"', html)
        self.assertNotIn("skills-legend", html)
        self.assertNotIn('id="btn-block-selected"', html)
        self.assertNotIn('id="btn-delete-selected"', html)

    def test_dashboard_sync_status_shares_header_row_with_title(self):
        """Sync status shares the page-header row with the title (not under the toolbar)."""
        html = jinja_env.get_template("dashboard.html").render(
            **_minimal_dashboard_context()
        )
        header_match = re.search(
            r'class="page-header"[^>]*>(.*?)</div>\s*<div class="controls-wrap"',
            html,
            re.DOTALL,
        )
        self.assertIsNotNone(header_match, "page-header should wrap title + sync status before toolbar")
        header_html = header_match.group(1)
        self.assertIn("<h1>", header_html)
        self.assertIn('id="sync-inbox-status"', header_html)
        self.assertLess(header_html.index("<h1>"), header_html.index('id="sync-inbox-status"'))
        self.assertNotIn("controls-actions", header_html)

        actions_match = re.search(
            r'class="controls-actions"[^>]*>(.*?)</div>\s*</div>\s*</div>',
            html,
            re.DOTALL,
        )
        self.assertIsNotNone(actions_match)
        actions_html = actions_match.group(1)
        self.assertIn('id="btn-sync-inbox"', actions_html)
        self.assertIn('id="btn-profile"', actions_html)
        self.assertNotIn("sync-inbox-status", actions_html)

        self.assertIn(".page-header { display: flex;", html)
        self.assertIn("align-items: flex-end", html)
        self.assertIn("justify-content: space-between", html)
        self.assertIn("text-align: right", html)
        sync_css_start = html.index(".sync-inbox-status {")
        sync_css = html[sync_css_start : html.index("}", sync_css_start)]
        self.assertNotIn("text-overflow", sync_css)
        self.assertNotIn("ellipsis", sync_css)
        self.assertIn(".sync-inbox-status:empty { display: none;", html)
        self.assertIn('id="sync-inbox-status" class="sync-inbox-status" aria-live="polite"', html)

    def test_company_dashboard_html_includes_corner_css_from_partial(self):
        template = jinja_env.get_template("company_dashboard.html")
        html = template.render(**_minimal_company_context())
        self.assertIn(".applied-date { bottom: 8px; }", html)
        self.assertIn(".card.has-applied-date .feedback", html)

    def test_set_applied_true_branch_waits_reload_when_helper_exists(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                applied_body = _extract_js_function_body(
                    _render_template(name), "setApplied"
                )
                applied_branch = _extract_if_block(applied_body, "applied")
                self.assertIn(
                    "typeof waitForRebuildThenReload === 'function'", applied_branch
                )
                self.assertIn("waitForRebuildThenReload('applied')", applied_branch)
                self.assertNotIn("setMode", applied_branch)
                self.assertNotIn("switchTab", applied_branch)
                self.assertIn("Saved: applied", applied_branch)
                self.assertLess(
                    applied_branch.index("Saved: applied"),
                    applied_branch.index("waitForRebuildThenReload('applied')"),
                )
                # Soft-hidden triage cards already excluded from data-total.
                counted_at = applied_branch.index(
                    "countedInTotal = cardCountsInTriageTotal(card)"
                )
                clear_at = applied_branch.index("hiddenCheckbox.checked = false")
                bump_at = applied_branch.index(
                    "if (countedInTotal) bumpPanelTotal(card.parentElement, -1)"
                )
                self.assertLess(counted_at, clear_at)
                self.assertLess(clear_at, bump_at)
        main_html = _render_template("dashboard.html")
        company_html = _render_template("company_dashboard.html")
        self.assertIn("function waitForRebuildThenReload", main_html)
        self.assertNotIn("function waitForRebuildThenReload", company_html)

    def test_set_applied_false_branch_does_not_navigate(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                body = _extract_js_function_body(_render_template(name), "setApplied")
                marker = "removeCardFromAppliedStages(card)"
                self.assertIn(marker, body)
                # Unapply clears soft-hide UI before leaving applied stages.
                before_remove = body.split(marker, 1)[0]
                false_start = before_remove.rfind("} else {")
                false_prefix = before_remove[false_start:]
                self.assertIn("hiddenCheckbox.checked = false", false_prefix)
                self.assertIn("is-soft-filtered", false_prefix)
                false_branch = body.split(marker, 1)[1]
                # Uncheck: card placement only — no tab focus change.
                self.assertNotIn("setMode", false_branch)
                self.assertNotIn("waitForRebuildThenReload", false_branch)
                self.assertIn("stillViewed", false_branch)
                self.assertIn("panelEditedToday", false_branch)

    def test_set_hidden_soft_only(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _render_template(name)
                body = _extract_js_function_body(text, "setHidden")
                self.assertNotIn("setMode", body)
                self.assertIn("/api/hidden", body)
                self.assertNotIn("panelHidden", body)
                self.assertNotIn("removeAppliedOnlyUI(card)", body)
                self.assertIn("applyShowHiddenFilter", body)
                self.assertIn("bumpPanelTotal", body)

    def test_show_hidden_control_main_only(self):
        main = _render_template("dashboard.html")
        company = _render_template("company_dashboard.html")
        self.assertIn('id="show-hidden-wrap"', main)
        self.assertIn('id="show-hidden"', main)
        self.assertIn("spejder-show-hidden", main)
        self.assertIn("function applyShowHiddenFilter", main)
        self.assertIn("softHideFilterPanels", main)
        self.assertIn(".is-soft-filtered", main)
        self.assertNotIn('id="show-hidden-wrap"', company)
        self.assertNotIn("spejder-show-hidden", company)
        self.assertNotIn("function applyShowHiddenFilter", company)

        # Applied-stage counts include soft-hidden; Relevant/Not relevant exclude by
        # soft-hide checkbox state (not is-soft-filtered / Show hidden visibility).
        refresh_body = _extract_js_function_body(main, "refreshCounts")
        for panel in ("panelApplied", "panelInterview", "panelStopped"):
            self.assertIn(f"{panel}.querySelectorAll('.card').length", refresh_body)
            self.assertNotIn(
                f"{panel}.querySelectorAll('.card:not",
                refresh_body,
            )
        triage_exclude = ".card:not(:has(.hidden-wrap input:checked))"
        self.assertIn(triage_exclude, refresh_body)
        self.assertNotIn(
            "panelRelevant.querySelectorAll(\n"
            "                    '.card:not(.is-soft-filtered)'\n"
            "                ).length",
            refresh_body,
        )
        empty_body = _extract_js_function_body(main, "ensureEmptyState")
        self.assertIn(".card:not(.is-soft-filtered)", empty_body)
        # Initial Show-hidden filter must refresh badges/empty after CSS applies.
        loaded = main.split("DOMContentLoaded", 1)[1]
        filter_at = loaded.index("applyShowHiddenFilter()")
        refresh_at = loaded.index("refreshCounts()", filter_at)
        self.assertLess(filter_at, refresh_at)

    def test_main_set_relevant_unviewed_move_bumps_both_totals(self):
        body = _extract_js_function_body(
            _render_template("dashboard.html"), "setRelevant"
        )
        self.assertIn("countedInTotal = cardCountsInTriageTotal(card)", body)
        marker = (
            "if (card && !isViewed && card.parentElement !== targetPanel)"
        )
        start = body.index(marker)
        viewed = body.index(
            "} else if (card && isViewed && !appliedStagePanels",
            start,
        )
        unviewed = body[start:viewed]
        source_guard = (
            "card.parentElement === panelRelevant || "
            "card.parentElement === panelNotRelevant"
        )
        self.assertIn(source_guard, unviewed)
        self.assertIn("&& countedInTotal", unviewed)
        self.assertIn("if (countedInTotal) bumpPanelTotal(targetPanel, 1)", unviewed)
        source_bump = unviewed.index("bumpPanelTotal(card.parentElement, -1)")
        target_bump = unviewed.index("bumpPanelTotal(targetPanel, 1)")
        prepend = unviewed.index("targetPanel.prepend(card)")
        self.assertLess(unviewed.index(source_guard), source_bump)
        self.assertLess(source_bump, prepend)
        self.assertLess(target_bump, prepend)

    def test_set_relevant_not_relevant_clears_soft_hide_on_main(self):
        body = _extract_js_function_body(
            _render_template("dashboard.html"), "setRelevant"
        )
        marker = "if (signal === 'not relevant')"
        self.assertIn(marker, body)
        not_rel = body.split(marker, 1)[1]
        auto_view = not_rel.index("/api/viewed")
        soft_prefix = not_rel[:auto_view]
        self.assertIn("hiddenCheckbox.checked = false", soft_prefix)
        self.assertIn("is-soft-filtered", soft_prefix)
        self.assertIn("removeCardFromAppliedStages(card)", not_rel)
        self.assertIn("panelEditedToday", not_rel)
        # Capture counts-in-total before soft-hide clear; gate triage leave bumps.
        counted_at = body.index("countedInTotal = cardCountsInTriageTotal(card)")
        clear_at = body.index("hiddenCheckbox.checked = false", body.index(marker))
        self.assertLess(counted_at, clear_at)
        self.assertIn("fromTriage && isViewed && countedInTotal", not_rel)
        self.assertIn("fromTriage && countedInTotal", not_rel)

    def test_main_set_viewed_true_gates_bump_before_soft_hide_clear(self):
        body = _extract_js_function_body(
            _render_template("dashboard.html"), "setViewed"
        )
        viewed_true = _extract_if_block(body, "viewed")
        counted_at = viewed_true.index(
            "countedInTotal = cardCountsInTriageTotal(card)"
        )
        clear_at = viewed_true.index("hiddenCheckbox.checked = false")
        soft_clear = viewed_true.index("is-soft-filtered")
        bump_at = viewed_true.index(
            "bumpPanelTotal(card.parentElement, -1)"
        )
        self.assertLess(counted_at, clear_at)
        self.assertLess(clear_at, bump_at)
        self.assertLess(soft_clear, bump_at)
        self.assertIn("&& countedInTotal", viewed_true)

    def test_main_set_viewed_false_keeps_soft_hide_and_gates_bump(self):
        body = _extract_js_function_body(
            _render_template("dashboard.html"), "setViewed"
        )
        # False branch after `if (viewed) { ... } else {`
        marker = "const wasApplied = Boolean(appliedCheckbox && appliedCheckbox.checked)"
        self.assertIn(marker, body)
        false_branch = body.split(marker, 1)[1]
        self.assertIn("if (wasApplied)", false_branch)
        self.assertIn("hiddenCheckbox.checked = false", false_branch)
        self.assertIn("is-soft-filtered", false_branch)
        self.assertIn(
            "if (cardCountsInTriageTotal(card)) bumpPanelTotal(targetPanel, 1)",
            false_branch,
        )
        self.assertIn("applyShowHiddenFilter", false_branch)
        # Unconditional clear must not appear outside wasApplied.
        before_was = false_branch.split("if (wasApplied)", 1)[0]
        self.assertNotIn("hiddenCheckbox.checked = false", before_was)

    def test_company_set_viewed_true_moves_to_edited_today(self):
        body = _extract_js_function_body(
            _render_template("company_dashboard.html"), "setViewed"
        )
        self.assertNotIn("setMode", body)
        viewed_true = body.split("} else {", 1)[1]
        self.assertIn("panelEditedToday", viewed_true)
        self.assertNotIn("panelRelevant", viewed_true)
        self.assertNotIn("panelNotRelevant", viewed_true)
        # Company has no soft-hide UI / triage totals — no invent hide bumps.
        self.assertNotIn("cardCountsInTriageTotal", body)
        self.assertNotIn("hiddenCheckbox", body)
        self.assertNotIn("applyShowHiddenFilter", body)

    def test_company_bump_panel_total_is_empty_noop(self):
        body = _extract_js_function_body(
            _render_template("company_dashboard.html"), "bumpPanelTotal"
        )
        self.assertFalse(body.strip())
        self.assertNotIn("dataset.total", body)

    def test_templates_include_edited_today_tab(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _read_template(name)
                self.assertIn('id="btn-edited-today"', text)
                self.assertIn('id="panel-edited-today"', text)
                self.assertIn("'edited today'", text)

    def test_set_viewed_true_has_no_set_mode(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                body = _extract_js_function_body(_render_template(name), "setViewed")
                self.assertNotIn("setMode", body)
                self.assertIn("panelEditedToday", body)

    def test_templates_omit_hidden_tab(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                source = _read_template(name)
                html = _render_template(name)
                self.assertNotIn('id="btn-hidden"', source)
                self.assertNotIn('id="panel-hidden"', source)
                self.assertIn('{% include "partials/dashboard_card_stage.js" %}', source)
                self.assertIn('{% include "partials/dashboard_card_actions.js" %}', source)
                self.assertIn(
                    '{% include "partials/dashboard_card_applied_extras.js" %}', source
                )
                if name == "dashboard.html":
                    ops_js = source.index(
                        '{% include "partials/dashboard_ops.js" %}'
                    )
                    set_mode = source.index("setMode")
                    card_actions = source.index(
                        '{% include "partials/dashboard_card_actions.js" %}'
                    )
                    applied_extras = source.index(
                        '{% include "partials/dashboard_card_applied_extras.js" %}'
                    )
                    skills_flags = source.index(
                        '{% include "partials/dashboard_skills_flags.js" %}'
                    )
                    skills_bulk = source.index(
                        '{% include "partials/dashboard_skills_bulk.js" %}'
                    )
                    skills_sort = source.index(
                        '{% include "partials/dashboard_skills_sort.js" %}'
                    )
                    portrait = source.index(
                        '{% include "partials/dashboard_portrait.js" %}'
                    )
                    self.assertLess(ops_js, set_mode)
                    self.assertLess(set_mode, card_actions)
                    self.assertLess(card_actions, applied_extras)
                    self.assertLess(applied_extras, portrait)
                    self.assertLess(portrait, skills_flags)
                    self.assertLess(skills_flags, skills_bulk)
                    self.assertLess(skills_bulk, skills_sort)
                    self.assertIn('{% include "partials/dashboard_portrait.js" %}', source)
                    self.assertNotIn(
                        '{% include "partials/dashboard_skills.js" %}', source
                    )
                else:
                    card_stage = source.index(
                        '{% include "partials/dashboard_card_stage.js" %}'
                    )
                    card_actions = source.index(
                        '{% include "partials/dashboard_card_actions.js" %}'
                    )
                    applied_extras = source.index(
                        '{% include "partials/dashboard_card_applied_extras.js" %}'
                    )
                    self.assertLess(card_stage, card_actions)
                    self.assertLess(card_actions, applied_extras)
                    self.assertNotIn('{% include "partials/dashboard_ops.js" %}', source)
                    self.assertNotIn('{% include "partials/dashboard_portrait.js" %}', source)
                    self.assertNotIn('{% include "partials/dashboard_skills.js" %}', source)
                    self.assertNotIn(
                        '{% include "partials/dashboard_skills_flags.js" %}', source
                    )
                    self.assertNotIn(
                        '{% include "partials/dashboard_skills_bulk.js" %}', source
                    )
                    self.assertNotIn(
                        '{% include "partials/dashboard_skills_sort.js" %}', source
                    )
                self.assertIn("function setApplied", html)
                self.assertIn("function setHidden", html)
                self.assertIn("function setOnInterview", html)
                self.assertIn("function setInterviewStopped", html)
                self.assertIn("function saveCompanyFeedback", html)
                self.assertIn("function deleteJob", html)
                self.assertIn("function appendAppliedRawText", html)
                self.assertIn("function setCoverLetterRequested", html)
                self.assertIn("function saveCoverLetter", html)
                self.assertIn("function removeAppliedOnlyUI", html)

    def test_rendered_card_includes_delete_button_and_delete_job(self):
        from spejder.managers.dashboard_cards import _build_job_cards

        card = _build_job_cards(
            [
                {
                    "id": 7,
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
                    "viewed": 0,
                    "applied": 0,
                    "hidden": 0,
                }
            ]
        )
        self.assertIn('class="delete-job-btn"', card)
        self.assertIn('onclick="deleteJob(7, this)"', card)
        self.assertIn(">Delete</button>", card)
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                context = (
                    _minimal_dashboard_context()
                    if name == "dashboard.html"
                    else _minimal_company_context()
                )
                context["relevant_cards"] = card
                html = jinja_env.get_template(name).render(**context)
                self.assertIn('onclick="deleteJob(7, this)"', html)
                self.assertIn(">Delete</button>", html)
                self.assertIn("function deleteJob", html)
                body = _extract_js_function_body(html, "deleteJob")
                self.assertIn("window.confirm", body)
                self.assertIn("/api/job/delete", body)
                self.assertLess(
                    body.index("window.confirm"), body.index("/api/job/delete")
                )
                self.assertIn('`[data-job-id="${jobId}"]`', body)
                self.assertIn(".remove()", body)
                self.assertIn("bumpPanelTotal", body)
                self.assertIn("cardCountsInTriageTotal", body)
                self.assertIn("panelRelevant", body)
                self.assertIn("panelNotRelevant", body)
                self.assertIn("refreshCounts()", body)
                self.assertIn(".feedback-status", body)

    def test_remove_applied_only_ui_strips_applied_chrome(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                body = _extract_js_function_body(
                    _render_template(name), "removeAppliedOnlyUI"
                )
                self.assertIn(".applied-date", body)
                self.assertIn(".interview-wrap", body)
                self.assertIn(".stopped-wrap", body)
                self.assertIn("has-applied-date", body)

    def test_interview_handlers_check_true_navigates_uncheck_does_not(self):
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                text = _render_template(name)
                for fn, flag in (
                    ("setOnInterview", "onInterview"),
                    ("setInterviewStopped", "stopped"),
                ):
                    with self.subTest(function=fn):
                        body = _extract_js_function_body(text, fn)
                        # Navigation is gated on check-true only (shared helper or setMode).
                        self.assertIn(f"if ({flag}) {{", body)
                        gate = body.split(f"if ({flag}) {{", 1)[1]
                        self.assertIn(
                            "typeof waitForRebuildThenReload === 'function'", gate
                        )
                        self.assertIn(
                            "waitForRebuildThenReload(resolveAppliedStage(card).mode)",
                            gate,
                        )
                        self.assertIn(
                            "setMode(resolveAppliedStage(card).mode)", gate
                        )
                        self.assertNotIn("switchTab", gate)
                        self.assertEqual(body.count("waitForRebuildThenReload"), 2)
                        self.assertEqual(
                            body.count("setMode(resolveAppliedStage(card).mode)"), 1
                        )
                        saved_label = (
                            "Saved: on interview"
                            if flag == "onInterview"
                            else "Saved: stopped"
                        )
                        self.assertIn(saved_label, gate)
                        self.assertLess(
                            gate.index(saved_label),
                            gate.index(
                                "waitForRebuildThenReload(resolveAppliedStage(card).mode)"
                            ),
                        )
                        # moveCard always; focus change only inside the true gate.
                        self.assertLess(
                            body.index("moveCardToAppliedStage(card)"),
                            body.index(f"if ({flag}) {{"),
                        )
        self.assertIn(
            "function waitForRebuildThenReload",
            _render_template("dashboard.html"),
        )
        self.assertNotIn(
            "function waitForRebuildThenReload",
            _render_template("company_dashboard.html"),
        )

    def test_dashboard_html_embeds_report_mtime_meta(self):
        template = jinja_env.get_template("dashboard.html")
        html = template.render(**_minimal_dashboard_context())

        self.assertIn(
            '<meta name="spejder-report-mtime" content="Wed, 01 Jan 2025 00:00:00 GMT" />',
            html,
        )

    def test_dashboard_tab_buttons_use_switch_tab(self):
        text = _read_template("dashboard.html")
        for mode in ("relevant", "not relevant", "applied", "interview", "stopped", "skills", "edited today"):
            with self.subTest(mode=mode):
                self.assertIn(f"switchTab('{mode}')", text)
        self.assertNotIn("switchTab('hidden')", text)
        self.assertIn("btnPortrait.addEventListener('click', () => setMode('portrait'))", text)

    def test_dashboard_switch_tab_checks_stale_report(self):
        html = _render_template("dashboard.html")
        body = _extract_js_function_body(html, "switchTab")
        wait_body = _extract_js_function_body(html, "waitForRebuildThenReload")
        busy_body = _extract_js_function_body(html, "setRegenerateBusy")
        self.assertIn("fetchReportStatus", body)
        self.assertIn("pageReportMtime", body)
        self.assertIn("reloadWithTab", body)
        self.assertIn("await waitForRebuildThenReload(mode)", body)
        self.assertNotIn("reloading = true", body)
        self.assertIn("if (tabRefreshInProgress) return;", wait_body)
        self.assertLess(
            wait_body.index("if (tabRefreshInProgress) return;"),
            wait_body.index("tabRefreshInProgress = true"),
        )
        self.assertIn("tabRefreshInProgress = true", wait_body)
        self.assertIn("setRegenerateBusy(true)", wait_body)
        self.assertIn("pageReportMtime", wait_body)
        # Helper must wait for mtime change — no idle+same-mtime instant shortcut.
        self.assertNotIn("last_modified === pageReportMtime", wait_body)
        self.assertNotIn("setMode(mode, true)", wait_body)
        self.assertEqual(wait_body.count("setMode(mode)"), 2)
        self.assertIn("window.location.protocol === 'file:'", wait_body)
        self.assertIn("catch (_err)", wait_body)
        self.assertIn("classList.add('is-busy')", busy_body)
        self.assertIn("regenerateBusyCount += 1", busy_body)
        self.assertIn("regenerateBusyCount > 0", busy_body)
        self.assertNotIn("Refreshing…", body)
        self.assertNotIn("/api/report/rebuild", body)
        self.assertIn("tabRefreshStatus.textContent = ''", body)
        self.assertEqual(wait_body.count("reloading = true"), 2)
        self.assertIn(
            "reloading = true;\n                            reloadWithTab(mode);",
            wait_body,
        )
        self.assertIn(
            "reloading = true;\n                    reloadWithTab(mode);",
            wait_body,
        )
        self.assertIn(
            "if (!reloading) {\n                        setRegenerateBusy(false);\n                    }",
            wait_body,
        )
        self.assertIn("setMode(mode)", wait_body)

    def test_mid_sync_reload_generation_gate(self):
        html = _render_template("dashboard.html")
        reload_body = _extract_js_function_body(html, "maybeMidSyncReload")
        poll_body = _extract_js_function_body(html, "pollSyncStatus")
        init_body = _extract_js_function_body(html, "initInboxSyncStatus")
        self.assertIn("spejder-sync-reload-generation", html)
        self.assertIn("reload_generation", reload_body)
        self.assertIn("handledSyncReloadGeneration", reload_body)
        self.assertIn("sessionStorage.setItem", reload_body)
        # Process restart: server gen resets while sessionStorage may be stale.
        self.assertIn("gen < handled", reload_body)
        self.assertIn(
            "sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, '0')",
            reload_body,
        )
        self.assertLess(
            reload_body.index("gen < handled"),
            reload_body.index("gen <= handled"),
        )
        self.assertLess(
            reload_body.index("sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, '0')"),
            reload_body.index("gen <= handled"),
        )
        self.assertLess(
            reload_body.index(
                "sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, String(gen))"
            ),
            reload_body.index("reloadWithTab(currentMode)"),
        )
        self.assertIn("if (tabRefreshInProgress)", reload_body)
        self.assertLess(
            reload_body.index(
                "sessionStorage.setItem(SYNC_RELOAD_GENERATION_KEY, String(gen))"
            ),
            reload_body.index("if (tabRefreshInProgress)"),
        )
        self.assertIn("maybeMidSyncReload(data)", poll_body)
        self.assertIn("maybeMidSyncReload(data)", init_body)
        self.assertIn("if (data.running)", init_body)
        self.assertLess(
            init_body.index("maybeMidSyncReload(data)"),
            init_body.index("if (data.running)"),
        )
        self.assertIn("pollSyncStatus(btnEl, statusEl)", init_body)

    def test_regenerate_report_uses_busy_spin_not_status_text(self):
        html = _render_template("dashboard.html")
        self.assertIn(".toolbar-action-btn.is-busy svg", html)
        self.assertIn("@keyframes toolbar-action-spin", html)
        self.assertIn("prefers-reduced-motion: reduce", html)
        self.assertIn(".regenerate-status:empty { display: none;", html)
        body = _extract_js_function_body(html, "regenerateReport")
        busy_body = _extract_js_function_body(html, "setRegenerateBusy")
        self.assertIn("setRegenerateBusy(true)", body)
        self.assertIn("setRegenerateBusy(false)", body)
        self.assertIn("regenerateBusyCount += 1", busy_body)
        self.assertIn("regenerateBusyCount > 1", busy_body)
        self.assertIn("regenerateBusyCount > 0", busy_body)
        self.assertIn("classList.add('is-busy')", busy_body)
        self.assertIn("classList.remove('is-busy')", busy_body)
        self.assertIn("aria-busy", busy_body)
        self.assertIn("setAttribute('title', 'Regenerating…')", busy_body)
        self.assertIn("setAttribute('title', regenerateIdleTitle)", busy_body)
        self.assertIn("statusEl.textContent = ''", body)
        self.assertNotIn("statusEl.textContent = 'Regenerating…'", body)
        self.assertIn("Rebuild queued — refresh manually if the page does not update", body)
        self.assertIn("/api/report/rebuild", body)

    def test_clean_skills_button_and_relevant_uncheck_auto_clean(self):
        import tempfile

        from spejder.managers.dashboard_cards import (
            _build_job_cards,
            _render_html_from_items,
        )

        card = _build_job_cards(
            [
                {
                    "id": 7,
                    "source": "Test",
                    "company": "Acme",
                    "title": "Engineer",
                    "place": "",
                    "work_type": "Unknown",
                    "description": "",
                    "skills": "Python",
                    "position_link": "https://example.com/job",
                    "relevance_score": 0.5,
                    "category": "relevant",
                    "viewed": 0,
                    "applied": 0,
                    "hidden": 0,
                }
            ]
        )
        self.assertIn('class="clean-skills-btn"', card)
        self.assertIn('onclick="cleanJobSkills(7, this)"', card)
        self.assertIn(">Clean skills</button>", card)
        self.assertIn('class="feedback-actions"', card)
        self.assertLess(card.index("feedback-actions"), card.index("clean-skills-btn"))
        self.assertLess(card.index("clean-skills-btn"), card.index("delete-job-btn"))
        actions_start = card.index('class="feedback-actions"')
        actions_end = card.index("</span>", actions_start)
        actions_html = card[actions_start:actions_end]
        self.assertIn("clean-skills-btn", actions_html)
        self.assertIn("delete-job-btn", actions_html)
        for name in ("dashboard.html", "company_dashboard.html"):
            with self.subTest(template=name):
                context = (
                    _minimal_dashboard_context()
                    if name == "dashboard.html"
                    else _minimal_company_context()
                )
                context["relevant_cards"] = card
                html = jinja_env.get_template(name).render(**context)
                self.assertIn('onclick="cleanJobSkills(7, this)"', html)
                self.assertIn(">Clean skills</button>", html)
                self.assertIn(".feedback-actions", html)
                self.assertIn(".clean-skills-btn", html)
                self.assertIn("function cleanJobSkills", html)
                relevant = _extract_js_function_body(html, "setRelevant")
                self.assertIn("signal === 'not relevant'", relevant)
                self.assertIn(
                    "await cleanJobSkills(jobId, inputEl, { preserveViewed: true })",
                    relevant,
                )
                self.assertNotIn("skipConfirm", relevant)
                self.assertNotIn(
                    "Clear skills and re-extract them on the next sync?",
                    relevant,
                )
                self.assertLess(
                    relevant.index("/api/feedback"),
                    relevant.index(
                        "cleanJobSkills(jobId, inputEl, { preserveViewed: true })"
                    ),
                )
                cleaner = _extract_js_function_body(html, "cleanJobSkills")
                self.assertIn("/api/job/clean-skills", cleaner)
                self.assertNotIn("window.confirm", cleaner)
                self.assertNotIn("skipConfirm", cleaner)
                self.assertIn("preserveViewed", cleaner)
                self.assertIn("preserve_viewed", cleaner)
                self.assertIn("viewed_cleared", cleaner)
                self.assertIn(".skill-tags", cleaner)
                self.assertIn("skills-empty", cleaner)
                self.assertNotIn(
                    "if (appliedStagePanels.includes(el.parentElement)) return;",
                    cleaner,
                )
                self.assertIn("removeAppliedOnlyUI(el)", cleaner)
                self.assertLess(
                    cleaner.index("if (!data.viewed_cleared) return;"),
                    cleaner.index("removeAppliedOnlyUI(el)"),
                )
                self.assertIn("targetPanel.prepend(el)", cleaner)
                self.assertIn("bumpPanelTotal", cleaner)
                self.assertIn(
                    "countedInTotal = cardCountsInTriageTotal(el)", cleaner
                )
                self.assertIn("if (countedInTotal) bumpPanelTotal(targetPanel, 1)", cleaner)
                self.assertIn("panelRelevant", cleaner)
                self.assertIn("panelNotRelevant", cleaner)
                self.assertIn(".feedback-status", cleaner)
                self.assertIn('`[data-job-id="${jobId}"]`', cleaner)
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "report.html")
            _render_html_from_items(
                [
                    {
                        "source": "Test",
                        "company": "Acme",
                        "title": "Engineer",
                        "place": "",
                        "work_type": "Unknown",
                        "description": "",
                        "skills": "Python",
                        "position_link": "https://example.com/job",
                    }
                ],
                out,
                "Report",
            )
            with open(out, encoding="utf-8") as handle:
                report = handle.read()
        self.assertIn('class="skill-tag"', report)
        self.assertNotIn("clean-skills-btn", report)
        self.assertNotIn("cleanJobSkills", report)

    def test_dashboard_restores_tab_from_query_param(self):
        text = _read_template("dashboard.html")
        self.assertIn("function initTabFromUrl()", text)
        self.assertIn("params.get('tab')", text)
        self.assertIn("history.replaceState", text)
        self.assertIn("DOMContentLoaded", text)
        self.assertIn("'edited today'", text)
        self.assertIn("validModes", text)


if __name__ == "__main__":
    unittest.main()
