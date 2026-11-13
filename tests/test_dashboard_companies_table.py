"""Tests for Companies tab SSR table and main dashboard tab presence."""

import html as html_lib
import json
import os
import re
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import ensure_db, list_company_catalog_rows, upsert_job
from spejder.db.connection import _connect
from spejder.managers.dashboard_companies_table import _render_companies_table_html
from spejder.managers.dashboard_manager import (
    _render_company_dashboard_html,
    _render_html_dashboard,
)
from spejder.managers.dashboard_templates import jinja_env
from spejder.tests.test_dashboard_templates import (
    _extract_js_function_body,
    _minimal_dashboard_context,
    _read_template,
)


class DashboardCompaniesTableTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Acme",
                "title": "Engineer",
                "position_link": "https://example.com/acme",
                "raw_text": "raw",
            },
        )
        self.out_html = os.path.join(self._tmpdir.name, "dashboard.html")
        rows = list_company_catalog_rows(self.db_path)
        _render_html_dashboard(
            [],
            [],
            [],
            self.out_html,
            "Test dashboard",
            companies_items=rows,
            runtime_profile=AppConfig(),
            db_path=self.db_path,
        )
        with open(self.out_html, encoding="utf-8") as f:
            self.html = f.read()

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_companies_tab_and_table_rendered(self):
        self.assertIn('id="btn-companies"', self.html)
        self.assertIn("Companies (", self.html)
        self.assertIn('id="panel-companies"', self.html)
        self.assertIn('class="companies-table-scroll"', self.html)
        self.assertIn('id="companies-table"', self.html)
        self.assertIn("dashboard-sticky-chrome", self.html)
        self.assertIn("--dashboard-chrome-offset: 12rem", self.html)
        self.assertIn(".companies-table-scroll", self.html)
        scroll_css_idx = self.html.index(".companies-table-scroll")
        self.assertIn("overflow: auto", self.html[scroll_css_idx : scroll_css_idx + 200])
        wrap_css_idx = self.html.index(".companies-wrap:not(.hidden)")
        wrap_css = self.html[wrap_css_idx : wrap_css_idx + 280]
        self.assertIn("calc(100vh - var(--dashboard-chrome-offset, 12rem))", wrap_css)
        self.assertIn("calc(100dvh - var(--dashboard-chrome-offset, 12rem))", wrap_css)
        table_css_idx = self.html.index(".companies-table {")
        table_css = self.html[table_css_idx : table_css_idx + 120]
        self.assertIn("border-collapse: separate", table_css)
        self.assertIn("border-spacing: 0", table_css)
        th_css_idx = self.html.index(".companies-table th {")
        self.assertIn("position: sticky", self.html[th_css_idx : th_css_idx + 200])
        self.assertIn("Acme", self.html)
        panel = self.html[
            self.html.index('id="panel-companies"') : self.html.index(
                'id="panel-edited-today"'
            )
        ]
        self.assertLess(
            panel.index('class="companies-bulk-bar"'),
            panel.index('class="companies-table-scroll"'),
        )
        self.assertLess(
            panel.index('class="companies-table-scroll"'),
            panel.index('id="companies-table"'),
        )
        self.assertIn("mergeSelectedCompanies", self.html)
        self.assertIn('id="company-merge-modal"', self.html)
        self.assertIn('id="company-merge-display-name"', self.html)
        self.assertIn('id="company-merge-name-list"', self.html)
        self.assertIn('id="company-merge-cancel"', self.html)
        self.assertIn('id="company-merge-confirm"', self.html)
        self.assertIn("buildCompanyMergeCandidates", self.html)
        self.assertIn("openCompanyMergeModal", self.html)
        self.assertIn("confirmCompanyMerge", self.html)
        self.assertIn("renameCompanyRow", self.html)
        self.assertNotIn("renameSelectedCompany", self.html)
        self.assertNotIn("btn-rename-company", self.html)
        self.assertIn("deleteSelectedCompanies", self.html)
        self.assertIn("deleteCompanyRow", self.html)
        self.assertIn("rename-company-btn", self.html)
        self.assertIn("delete-company-btn", self.html)
        self.assertIn("companies-action", self.html)
        merge_body = _extract_js_function_body(self.html, "mergeSelectedCompanies")
        self.assertIn("buildCompanyMergeCandidates", merge_body)
        self.assertIn("openCompanyMergeModal", merge_body)
        self.assertNotIn("prompt(", merge_body)
        self.assertNotIn("confirm(", merge_body)
        self.assertNotIn("/api/company/merge", merge_body)
        confirm_body = _extract_js_function_body(self.html, "confirmCompanyMerge")
        self.assertIn("/api/company/merge", confirm_body)
        self.assertIn("waitForRebuildThenReload('companies')", confirm_body)
        self.assertIn("updateCompaniesBulkBar()", confirm_body)
        self.assertIn("inFlight", confirm_body)
        self.assertNotIn("prompt(", confirm_body)
        self.assertNotIn("confirm(", confirm_body)
        self.assertIn("data-company-merge-dismiss", self.html)
        keydown_body = _extract_js_function_body(self.html, "onCompanyMergeKeydown")
        self.assertIn("Escape", keydown_body)
        self.assertIn("closeCompanyMergeModal", keydown_body)
        self.assertIn("isCompanyMergeInFlight", keydown_body)
        close_body = _extract_js_function_body(self.html, "closeCompanyMergeModal")
        self.assertIn("isCompanyMergeInFlight", close_body)
        self.assertIn("force", close_body)
        sort_body = _extract_js_function_body(self.html, "buildCompanyMergeCandidates")
        self.assertIn("b.jobCount - a.jobCount", sort_body)
        self.assertIn("dataset.displayName", sort_body)
        self.assertIn("dataset.jobCount", sort_body)
        rename_body = _extract_js_function_body(self.html, "renameCompanyRow")
        self.assertIn("waitForRebuildThenReload('companies')", rename_body)
        self.assertIn("/api/company/rename", rename_body)
        self.assertIn("prompt('Rename company to:', current)", rename_body)
        reload_body = _extract_js_function_body(self.html, "reloadCompaniesAfterDelete")
        self.assertIn("waitForRebuildThenReload('companies')", reload_body)
        self.assertIn("reloadWithTab('companies')", reload_body)
        delete_body = _extract_js_function_body(self.html, "deleteSelectedCompanies")
        self.assertIn("/api/company/delete-batch", delete_body)
        self.assertIn("reloadCompaniesAfterDelete", delete_body)
        self.assertIn("deleted_ids", delete_body)
        self.assertNotIn("removeCompanyRows", delete_body)
        row_body = _extract_js_function_body(self.html, "deleteCompanyRow")
        self.assertIn("/api/company/delete", row_body)
        self.assertIn("reloadCompaniesAfterDelete", row_body)
        self.assertIn("already_gone", row_body)
        bar_body = _extract_js_function_body(self.html, "updateCompaniesBulkBar")
        self.assertIn("count === 0", bar_body)
        self.assertIn("company-merge-modal", bar_body)
        self.assertIn("hasAttribute('hidden')", bar_body)
        self.assertIn("mergeModalOpen || count < 2", bar_body)
        self.assertNotIn("btn-rename-company", bar_body)
        self.assertIn("syncCompanyRowHighlights", bar_body)
        open_body = _extract_js_function_body(self.html, "openCompanyMergeModal")
        self.assertIn("removeEventListener('keydown', onCompanyMergeKeydown)", open_body)
        self.assertIn("addEventListener('keydown', onCompanyMergeKeydown)", open_body)
        highlight_body = _extract_js_function_body(self.html, "syncCompanyRowHighlights")
        self.assertIn("company-row-selected", highlight_body)
        self.assertIn("company-row-added-today", highlight_body)
        self.assertIn("companiesLocalTodayKey", highlight_body)
        self.assertIn(
            ".companies-table tbody tr.company-row-selected td",
            self.html,
        )
        self.assertIn(
            ".companies-table tbody tr.company-row-added-today td",
            self.html,
        )
        self.assertIn(
            ".companies-table tbody tr.company-row-selected.company-row-added-today td:first-child",
            self.html,
        )
        self.assertIn("box-shadow: inset 3px 0 0 #c9962a", self.html)
        sort_body = _extract_js_function_body(self.html, "initCompaniesTableSort")
        self.assertIn("syncCompanyRowHighlights", sort_body)

    def test_tab_order_skills_then_companies_then_edited_today(self):
        skills = self.html.index('id="btn-skills"')
        companies = self.html.index('id="btn-companies"')
        edited = self.html.index('id="btn-edited-today"')
        self.assertLess(skills, companies)
        self.assertLess(companies, edited)

    def test_company_dashboard_has_no_companies_tab(self):
        source = _read_template("company_dashboard.html")
        self.assertNotIn('id="btn-companies"', source)
        self.assertNotIn("panel-companies", source)
        self.assertNotIn("dashboard_companies", source)
        self.assertNotIn("dashboard_companies.js", source)
        self.assertNotIn("dashboard_companies.css", source)
        self.assertNotIn("dashboard_companies_merge", source)
        self.assertNotIn("company-merge-modal", source)
        self.assertIn("dashboard_company_page_actions.js", source)
        self.assertNotIn("dashboard-sticky-chrome", source)
        self.assertNotIn("skills-table-scroll", source)
        self.assertNotIn("companies-table-scroll", source)

    def test_company_page_actions_markup_with_company_id(self):
        html = _render_company_dashboard_html("Acme Corp", [], company_id=42)
        self.assertIn("company-page-heading", html)
        self.assertIn("<h1>Acme Corp Positions</h1>", html)
        self.assertIn('class="company-total"', html)
        self.assertIn("Total in DB: 0", html)
        self.assertIn('class="controls-actions"', html)
        self.assertIn('class="rename-company-btn"', html)
        self.assertIn('class="company-delete-btn"', html)
        self.assertIn('class="company-back-btn"', html)
        self.assertNotIn('class="company-merge-btn"', html)
        self.assertNotIn("Back to full report", html)
        self.assertIn(">Rename<", html)
        self.assertIn(">Delete<", html)
        self.assertIn(">Back<", html)
        self.assertIn("renameCompanyOnPage(42,", html)
        self.assertIn("deleteCompanyOnPage(42,", html)
        self.assertIn("backToFullReport()", html)
        self.assertIn("/api/company/rename", html)
        self.assertIn("/api/company/delete", html)
        self.assertIn("/company.html?company=", html)
        self.assertIn("encodeURIComponent(trimmed)", html)
        self.assertIn("apiUrl('/report.html')", html)
        self.assertNotIn("dashboard_companies.js", html)
        self.assertNotIn("dashboard_companies.css", html)
        # Rename · Delete · Back order inside controls-actions.
        actions_idx = html.index('class="controls-actions"')
        rename_idx = html.index('class="rename-company-btn"', actions_idx)
        delete_idx = html.index('class="company-delete-btn"', actions_idx)
        back_idx = html.index('class="company-back-btn"', actions_idx)
        self.assertLess(rename_idx, delete_idx)
        self.assertLess(delete_idx, back_idx)
        rename_body = _extract_js_function_body(html, "renameCompanyOnPage")
        self.assertIn("prompt('Rename company to:', current)", rename_body)
        self.assertIn("/api/company/rename", rename_body)
        self.assertIn(
            "location.href = apiUrl('/company.html?company=' + encodeURIComponent(trimmed))",
            rename_body,
        )
        self.assertNotIn("waitForRebuildThenReload", rename_body)
        self.assertNotIn("?tab=companies", rename_body)
        delete_body = _extract_js_function_body(html, "deleteCompanyOnPage")
        self.assertIn(
            'Delete company "${name}" and its ${jobs} job(s)? This cannot be undone.',
            delete_body,
        )
        self.assertIn("/api/company/delete", delete_body)
        self.assertIn("location.href = apiUrl('/report.html')", delete_body)
        self.assertNotIn("?tab=companies", delete_body)
        self.assertNotIn("waitForRebuildThenReload", delete_body)
        back_body = _extract_js_function_body(html, "backToFullReport")
        self.assertIn("location.href = apiUrl('/report.html')", back_body)
        actions_js = _read_template("partials/dashboard_company_page_actions.js")
        self.assertNotIn("waitForRebuildThenReload", actions_js)
        self.assertNotIn("?tab=companies", actions_js)
        self.assertIn("function backToFullReport", actions_js)
        chrome_css = _read_template("partials/dashboard_chrome.css")
        self.assertIn(".controls-actions {", chrome_css)
        companies_js = _read_template("partials/dashboard_companies.js")
        companies_delete = _extract_js_function_body(
            companies_js, "deleteCompanyRow"
        )
        self.assertIn(
            'Delete company "${name}" and its ${jobs} job(s)? This cannot be undone.',
            companies_delete,
        )

    def test_company_page_actions_escape_special_company_name(self):
        name = 'Acme "Corp" & <Co>'
        html = _render_company_dashboard_html(name, [], company_id=7)
        label_js = html_lib.escape(json.dumps(name), quote=True)
        safe = html_lib.escape(name)
        self.assertIn(f"<h1>{safe} Positions</h1>", html)
        self.assertIn('class="rename-company-btn"', html)
        self.assertIn('class="company-delete-btn"', html)
        self.assertIn(f"renameCompanyOnPage(7, {label_js}, this)", html)
        self.assertIn(f"deleteCompanyOnPage(7, {label_js}, 0, this)", html)
        self.assertIn(safe, html)

    def test_company_page_actions_hidden_without_company_id(self):
        html = _render_company_dashboard_html("Ephemeral Co", [])
        self.assertIn("<h1>Ephemeral Co Positions</h1>", html)
        self.assertIn('class="controls-actions"', html)
        self.assertIn('class="company-back-btn"', html)
        self.assertIn("backToFullReport()", html)
        self.assertNotIn('class="rename-company-btn"', html)
        self.assertNotIn('class="company-delete-btn"', html)
        self.assertNotIn(">Rename<", html)
        self.assertNotIn(">Delete<", html)
        self.assertNotIn("onclick=\"renameCompanyOnPage(", html)
        self.assertNotIn("onclick=\"deleteCompanyOnPage(", html)
        self.assertNotIn('<details class="company-aliases">', html)
        self.assertNotIn("Aliases (", html)
        # Handlers still defined (partial always included) for catalogued pages.
        self.assertIn("function renameCompanyOnPage", html)
        self.assertIn("function deleteCompanyOnPage", html)
        self.assertIn("function backToFullReport", html)

    def test_company_page_aliases_omits_display_name_and_empty(self):
        html_empty = _render_company_dashboard_html(
            "Acme Corp", [], company_id=1, aliases=["Acme Corp", "acme corp"]
        )
        self.assertNotIn('<details class="company-aliases">', html_empty)
        self.assertNotIn("Aliases (", html_empty)

        html = _render_company_dashboard_html(
            "Acme Corp",
            [],
            company_id=1,
            aliases=["Acme Corp", "Old Alias", "acme corp", "Another Name"],
        )
        self.assertIn('<details class="company-aliases">', html)
        self.assertIn("<h2>Aliases (2)</h2>", html)
        self.assertIn("summary::before", html)
        self.assertIn('content: "▸"', html)
        self.assertIn("<li>Old Alias</li>", html)
        self.assertIn("<li>Another Name</li>", html)
        # Display-name key matches must not reappear in the list.
        self.assertNotIn("<li>Acme Corp</li>", html)
        self.assertNotIn("<li>acme corp</li>", html)

    def test_ssr_tbody_order_added_desc(self):
        """Python/SSR tbody order is Added DESC before client JS runs."""
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Older Co",
                "title": "Engineer",
                "position_link": "https://example.com/older",
                "raw_text": "raw",
            },
        )
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "Newer Co",
                "title": "Engineer",
                "position_link": "https://example.com/newer",
                "raw_text": "raw",
            },
        )
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "UPDATE companies SET created_at=? WHERE display_name=?",
                ("2024-01-01T00:00:00+00:00", "Older Co"),
            )
            cur.execute(
                "UPDATE companies SET created_at=? WHERE display_name=?",
                ("2024-06-15T12:00:00+00:00", "Newer Co"),
            )
            conn.commit()
        finally:
            conn.close()

        rows = list_company_catalog_rows(self.db_path)
        names = [row["display_name"] for row in rows]
        # Name ASC would put Newer before Older; Added DESC must keep Newer first.
        self.assertLess(names.index("Newer Co"), names.index("Older Co"))

        table = _render_companies_table_html(rows)
        tbody = re.search(r"<tbody>(.*?)</tbody>", table, re.DOTALL)
        self.assertIsNotNone(tbody)
        body = tbody.group(1)
        self.assertLess(body.index("Newer Co"), body.index("Older Co"))

    def test_render_companies_table_html_rows(self):
        table = _render_companies_table_html(
            [
                {
                    "id": 7,
                    "display_name": "Acme",
                    "created_at": "2024-06-15T12:00:00+00:00",
                    "job_count": 3,
                    "not_viewed_count": 2,
                    "relevant_count": 1,
                    "not_relevant_count": 0,
                    "applied_count": 1,
                    "interview_count": 0,
                    "stopped_count": 1,
                    "alias_count": 2,
                }
            ]
        )
        self.assertIn('data-company-id="7"', table)
        self.assertIn("Acme", table)
        self.assertIn('data-job-count="3"', table)
        self.assertIn('data-sort-name="Acme"', table)
        self.assertIn('data-sort-added-at="2024-06-15T12:00:00+00:00"', table)
        self.assertIn('data-sort-job-count="3"', table)
        self.assertIn('data-sort-not-viewed="2"', table)
        self.assertIn('data-sort-relevant="1"', table)
        self.assertIn('data-sort-not-relevant="0"', table)
        self.assertIn('data-sort-applied="1"', table)
        self.assertIn('data-sort-interview="0"', table)
        self.assertIn('data-sort-stopped="1"', table)
        self.assertIn('data-sort-alias-count="2"', table)
        self.assertIn(">2024-06-15<", table)
        self.assertIn("Catalog row created: 2024-06-15T12:00:00+00:00", table)
        self.assertIn(">3<", table)
        self.assertIn(">2<", table)
        self.assertIn("openCompanyPage", table)
        self.assertIn("renameCompanyRow", table)
        self.assertIn("rename-company-btn", table)
        self.assertIn("deleteCompanyRow", table)
        self.assertIn("delete-company-btn", table)
        self.assertIn('class="companies-action"', table)
        self.assertLess(table.index("rename-company-btn"), table.index("delete-company-btn"))
        self.assertIn('data-sort-key="added_at"', table)
        self.assertIn('data-sort-key="not_viewed"', table)
        self.assertIn(
            "companies-sortable companies-sort-active companies-sort-desc",
            table,
        )
        self.assertRegex(
            table,
            r'<th\b[^>]*\bdata-sort-key="added_at"[^>]*\baria-sort="descending"[^>]*>',
        )
        self.assertNotRegex(
            table,
            r'<th\b[^>]*\bdata-sort-key="job_count"[^>]*\baria-sort=',
        )
        header = re.search(r"<thead>.*?</thead>", table, re.DOTALL)
        self.assertIsNotNone(header)
        header_html = header.group(0)
        self.assertLess(
            header_html.index("companies-select-all"),
            header_html.index(">Action<"),
        )
        self.assertLess(header_html.index(">Action<"), header_html.index(">Company<"))
        self.assertLess(header_html.index(">Company<"), header_html.index(">Added<"))
        self.assertLess(header_html.index(">Added<"), header_html.index(">Jobs<"))
        self.assertLess(header_html.index(">Jobs<"), header_html.index(">Not viewed<"))
        self.assertLess(header_html.index(">Not viewed<"), header_html.index(">Relevant<"))
        self.assertLess(header_html.index(">Jobs<"), header_html.index(">Aliases<"))
        self.assertIn("Relevant", table)
        self.assertIn("Not relevant", table)
        self.assertIn("Applied", table)
        self.assertIn("Interview", table)
        self.assertIn("Stopped", table)
        self.assertIn("Aliases", table)

        sort_js = _read_template("partials/dashboard_companies_sort.js")
        self.assertIn("key: 'added_at'", sort_js)
        self.assertIn("sortNotViewed", sort_js)

    def test_template_includes_companies_partials(self):
        source = _read_template("dashboard.html")
        self.assertIn('{% include "partials/dashboard_companies.css" %}', source)
        self.assertIn('{% include "partials/dashboard_companies.js" %}', source)
        self.assertIn(
            '{% include "partials/dashboard_companies_merge.js" %}', source
        )
        self.assertIn(
            '{% include "partials/dashboard_companies_merge_modal.html" %}',
            source,
        )
        self.assertIn('{% include "partials/dashboard_companies_sort.js" %}', source)
        companies_js = source.index(
            '{% include "partials/dashboard_companies.js" %}'
        )
        merge_js = source.index(
            '{% include "partials/dashboard_companies_merge.js" %}'
        )
        self.assertLess(companies_js, merge_js)
        self.assertIn("initCompaniesTableSort()", source)
        companies_partial = _read_template("partials/dashboard_companies.js")
        self.assertNotIn("function mergeSelectedCompanies", companies_partial)
        self.assertNotIn("prompt(", companies_partial.split("renameCompanyRow", 1)[0])
        merge_partial = _read_template("partials/dashboard_companies_merge.js")
        self.assertIn("function mergeSelectedCompanies", merge_partial)
        self.assertIn("function buildCompanyMergeCandidates", merge_partial)
        self.assertIn("function confirmCompanyMerge", merge_partial)
        self.assertIn("function onCompanyMergeKeydown", merge_partial)
        self.assertIn("function closeCompanyMergeModal", merge_partial)
        self.assertIn("isCompanyMergeInFlight", merge_partial)
        self.assertIn("inFlight", merge_partial)
        self.assertIn("Escape", merge_partial)
        self.assertNotIn("prompt(", merge_partial)
        self.assertNotIn("confirm(", merge_partial)
        self.assertIn("textContent", merge_partial)
        self.assertNotIn("innerHTML", merge_partial)
        modal_partial = _read_template(
            "partials/dashboard_companies_merge_modal.html"
        )
        self.assertIn('id="company-merge-modal"', modal_partial)
        self.assertIn('id="company-merge-display-name"', modal_partial)
        self.assertIn('id="company-merge-name-list"', modal_partial)
        self.assertIn("data-company-merge-dismiss", modal_partial)
        context = _minimal_dashboard_context()
        context["len_companies_items"] = 1
        context["companies_table_html"] = (
            '<table class="companies-table" id="companies-table">'
            "<tbody><tr></tr></tbody></table>"
        )
        html = jinja_env.get_template("dashboard.html").render(**context)
        self.assertIn('id="btn-merge-companies"', html)
        self.assertIn('id="company-merge-modal"', html)
        self.assertIn("initCompaniesTableSort()", html)
        self.assertRegex(html, re.compile(r'id="btn-skills".*?id="btn-companies".*?id="btn-edited-today"', re.DOTALL))


if __name__ == "__main__":
    unittest.main()
