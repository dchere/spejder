"""Tests for Companies tab SSR table and main dashboard tab presence."""

import os
import re
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import ensure_db, list_company_catalog_rows, upsert_job
from spejder.db.connection import _connect
from spejder.managers.dashboard_companies_table import _render_companies_table_html
from spejder.managers.dashboard_manager import _render_html_dashboard
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
        self.assertIn('id="companies-table"', self.html)
        self.assertIn("Acme", self.html)
        self.assertIn("mergeSelectedCompanies", self.html)
        self.assertIn("renameCompanyRow", self.html)
        self.assertNotIn("renameSelectedCompany", self.html)
        self.assertNotIn("btn-rename-company", self.html)
        self.assertIn("deleteSelectedCompanies", self.html)
        self.assertIn("deleteCompanyRow", self.html)
        self.assertIn("rename-company-btn", self.html)
        self.assertIn("delete-company-btn", self.html)
        self.assertIn("companies-action", self.html)
        merge_body = _extract_js_function_body(self.html, "mergeSelectedCompanies")
        self.assertIn("waitForRebuildThenReload('companies')", merge_body)
        rename_body = _extract_js_function_body(self.html, "renameCompanyRow")
        self.assertIn("waitForRebuildThenReload('companies')", rename_body)
        self.assertIn("/api/company/rename", rename_body)
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
        self.assertNotIn("btn-rename-company", bar_body)
        self.assertIn("syncCompanyRowHighlights", bar_body)
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
        self.assertIn('{% include "partials/dashboard_companies_sort.js" %}', source)
        self.assertIn("initCompaniesTableSort()", source)
        context = _minimal_dashboard_context()
        context["len_companies_items"] = 1
        context["companies_table_html"] = (
            '<table class="companies-table" id="companies-table">'
            "<tbody><tr></tr></tbody></table>"
        )
        html = jinja_env.get_template("dashboard.html").render(**context)
        self.assertIn('id="btn-merge-companies"', html)
        self.assertIn("initCompaniesTableSort()", html)
        self.assertRegex(html, re.compile(r'id="btn-skills".*?id="btn-companies".*?id="btn-edited-today"', re.DOTALL))


if __name__ == "__main__":
    unittest.main()
