"""Tests for Companies tab SSR table and main dashboard tab presence."""

import os
import re
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import ensure_db, list_company_catalog_rows, upsert_job
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
        self.assertIn("renameSelectedCompany", self.html)
        self.assertIn("deleteSelectedCompany", self.html)
        for fn_name in (
            "mergeSelectedCompanies",
            "renameSelectedCompany",
            "deleteSelectedCompany",
        ):
            body = _extract_js_function_body(self.html, fn_name)
            self.assertIn("waitForRebuildThenReload('companies')", body)
        delete_body = _extract_js_function_body(self.html, "deleteSelectedCompany")
        self.assertNotIn("removeCompanyRows", delete_body)

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

    def test_render_companies_table_html_rows(self):
        table = _render_companies_table_html(
            [
                {
                    "id": 7,
                    "display_name": "Acme",
                    "job_count": 3,
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
        self.assertIn('data-sort-job-count="3"', table)
        self.assertIn('data-sort-relevant="1"', table)
        self.assertIn('data-sort-not-relevant="0"', table)
        self.assertIn('data-sort-applied="1"', table)
        self.assertIn('data-sort-interview="0"', table)
        self.assertIn('data-sort-stopped="1"', table)
        self.assertIn('data-sort-alias-count="2"', table)
        self.assertIn(">3<", table)
        self.assertIn(">2<", table)
        self.assertIn("openCompanyPage", table)
        self.assertIn('data-sort-key="job_count"', table)
        self.assertIn(
            "companies-sortable companies-sort-active companies-sort-desc",
            table,
        )
        self.assertRegex(
            table,
            r'<th\b[^>]*\bdata-sort-key="job_count"[^>]*\baria-sort="descending"[^>]*>',
        )
        self.assertIn("Relevant", table)
        self.assertIn("Not relevant", table)
        self.assertIn("Applied", table)
        self.assertIn("Interview", table)
        self.assertIn("Stopped", table)
        self.assertIn("Aliases", table)

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
