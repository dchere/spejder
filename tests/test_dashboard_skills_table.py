"""Tests for dashboard skills table HTML rendering."""

import json
import os
import re
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.db import ensure_db, upsert_skill_pattern
from spejder.extractors.skill_extractor.ui import (
    SKILLS_EMPTY_ADDED_AT_SORT,
    _build_skills_tab_items,
)
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.tests.skill_test_utils import stamp_skill_patterns_created_at


def _extract_skills_table(html: str) -> str:
    start = html.index('<table class="skills-table"')
    end = html.index("</table>", start) + len("</table>")
    return html[start:end]


def _extract_sort_keys(table_html: str) -> list[str]:
    headers = re.findall(
        r'<th class="skills-sortable[^"]*" data-sort-key="([^"]+)"',
        table_html,
    )
    return headers


def _extract_skill_rows(table_html: str) -> list[str]:
    return re.findall(
        r'<tr data-skill-key="[^"]*"[^>]*>(.*?)</tr>',
        table_html,
        flags=re.DOTALL,
    )


class DashboardSkillsTableTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.profile = AppConfig(user_skills=["Rust"])
        self.out_html = os.path.join(self._tmpdir.name, "dashboard.html")

        upsert_skill_pattern(self.db_path, name="Alpha", pattern=r"\bAlpha\b", source="test")
        upsert_skill_pattern(self.db_path, name="Zebra", pattern=r"\bZebra\b", source="test")

        stamp_skill_patterns_created_at(
            self.db_path,
            {
                "alpha": "2024-01-01T12:00:00+00:00",
                "zebra": "2024-06-01T12:00:00+00:00",
            },
        )

        skills_items = _build_skills_tab_items(self.db_path, self.profile)
        _render_html_dashboard(
            [],
            [],
            [],
            self.out_html,
            "Test dashboard",
            skills_items=skills_items,
        )
        with open(self.out_html, encoding="utf-8") as f:
            self.html = f.read()
        self.table_html = _extract_skills_table(self.html)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_skills_table_column_order_and_default_sort(self):
        sort_keys = _extract_sort_keys(self.table_html)
        self.assertEqual(sort_keys[:3], ["name", "added_at", "source"])

        self.assertIn(
            'data-sort-key="added_at" title="When the skill was first stored',
            self.table_html,
        )
        self.assertIn("skills-sort-active", self.table_html)
        self.assertIn(
            'class="skills-sortable skills-sort-active" data-sort-key="added_at"',
            self.table_html,
        )

        row_htmls = _extract_skill_rows(self.table_html)
        row_names = [
            re.search(r"<td>([^<]+)</td>", row_html).group(1).lower()
            for row_html in row_htmls
        ]
        self.assertEqual(row_names, ["zebra", "alpha", "rust"])

    def test_skills_table_added_at_attributes_and_display(self):
        alpha_row = re.search(
            r'<tr data-skill-key="alpha"[^>]*>.*?</tr>',
            self.table_html,
            flags=re.DOTALL,
        ).group(0)
        zebra_row = re.search(
            r'<tr data-skill-key="zebra"[^>]*>.*?</tr>',
            self.table_html,
            flags=re.DOTALL,
        ).group(0)
        rust_row = re.search(
            r'<tr data-skill-key="rust"[^>]*>.*?</tr>',
            self.table_html,
            flags=re.DOTALL,
        ).group(0)

        self.assertIn('data-sort-added-at="2024-01-01T12:00:00+00:00"', alpha_row)
        self.assertIn(">2024-01-01</td>", alpha_row)

        self.assertIn('data-sort-added-at="2024-06-01T12:00:00+00:00"', zebra_row)
        self.assertIn(">2024-06-01</td>", zebra_row)

        self.assertIn(f'data-sort-added-at="{SKILLS_EMPTY_ADDED_AT_SORT}"', rust_row)
        self.assertIn(">—</td>", rust_row)

    def test_skills_sort_sentinel_wired_into_js(self):
        expected = f"const SKILLS_EMPTY_ADDED_AT_SORT = {json.dumps(SKILLS_EMPTY_ADDED_AT_SORT)};"
        self.assertIn(expected, self.html)


if __name__ == "__main__":
    unittest.main()
