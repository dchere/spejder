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
from spejder.tests.test_dashboard_templates import (
    _css_rule_body,
    _extract_js_function_body,
    _read_template,
)


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
            runtime_profile=self.profile,
            db_path=self.db_path,
        )
        with open(self.out_html, encoding="utf-8") as f:
            self.html = f.read()
        self.table_html = _extract_skills_table(self.html)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_skills_cloud_status_rendered(self):
        self.assertIn('id="skills-cloud-status"', self.html)
        self.assertIn("Bad cloud:", self.html)

        self.assertIn(
            'data-sort-key="added_at" title="When the skill was first stored',
            self.table_html,
        )
        self.assertIn("skills-sort-active", self.table_html)
        self.assertIn(
            'class="skills-sortable skills-sort-active" data-sort-key="added_at"',
            self.table_html,
        )
        self.assertIn('data-sort-key="not_for_me"', self.table_html)
        self.assertIn(">Pass<", self.table_html)
        self.assertIn("setUnwantedSkill", self.table_html)
        self.assertIn("data-sort-not-for-me=", self.table_html)

        thead = re.search(r"<thead>.*?</thead>", self.table_html, flags=re.DOTALL).group(0)
        ths = re.findall(r"<th\b[^>]*>.*?</th>", thead, flags=re.DOTALL)
        header_texts = [re.sub(r"<[^>]+>", "", th).strip() for th in ths]
        self.assertEqual(
            header_texts,
            [
                "",
                "Have",
                "Want",
                "Pass",
                "Action",
                "Skill",
                "Added",
                "Source",
                "Job share",
                "Learned",
            ],
        )
        self.assertIn('id="skills-select-all"', ths[0])
        self.assertIn(
            'title="I already have this skill. Click to sort."',
            ths[1],
        )
        self.assertIn(
            'title="I want to learn this skill. Click to sort."',
            ths[2],
        )
        self.assertIn(
            'title="This skill is not for me. Score penalty. Click to sort."',
            ths[3],
        )
        self.assertIn('class="skills-action"', ths[4])
        self.assertIn(
            'title="Block = hide + teach the system (bad cloud). Delete = remove from profile and DB without teaching."',
            ths[4],
        )

        self.assertIn(">Block selected</button>", self.html)
        self.assertIn(">Delete selected</button>", self.html)
        self.assertIn('id="btn-sync-from-cv"', self.html)
        self.assertIn("Sync from CV", self.html)
        self.assertIn("syncSkillsFromCv", self.html)
        self.assertIn("skills-bulk-bar-end", self.html)
        self.assertNotIn("skills-legend", self.html)
        self.assertNotIn("skills-toolbar", self.html)
        self.assertNotIn(">Block</button>", self.table_html)
        self.assertNotIn(">Delete</button>", self.table_html)

        panel = re.search(
            r'<section id="panel-skills"[^>]*>.*?</section>',
            self.html,
            flags=re.DOTALL,
        ).group(0)
        bulk_idx = panel.index('class="skills-bulk-bar"')
        sync_idx = panel.index('id="btn-sync-from-cv"')
        scroll_idx = panel.index('class="skills-table-scroll"')
        table_idx = panel.index('class="skills-table"')
        cloud_idx = panel.index('id="skills-cloud-status"')
        self.assertLess(bulk_idx, sync_idx)
        self.assertLess(sync_idx, scroll_idx)
        self.assertLess(scroll_idx, table_idx)
        self.assertLess(table_idx, cloud_idx)
        self.assertIn(".skills-table-scroll", self.html)
        scroll_css_idx = self.html.index(".skills-table-scroll")
        self.assertIn("overflow: auto", self.html[scroll_css_idx : scroll_css_idx + 200])
        wrap_css_idx = self.html.index(".skills-wrap:not(.hidden)")
        wrap_css = self.html[wrap_css_idx : wrap_css_idx + 280]
        self.assertIn("calc(100vh - var(--dashboard-chrome-offset, 12rem))", wrap_css)
        self.assertIn("calc(100dvh - var(--dashboard-chrome-offset, 12rem))", wrap_css)
        table_css_idx = self.html.index(".skills-table {")
        table_css = self.html[table_css_idx : table_css_idx + 120]
        self.assertIn("border-collapse: separate", table_css)
        self.assertIn("border-spacing: 0", table_css)
        th_css_idx = self.html.index(".skills-table th {")
        self.assertIn("position: sticky", self.html[th_css_idx : th_css_idx + 200])
        self.assertIn("dashboard-sticky-chrome", self.html)
        self.assertIn("--dashboard-chrome-offset: 12rem", self.html)
        self.assertGreater(sync_idx, panel.index(">Block selected</button>"))
        self.assertGreater(sync_idx, panel.index(">Delete selected</button>"))

        row_htmls = _extract_skill_rows(self.table_html)
        sample_row = row_htmls[0]
        self.assertIn('title="I already have this skill."', sample_row)
        self.assertIn('title="I want to learn this skill."', sample_row)
        self.assertIn('title="This skill is not for me. Score penalty."', sample_row)
        row_names = []
        for row_html in row_htmls:
            td_opens = re.findall(r"<td\b[^>]*>", row_html)
            tds = re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.DOTALL)
            self.assertGreaterEqual(len(tds), 6)
            self.assertIn('class="skill-row-select"', tds[0])
            self.assertIn('class="skill-has-checkbox"', tds[1])
            self.assertIn('class="skill-learn-checkbox"', tds[2])
            self.assertIn('class="skill-unwanted-checkbox"', tds[3])
            self.assertIn('class="skills-action"', td_opens[4])
            self.assertIn('class="block-skill-btn"', tds[4])
            self.assertIn('class="delete-skill-btn"', tds[4])
            block_btn = re.search(
                r'<button[^>]*class="block-skill-btn"[^>]*>.*?</button>',
                tds[4],
                flags=re.DOTALL,
            ).group(0)
            delete_btn = re.search(
                r'<button[^>]*class="delete-skill-btn"[^>]*>.*?</button>',
                tds[4],
                flags=re.DOTALL,
            ).group(0)
            self.assertIn('title="Block: hide from extraction and teach the bad cloud"', block_btn)
            self.assertIn('aria-label="Block"', block_btn)
            self.assertIn("<circle", block_btn)
            self.assertIn(
                'title="Delete: remove from profile and DB without teaching the system"',
                delete_btn,
            )
            self.assertIn('aria-label="Delete"', delete_btn)
            self.assertIn("M3 6h18", delete_btn)
            self.assertNotIn("<circle", delete_btn)
            self.assertNotIn("M3 6h18", block_btn)
            row_names.append(tds[5].strip().lower())
        self.assertEqual(row_names, ["zebra", "alpha", "rust"])

        pass_th = next(
            th for th in ths if re.sub(r"<[^>]+>", "", th).strip() == "Pass"
        )
        self.assertIn(
            'title="This skill is not for me. Score penalty. Click to sort."',
            pass_th,
        )

    def test_skills_checkbox_columns_shrink_wrap_css(self):
        """Select…Pass columns are content-sized; Skill absorbs leftover; hit area on control."""
        css = _read_template("partials/dashboard_skills.css")
        shrink_sel = (
            r"\.skills-table th:nth-child\(-n\+4\),\s*"
            r"\.skills-table td:nth-child\(-n\+4\)"
        )
        shrink_body = _css_rule_body(css, shrink_sel)
        self.assertIn("width: 1%", shrink_body)
        self.assertIn("white-space: nowrap", shrink_body)
        self.assertIn("padding-left: 4px", shrink_body)
        self.assertIn("padding-right: 4px", shrink_body)
        # Header + body share the same center line (same th/td shrink selectors).
        self.assertIn("text-align: center", shrink_body)
        self.assertNotIn("transform: rotate", css)
        self.assertNotIn("text-overflow: ellipsis", shrink_body)
        # Action is 5th (skills-action); not under shrink -n+4.
        self.assertNotRegex(
            css,
            re.compile(
                r"\.skills-table th\.skills-action[^{]*\{[^}]*width:\s*1%",
                re.DOTALL,
            ),
        )
        hit_body = _css_rule_body(
            css,
            r'\.skills-table td:nth-child\(-n\+4\) input\[type="checkbox"\]',
        )
        self.assertIn("min-width: 1.1rem", hit_body)
        self.assertIn("min-height: 1.1rem", hit_body)
        name_body = _css_rule_body(
            css,
            r"\.skills-table th:nth-child\(6\),\s*"
            r"\.skills-table td:nth-child\(6\)",
        )
        self.assertIn("width: 100%", name_body)
        # Sort chrome stays (not display:none); inactive width is zero so labels truly center.
        indicator_body = _css_rule_body(css, r"^\s*\.skills-sort-indicator")
        self.assertIn("display: inline-block", indicator_body)
        self.assertIsNone(
            re.search(
                r"nth-child\(-n\+4\)[^{]*sort-indicator[^{]*\{[^}]*display:\s*none",
                css,
                flags=re.DOTALL,
            )
        )
        flag_indicator_body = _css_rule_body(
            css,
            r"\.skills-table th:nth-child\(-n\+4\) \.skills-sort-indicator",
        )
        self.assertIn("width: 0", flag_indicator_body)
        self.assertIn("margin-left: 0", flag_indicator_body)
        # Included in rendered dashboard.
        self.assertIn("th:nth-child(-n+4)", self.html)
        self.assertIn("width: 1%", self.html)

    def test_skills_action_column_centered_css(self):
        """Action th/td share one center line; not checkbox shrink, additive only."""
        css = _read_template("partials/dashboard_skills.css")
        action_body = _css_rule_body(
            css,
            r"\.skills-table th\.skills-action,\s*"
            r"\.skills-table td\.skills-action",
        )
        self.assertIn("text-align: center", action_body)
        self.assertIn("white-space: nowrap", action_body)

    def test_skills_row_highlight_classes_wired(self):
        bar_body = _extract_js_function_body(self.html, "updateSkillsBulkBar")
        self.assertIn("syncSkillRowHighlights", bar_body)
        highlight_body = _extract_js_function_body(self.html, "syncSkillRowHighlights")
        self.assertIn("skill-row-selected", highlight_body)
        self.assertIn("skill-row-added-today", highlight_body)
        self.assertIn("skillsLocalTodayKey", highlight_body)
        self.assertIn(
            ".skills-table tbody tr.skill-row-selected td",
            self.html,
        )
        self.assertIn(
            ".skills-table tbody tr.skill-row-added-today td",
            self.html,
        )
        self.assertIn(
            ".skills-table tbody tr.skill-row-selected.skill-row-added-today td:first-child",
            self.html,
        )
        self.assertIn("box-shadow: inset 3px 0 0 #c9962a", self.html)
        sort_body = _extract_js_function_body(self.html, "initSkillsTableSort")
        self.assertIn("syncSkillRowHighlights", sort_body)

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
        self.assertIn("function setUnwantedSkill", self.html)
        self.assertIn("sortNotForMe", self.html)


if __name__ == "__main__":
    unittest.main()
