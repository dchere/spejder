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
        self.assertIn("Not for me", self.table_html)
        self.assertIn("setUnwantedSkill", self.table_html)
        self.assertIn("data-sort-not-for-me=", self.table_html)

        thead = re.search(r"<thead>.*?</thead>", self.table_html, flags=re.DOTALL).group(0)
        ths = re.findall(r"<th\b[^>]*>.*?</th>", thead, flags=re.DOTALL)
        header_texts = [re.sub(r"<[^>]+>", "", th).strip() for th in ths]
        self.assertEqual(
            header_texts,
            [
                "",
                "Skill",
                "Action",
                "Added",
                "Source",
                "Job share",
                "Learned",
                "I have",
                "Want to learn",
                "Not for me",
            ],
        )
        self.assertIn('id="skills-select-all"', ths[0])
        self.assertIn('class="skills-action"', ths[2])
        self.assertIn(
            'title="Block = hide + teach the system (bad cloud). Delete = remove from profile and DB without teaching."',
            ths[2],
        )

        self.assertIn(">Block selected</button>", self.html)
        self.assertIn(">Delete selected</button>", self.html)
        self.assertIn('id="btn-sync-from-cv"', self.html)
        self.assertIn("Sync from CV", self.html)
        self.assertIn("skills-legend", self.html)
        self.assertIn("Not for me</strong> = score penalty", self.html)
        self.assertIn("Block</strong> = hide + teach the system", self.html)
        self.assertIn("Delete</strong> = remove without teaching", self.html)
        self.assertIn("syncSkillsFromCv", self.html)
        self.assertIn("skills-bulk-bar-end", self.html)
        self.assertIn("text-align: right", self.html)
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
        legend_idx = panel.index('class="skills-legend"')
        table_idx = panel.index('class="skills-table"')
        cloud_idx = panel.index('id="skills-cloud-status"')
        self.assertLess(bulk_idx, sync_idx)
        self.assertLess(sync_idx, legend_idx)
        self.assertLess(legend_idx, table_idx)
        self.assertLess(table_idx, cloud_idx)
        self.assertGreater(sync_idx, panel.index(">Block selected</button>"))
        self.assertGreater(sync_idx, panel.index(">Delete selected</button>"))

        row_htmls = _extract_skill_rows(self.table_html)
        row_names = []
        for row_html in row_htmls:
            td_opens = re.findall(r"<td\b[^>]*>", row_html)
            tds = re.findall(r"<td\b[^>]*>(.*?)</td>", row_html, flags=re.DOTALL)
            self.assertGreaterEqual(len(tds), 4)
            self.assertIn('class="skill-row-select"', tds[0])
            self.assertIn('class="skills-action"', td_opens[2])
            self.assertIn('class="block-skill-btn"', tds[2])
            self.assertIn('class="delete-skill-btn"', tds[2])
            block_btn = re.search(
                r'<button[^>]*class="block-skill-btn"[^>]*>.*?</button>',
                tds[2],
                flags=re.DOTALL,
            ).group(0)
            delete_btn = re.search(
                r'<button[^>]*class="delete-skill-btn"[^>]*>.*?</button>',
                tds[2],
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
            row_names.append(tds[1].strip().lower())
        self.assertEqual(row_names, ["zebra", "alpha", "rust"])

        not_for_me_th = ths[-1]
        self.assertIn("Not for me = score penalty", not_for_me_th)

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
