"""Tests for dashboard job card HTML builders."""

import os
import tempfile
import unittest

from spejder.managers.dashboard_cards import (
    APPLIED_DATE_CLASS,
    APPLIED_DATE_TITLE,
    RELEVANCE_SCORE_CLASS,
    RELEVANCE_SCORE_TITLE,
    _build_job_cards,
    _format_applied_date,
)
from spejder.managers.dashboard_manager import (
    _render_company_dashboard_html,
    _render_html_dashboard,
)


def _applied_card_item(**overrides):
    item = {
        "id": 1,
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
        "viewed": 1,
        "applied": 1,
        "on_interview": 0,
        "interview_stopped": 0,
        "applied_at": "2024-05-10T14:30:00+00:00",
    }
    item.update(overrides)
    return item


class DashboardCardsTest(unittest.TestCase):
    def test_format_applied_date_empty(self):
        self.assertEqual(_format_applied_date(""), "")
        self.assertEqual(_format_applied_date("   "), "")

    def test_format_applied_date_iso_with_offset(self):
        self.assertEqual(
            _format_applied_date("2024-05-10T14:30:00+02:00"),
            "2024-05-10",
        )

    def test_format_applied_date_z_suffix(self):
        self.assertEqual(
            _format_applied_date("2024-05-10T14:30:00Z"),
            "2024-05-10",
        )

    def test_format_applied_date_malformed_t_fallback(self):
        self.assertEqual(
            _format_applied_date("2024-05-10Tnot-a-time"),
            "2024-05-10",
        )

    def test_format_applied_date_malformed_space_fallback(self):
        self.assertEqual(
            _format_applied_date("2024-05-10 not-a-time"),
            "2024-05-10",
        )

    def test_build_job_cards_includes_delete_button(self):
        html = _build_job_cards([_applied_card_item(applied=0, viewed=0, hidden=0)])
        self.assertIn('class="delete-job-btn"', html)
        self.assertIn('onclick="deleteJob(1, this)"', html)
        self.assertIn(">Delete</button>", html)

    def test_build_job_cards_includes_hidden_checkbox(self):
        html = _build_job_cards([_applied_card_item(applied=0, viewed=0, hidden=0)])
        self.assertIn('class="hidden-wrap"', html)
        self.assertIn("setHidden(1, this.checked, this)", html)
        self.assertIn("Hidden</label>", html)

    def test_build_job_cards_hidden_panel_checks_hidden(self):
        html = _build_job_cards(
            [_applied_card_item(applied=0, viewed=0, hidden=1)],
            card_panel="hidden",
        )
        self.assertIn('class="hidden-wrap"><input type="checkbox" checked', html)

    def test_build_job_cards_hidden_checkbox_from_item_only(self):
        html = _build_job_cards(
            [_applied_card_item(applied=0, viewed=0, hidden=0)],
            card_panel="hidden",
        )
        self.assertIn('class="hidden-wrap"><input type="checkbox"  onchange="setHidden', html)

    def test_company_dashboard_partitions_hidden_only_to_hidden_tab(self):
        hidden_item = _applied_card_item(
            id=10,
            title="Parked Role",
            applied=0,
            viewed=0,
            hidden=1,
            category="relevant",
            position_link="https://example.com/parked",
        )
        visible_item = _applied_card_item(
            id=11,
            title="Visible Role",
            applied=0,
            viewed=1,
            hidden=0,
            category="relevant",
            position_link="https://example.com/visible",
        )
        html = _render_company_dashboard_html("Acme", [hidden_item, visible_item])

        self.assertIn(">Hidden (1)<", html)
        self.assertIn(">Relevant (1)<", html)

        hidden_panel_start = html.index('id="panel-hidden"')
        hidden_panel_end = html.index("</section>", hidden_panel_start)
        hidden_panel = html[hidden_panel_start:hidden_panel_end]
        self.assertIn("Parked Role", hidden_panel)
        self.assertNotIn("Visible Role", hidden_panel)

        relevant_panel_start = html.index('id="panel-relevant"')
        relevant_panel_end = html.index("</section>", relevant_panel_start)
        relevant_panel = html[relevant_panel_start:relevant_panel_end]
        self.assertIn("Visible Role", relevant_panel)
        self.assertNotIn("Parked Role", relevant_panel)

    def test_company_dashboard_soft_hidden_applied_stays_on_applied(self):
        soft_hidden = _applied_card_item(
            id=30,
            title="Soft Hidden Applied",
            applied=1,
            viewed=1,
            hidden=1,
            on_interview=0,
            interview_stopped=0,
            category="relevant",
            position_link="https://example.com/soft-applied",
        )
        soft_interview = _applied_card_item(
            id=31,
            title="Soft Hidden Interview",
            applied=1,
            viewed=1,
            hidden=1,
            on_interview=1,
            interview_stopped=0,
            category="relevant",
            position_link="https://example.com/soft-interview",
        )
        soft_stopped = _applied_card_item(
            id=32,
            title="Soft Hidden Stopped",
            applied=1,
            viewed=1,
            hidden=1,
            on_interview=0,
            interview_stopped=1,
            category="relevant",
            position_link="https://example.com/soft-stopped",
        )
        html = _render_company_dashboard_html(
            "Acme", [soft_hidden, soft_interview, soft_stopped]
        )

        self.assertIn(">Hidden (0)<", html)
        self.assertIn(">Applied (1)<", html)
        self.assertIn(">Interview (1)<", html)
        self.assertIn(">Stopped (1)<", html)

        hidden_panel_start = html.index('id="panel-hidden"')
        hidden_panel_end = html.index("</section>", hidden_panel_start)
        hidden_panel = html[hidden_panel_start:hidden_panel_end]
        self.assertNotIn("Soft Hidden Applied", hidden_panel)
        self.assertNotIn("Soft Hidden Interview", hidden_panel)
        self.assertNotIn("Soft Hidden Stopped", hidden_panel)

        applied_panel_start = html.index('id="panel-applied"')
        applied_panel_end = html.index("</section>", applied_panel_start)
        self.assertIn(
            "Soft Hidden Applied", html[applied_panel_start:applied_panel_end]
        )

        interview_panel_start = html.index('id="panel-interview"')
        interview_panel_end = html.index("</section>", interview_panel_start)
        self.assertIn(
            "Soft Hidden Interview",
            html[interview_panel_start:interview_panel_end],
        )

        stopped_panel_start = html.index('id="panel-stopped"')
        stopped_panel_end = html.index("</section>", stopped_panel_start)
        self.assertIn(
            "Soft Hidden Stopped", html[stopped_panel_start:stopped_panel_end]
        )

    def test_main_dashboard_soft_hidden_applied_stays_on_applied(self):
        """Soft-hidden applied rows render under Applied, not Hidden (main report)."""
        from spejder.db import (
            ensure_db,
            get_applied_jobs,
            get_hidden_jobs,
            set_job_applied,
            set_job_hidden,
            upsert_job,
        )
        from spejder.db.connection import _connect
        from spejder.config import AppConfig
        from spejder.workflows.dashboard import build_dashboard_record

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "jobs.db")
            out_html = os.path.join(tmpdir, "dashboard.html")
            ensure_db(db_path)
            upsert_job(
                db_path,
                {
                    "source": "Test",
                    "company": "Acme",
                    "title": "Soft Hidden Main",
                    "position_link": "https://example.com/soft-main",
                    "raw_text": "raw",
                },
            )
            conn = _connect(db_path)
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM jobs WHERE position_link=?",
                    ("https://example.com/soft-main",),
                )
                job_id = int(cur.fetchone()[0])
            finally:
                conn.close()
            self.assertTrue(set_job_applied(db_path, job_id, True))
            self.assertTrue(set_job_hidden(db_path, job_id, True))

            profile = AppConfig()
            cache = {}
            applied_items = [
                build_dashboard_record(
                    db_path,
                    profile,
                    cache,
                    row,
                    default_category="relevant",
                    default_viewed=1,
                    default_applied=1,
                    translate_title=False,
                )
                for row in get_applied_jobs(db_path, limit=0)
            ]
            hidden_items = [
                build_dashboard_record(
                    db_path,
                    profile,
                    cache,
                    row,
                    default_category="relevant",
                    translate_title=False,
                )
                for row in get_hidden_jobs(db_path, limit=0)
            ]
            _render_html_dashboard(
                [],
                [],
                applied_items,
                out_html,
                "Test dashboard",
                hidden_items=hidden_items,
            )
            with open(out_html, encoding="utf-8") as f:
                html = f.read()

        applied_panel_start = html.index('id="panel-applied"')
        applied_panel_end = html.index("</section>", applied_panel_start)
        self.assertIn(
            "Soft Hidden Main", html[applied_panel_start:applied_panel_end]
        )
        hidden_panel_start = html.index('id="panel-hidden"')
        hidden_panel_end = html.index("</section>", hidden_panel_start)
        self.assertNotIn(
            "Soft Hidden Main", html[hidden_panel_start:hidden_panel_end]
        )

    def test_company_dashboard_partitions_edited_today(self):
        today_item = _applied_card_item(
            id=20,
            title="Seen Today",
            applied=0,
            viewed=1,
            hidden=0,
            category="relevant",
            position_link="https://example.com/today",
        )
        older_viewed = _applied_card_item(
            id=21,
            title="Seen Earlier",
            applied=0,
            viewed=1,
            hidden=0,
            category="relevant",
            position_link="https://example.com/earlier",
        )
        html = _render_company_dashboard_html(
            "Acme",
            [today_item, older_viewed],
            viewed_today_order={20: 0},
        )

        self.assertIn(">Edited today (1)<", html)
        self.assertIn(">Relevant (1)<", html)

        edited_panel_start = html.index('id="panel-edited-today"')
        edited_panel_end = html.index("</section>", edited_panel_start)
        edited_panel = html[edited_panel_start:edited_panel_end]
        self.assertIn("Seen Today", edited_panel)
        self.assertNotIn("Seen Earlier", edited_panel)

        relevant_panel_start = html.index('id="panel-relevant"')
        relevant_panel_end = html.index("</section>", relevant_panel_start)
        relevant_panel = html[relevant_panel_start:relevant_panel_end]
        self.assertIn("Seen Earlier", relevant_panel)
        self.assertNotIn("Seen Today", relevant_panel)

    def test_build_job_cards_shows_formatted_applied_date(self):
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        self.assertIn(
            f'class="{APPLIED_DATE_CLASS}" title="{APPLIED_DATE_TITLE}">Applied: 2024-05-10</span>',
            html,
        )
        self.assertIn(
            f'class="{RELEVANCE_SCORE_CLASS}" title="{RELEVANCE_SCORE_TITLE}">0.50</span>',
            html,
        )
        article_start = html.find("<article")
        article_end = html.find("</article>")
        card_html = html[article_start:article_end]
        self.assertIn(f'class="{RELEVANCE_SCORE_CLASS}"', card_html)
        self.assertIn(f'class="{APPLIED_DATE_CLASS}"', card_html)
        self.assertIn(f'title="{RELEVANCE_SCORE_TITLE}"', card_html)
        self.assertIn(f'title="{APPLIED_DATE_TITLE}"', card_html)

    def test_build_job_cards_applied_date_outside_feedback_row(self):
        # Applied date is a sibling above .feedback, not nested inside the checkbox row.
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        applied_idx = html.find(f'class="{APPLIED_DATE_CLASS}"')
        feedback_idx = html.find('<div class="feedback">')
        self.assertNotEqual(applied_idx, -1)
        self.assertNotEqual(feedback_idx, -1)
        self.assertLess(applied_idx, feedback_idx)

    def test_build_job_cards_shows_applied_date_for_applied_stage_panels(self):
        expected = (
            f'class="{APPLIED_DATE_CLASS}" title="{APPLIED_DATE_TITLE}">Applied: 2024-05-10</span>'
        )
        for panel in ("applied", "interview", "stopped"):
            with self.subTest(panel=panel):
                html = _build_job_cards([_applied_card_item()], card_panel=panel)
                self.assertIn(expected, html)

    def test_build_job_cards_adds_has_applied_date_class_when_date_present(self):
        html = _build_job_cards([_applied_card_item()], card_panel="applied")
        article_start = html.find("<article")
        article_end = html.find(">", article_start)
        article_tag = html[article_start:article_end + 1]
        self.assertIn("has-applied-date", article_tag)

    def test_build_job_cards_omits_has_applied_date_class_when_date_absent(self):
        html = _build_job_cards(
            [_applied_card_item(applied_at="")],
            card_panel="applied",
        )
        article_start = html.find("<article")
        article_end = html.find(">", article_start)
        article_tag = html[article_start:article_end + 1]
        self.assertNotIn("has-applied-date", article_tag)

    def test_build_job_cards_omits_applied_line_when_empty(self):
        html = _build_job_cards(
            [_applied_card_item(applied_at="")],
            card_panel="applied",
        )
        self.assertNotIn("Applied:", html)

    def test_build_job_cards_saved_cover_letter_shows_viewer(self):
        html = _build_job_cards(
            [_applied_card_item(cover_letter="Dear hiring manager,\nThanks.")],
            card_panel="applied",
        )
        self.assertIn('class="cover-letter-viewer"', html)
        self.assertIn('class="cover-letter-saved-text"', html)
        self.assertIn("Dear hiring manager,\nThanks.", html)
        self.assertIn('type="checkbox" checked disabled', html)
        self.assertNotIn("cover-letter-input", html)
        self.assertNotIn("saveCoverLetter", html)

    def test_build_job_cards_empty_cover_letter_no_viewer(self):
        html = _build_job_cards(
            [_applied_card_item(cover_letter="", cover_letter_requested=0)],
            card_panel="applied",
        )
        self.assertNotIn("cover-letter-viewer", html)
        self.assertNotIn("cover-letter-saved-text", html)
        self.assertIn("setCoverLetterRequested", html)
        self.assertNotIn("cover-letter-input", html)

    def test_build_job_cards_requested_cover_letter_paste_ui_no_viewer(self):
        html = _build_job_cards(
            [_applied_card_item(cover_letter="", cover_letter_requested=1)],
            card_panel="applied",
        )
        self.assertNotIn("cover-letter-viewer", html)
        self.assertIn("cover-letter-input", html)
        self.assertIn("saveCoverLetter", html)
        self.assertIn('class="cover-letter-text"', html)

    def test_build_job_cards_saved_cover_letter_escapes_html(self):
        html = _build_job_cards(
            [_applied_card_item(cover_letter='<script>alert(1)</script> A & B')],
            card_panel="applied",
        )
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt; A &amp; B", html)
        self.assertNotIn("<script>alert(1)</script>", html)
        self.assertNotIn(" A & B", html)

    def test_build_job_cards_saved_cover_letter_viewer_on_interview_stopped(self):
        for panel in ("interview", "stopped"):
            with self.subTest(panel=panel):
                html = _build_job_cards(
                    [_applied_card_item(cover_letter="Saved letter body")],
                    card_panel=panel,
                )
                self.assertIn('class="cover-letter-viewer"', html)
                self.assertIn("Saved letter body", html)


if __name__ == "__main__":
    unittest.main()
