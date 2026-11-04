"""Tests for dashboard rebuild queue behavior."""

import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import ensure_db, upsert_job
from spejder.db.connection import _connect
from spejder.workflows.dashboard import (
    DashboardRebuildQueue,
    coalesce_rebuild_reasons,
)
from spejder.workflows.report_workflow import (
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
)


def _insert_job(db_path: str, link: str, *, title: str) -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _set_triage_row(
    db_path: str,
    job_id: int,
    *,
    score: float,
    hidden: int = 0,
    category: str = "relevant",
    viewed: int = 0,
    updated_at: str = "2024-01-01T00:00:00+00:00",
) -> None:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE jobs
            SET relevance_score=?, hidden=?, category=?, viewed=?, updated_at=?
            WHERE id=?
            """,
            (score, hidden, category, viewed, updated_at, job_id),
        )
        conn.commit()
    finally:
        conn.close()


class DashboardRebuildQueueTest(unittest.TestCase):
    def test_coalesce_rebuild_reasons_uses_latest_with_queue_count(self):
        self.assertEqual(
            coalesce_rebuild_reasons(["first", "second", "third"]),
            "third (+2 queued)",
        )

    def test_coalesce_rebuild_reasons_empty_returns_blank(self):
        self.assertEqual(coalesce_rebuild_reasons([]), "")

    @patch("spejder.workflows.dashboard._render_html_dashboard")
    @patch(
        "spejder.workflows.dashboard._build_skills_tab_items",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_viewed_jobs_count",
        return_value=0,
    )
    @patch(
        "spejder.workflows.dashboard.build_viewed_today_dashboard_records",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_stopped_interview_jobs",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_interview_jobs",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_applied_jobs",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_jobs_by_category_triage_window",
        return_value=[],
    )
    @patch(
        "spejder.workflows.dashboard.get_jobs_count_by_category",
        return_value=0,
    )
    def test_rebuild_counts_pass_exclude_hidden_false(
        self,
        mock_count,
        mock_triage_window,
        _mock_applied,
        _mock_interview,
        _mock_stopped,
        _mock_viewed_today,
        _mock_viewed_count,
        _mock_skills,
        _mock_render,
    ):
        profile = AppConfig()
        queue = DashboardRebuildQueue(
            "/tmp/unused.db",
            "/tmp/unused-dashboard.html",
            profile,
        )
        queue.rebuild(reason="")

        self.assertEqual(mock_count.call_count, 2)
        cats = []
        for call in mock_count.call_args_list:
            args, kwargs = call
            cats.append(args[1])
            self.assertEqual(kwargs.get("exclude_hidden"), False)
            self.assertEqual(kwargs.get("unviewed_only"), True)
        self.assertEqual(cats, ["relevant", "not relevant"])

        expected_limits = {
            "relevant": _report_max_relevant_positions(profile),
            "not relevant": _report_max_not_relevant_positions(profile),
        }
        self.assertEqual(expected_limits["relevant"], 7)
        self.assertEqual(expected_limits["not relevant"], 42)
        self.assertEqual(mock_triage_window.call_count, 2)
        for call in mock_triage_window.call_args_list:
            args, kwargs = call
            cat = args[1]
            self.assertEqual(kwargs.get("unviewed_only"), True)
            self.assertEqual(kwargs.get("non_hidden_limit"), expected_limits[cat])

    def test_rebuild_html_triage_window_seam(self):
        """DB → triage window → render: L non-hidden + soft-in-walk; soft-beyond absent; N full."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "jobs.db")
            dashboard_path = os.path.join(tmpdir, "dashboard.html")
            ensure_db(db_path)

            a = _insert_job(db_path, "https://example.com/a", title="NH-A")
            b = _insert_job(db_path, "https://example.com/b", title="Soft-In")
            c = _insert_job(db_path, "https://example.com/c", title="NH-C")
            d = _insert_job(db_path, "https://example.com/d", title="Soft-Beyond")
            e = _insert_job(db_path, "https://example.com/e", title="NH-E")
            _set_triage_row(db_path, a, score=100.0, hidden=0)
            _set_triage_row(db_path, b, score=90.0, hidden=1)
            _set_triage_row(db_path, c, score=80.0, hidden=0)
            _set_triage_row(db_path, d, score=70.0, hidden=1)
            _set_triage_row(db_path, e, score=60.0, hidden=0)

            profile = AppConfig(report_max_relevant_positions=2)

            def _passthrough_title_fields(db_path, row, **_kwargs):
                title = str(row.get("title", "") or "")
                return {
                    "title": title,
                    "title_english": "",
                    "place": str(row.get("place", "") or ""),
                }

            with (
                patch(
                    "spejder.workflows.dashboard._build_skills_tab_items",
                    return_value=[],
                ),
                patch(
                    "spejder.workflows.dashboard_records._build_title_fields",
                    side_effect=_passthrough_title_fields,
                ),
            ):
                queue = DashboardRebuildQueue(db_path, dashboard_path, profile)
                queue.rebuild(reason="")

            with open(dashboard_path, encoding="utf-8") as f:
                html = f.read()

        relevant_start = html.index('id="panel-relevant"')
        relevant_end = html.index("</section>", relevant_start)
        relevant_panel = html[relevant_start:relevant_end]
        self.assertIn("NH-A", relevant_panel)
        self.assertIn("Soft-In", relevant_panel)
        self.assertIn("NH-C", relevant_panel)
        self.assertNotIn("Soft-Beyond", relevant_panel)
        self.assertNotIn("NH-E", relevant_panel)
        self.assertIn('id="btn-relevant"', html)
        self.assertIn('data-total="5"', html)
        self.assertIn(
            'Relevant (2 + <span class="mode-btn-soft-k">1</span>/5)',
            html,
        )

    def test_rebuild_html_triage_window_equal_score_updated_at_seam(self):
        """Equal scores: soft-in-walk membership depends on updated_at through rebuild."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "jobs.db")
            dashboard_path = os.path.join(tmpdir, "dashboard.html")
            ensure_db(db_path)

            # Insert so id ASC disagrees with updated_at DESC (catches dropped updated_at).
            # ids: NH-Early < NH-Mid < Soft-In < Soft-Beyond < NH-Late
            nh_early = _insert_job(
                db_path, "https://example.com/eq-a", title="NH-Early"
            )
            nh_mid = _insert_job(db_path, "https://example.com/eq-c", title="NH-Mid")
            soft_in = _insert_job(
                db_path, "https://example.com/eq-soft-in", title="Soft-In-Tie"
            )
            soft_beyond = _insert_job(
                db_path, "https://example.com/eq-soft-beyond", title="Soft-Beyond-Tie"
            )
            nh_late = _insert_job(db_path, "https://example.com/eq-e", title="NH-Late")
            # Walk by updated_at DESC: NH-Early, Soft-In, NH-Mid, Soft-Beyond, NH-Late.
            # L=2 → Soft-In in window; Soft-Beyond after L-th NH.
            # Without updated_at, id ASC yields NH-Early, NH-Mid, Soft-In… → Soft-In dropped.
            _set_triage_row(
                db_path,
                nh_early,
                score=50.0,
                hidden=0,
                updated_at="2024-05-05T00:00:00+00:00",
            )
            _set_triage_row(
                db_path,
                soft_in,
                score=50.0,
                hidden=1,
                updated_at="2024-05-04T00:00:00+00:00",
            )
            _set_triage_row(
                db_path,
                nh_mid,
                score=50.0,
                hidden=0,
                updated_at="2024-05-03T00:00:00+00:00",
            )
            _set_triage_row(
                db_path,
                soft_beyond,
                score=50.0,
                hidden=1,
                updated_at="2024-05-02T00:00:00+00:00",
            )
            _set_triage_row(
                db_path,
                nh_late,
                score=50.0,
                hidden=0,
                updated_at="2024-05-01T00:00:00+00:00",
            )

            profile = AppConfig(report_max_relevant_positions=2)

            def _passthrough_title_fields(db_path, row, **_kwargs):
                title = str(row.get("title", "") or "")
                return {
                    "title": title,
                    "title_english": "",
                    "place": str(row.get("place", "") or ""),
                }

            with (
                patch(
                    "spejder.workflows.dashboard._build_skills_tab_items",
                    return_value=[],
                ),
                patch(
                    "spejder.workflows.dashboard_records._build_title_fields",
                    side_effect=_passthrough_title_fields,
                ),
            ):
                queue = DashboardRebuildQueue(db_path, dashboard_path, profile)
                queue.rebuild(reason="")

            with open(dashboard_path, encoding="utf-8") as f:
                html = f.read()

        relevant_start = html.index('id="panel-relevant"')
        relevant_end = html.index("</section>", relevant_start)
        relevant_panel = html[relevant_start:relevant_end]
        self.assertIn("NH-Early", relevant_panel)
        self.assertIn("Soft-In-Tie", relevant_panel)
        self.assertIn("NH-Mid", relevant_panel)
        self.assertNotIn("Soft-Beyond-Tie", relevant_panel)
        self.assertNotIn("NH-Late", relevant_panel)


if __name__ == "__main__":
    unittest.main()
