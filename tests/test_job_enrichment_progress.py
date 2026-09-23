"""Regression: on_progress still ticks at final idx when rows skip early."""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from spejder.workflows.job_descriptions import _generate_missing_descriptions_for_ingest
from spejder.workflows.job_skills_materialize import materialize_jobs_skills


class MaterializeJobsSkillsProgressTest(unittest.TestCase):
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills", return_value=["Python"])
    def test_on_progress_fires_at_total_when_skip_cached(
        self, _mock_get_skills, mock_materialize
    ):
        rows = [{"id": i} for i in range(1, 4)]
        recorded: list[tuple[int, int, int]] = []

        updated = materialize_jobs_skills(
            "unused.db",
            rows,
            skip_cached=True,
            on_progress=lambda checked, total, upd: recorded.append(
                (checked, total, upd)
            ),
        )

        self.assertEqual(updated, 0)
        mock_materialize.assert_not_called()
        self.assertEqual(recorded, [(3, 3, 0)])

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills", return_value=["Python"])
    def test_skip_cached_does_not_print_progress_label(
        self, _mock_get_skills, mock_materialize
    ):
        rows = [{"id": i} for i in range(1, 4)]
        buf = io.StringIO()
        with redirect_stdout(buf):
            materialize_jobs_skills(
                "unused.db",
                rows,
                skip_cached=True,
                progress_label="Background sync: skills",
            )
        mock_materialize.assert_not_called()
        self.assertEqual(buf.getvalue(), "")


class GenerateMissingDescriptionsProgressTest(unittest.TestCase):
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_refresh")
    def test_on_progress_fires_at_total_when_rows_continue_early(
        self, mock_refresh, _mock_enrich
    ):
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        recorded: list[tuple[int, int, int]] = []

        updated, skipped = _generate_missing_descriptions_for_ingest(
            "unused.db",
            on_progress=lambda checked, total, upd: recorded.append(
                (checked, total, upd)
            ),
        )

        self.assertEqual(updated, 0)
        self.assertEqual(skipped, 3)
        self.assertEqual(recorded, [(3, 3, 0)])


if __name__ == "__main__":
    unittest.main()
