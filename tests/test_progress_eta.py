"""Tests for rolling average collapse and skills / descriptions ETA formatting."""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from unittest.mock import patch

from spejder.workflows.job_descriptions import _generate_missing_descriptions_for_ingest
from spejder.workflows.job_skills_materialize import materialize_jobs_skills
from spejder.workflows.progress_eta import (
    DESCRIPTIONS_STAGE_MESSAGE,
    RollingTimeAverage,
    descriptions_eta_store_path,
    estimate_remaining_seconds,
    format_descriptions_stage_message,
    format_duration,
    format_skills_stage_message,
    load_rolling_average,
    save_rolling_average,
    skills_eta_store_path,
)


class RollingTimeAverageTest(unittest.TestCase):
    def test_average_and_record(self) -> None:
        avg = RollingTimeAverage()
        self.assertIsNone(avg.average)
        avg.record(10.0, n=2)
        self.assertEqual(avg.count, 2)
        self.assertEqual(avg.total_seconds, 10.0)
        self.assertEqual(avg.average, 5.0)
        avg.record(5.0)
        self.assertEqual(avg.count, 3)
        self.assertAlmostEqual(avg.average or 0.0, 5.0)

    def test_collapse_when_count_exceeds_threshold(self) -> None:
        avg = RollingTimeAverage(collapse_above=10, collapse_to=4)
        # 11th sample trips collapse (count 11 > 10) → 4 * avg(2.0)
        for _ in range(11):
            avg.record(2.0)
        self.assertEqual(avg.count, 4)
        self.assertAlmostEqual(avg.total_seconds, 8.0)
        self.assertAlmostEqual(avg.average or 0.0, 2.0)

    def test_collapse_preserves_average_at_default_thresholds(self) -> None:
        avg = RollingTimeAverage()
        for _ in range(2001):
            avg.record(1.5)
        self.assertEqual(avg.count, 1000)
        self.assertAlmostEqual(avg.total_seconds, 1500.0)
        self.assertAlmostEqual(avg.average or 0.0, 1.5)

    def test_load_save_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "jobs.db.skills_eta.json")
            avg = RollingTimeAverage(total_seconds=30.0, count=10)
            save_rolling_average(path, avg)
            loaded = load_rolling_average(path)
            self.assertEqual(loaded.count, 10)
            self.assertAlmostEqual(loaded.total_seconds, 30.0)
            self.assertAlmostEqual(loaded.average or 0.0, 3.0)

    def test_load_corrupt_or_missing_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "nope.json")
            self.assertEqual(load_rolling_average(missing).count, 0)
            bad = os.path.join(tmp, "bad.json")
            with open(bad, "w", encoding="utf-8") as handle:
                handle.write("{not-json")
            self.assertEqual(load_rolling_average(bad).count, 0)

    def test_skills_eta_store_path(self) -> None:
        path = skills_eta_store_path("/tmp/jobs.db")
        self.assertTrue(path.endswith("jobs.db.skills_eta.json"))

    def test_descriptions_eta_store_path(self) -> None:
        path = descriptions_eta_store_path("/tmp/jobs.db")
        self.assertTrue(path.endswith("jobs.db.descriptions_eta.json"))


class FormatDurationAndEtaTest(unittest.TestCase):
    def test_format_duration(self) -> None:
        self.assertEqual(format_duration(0), "0s")
        self.assertEqual(format_duration(5.4), "5s")
        self.assertEqual(format_duration(65), "1m 5s")
        self.assertEqual(format_duration(3661), "1h 1m 1s")

    def test_estimate_prefers_run_average_after_min_samples(self) -> None:
        historical = RollingTimeAverage(total_seconds=100.0, count=10)  # 10s/pos
        eta = estimate_remaining_seconds(
            remaining=5,
            run_total_seconds=9.0,
            run_count=3,
            historical=historical,
        )
        # run avg = 3s → 15s remaining
        self.assertAlmostEqual(eta or 0.0, 15.0)

    def test_estimate_falls_back_to_historical(self) -> None:
        historical = RollingTimeAverage(total_seconds=40.0, count=10)  # 4s/pos
        eta = estimate_remaining_seconds(
            remaining=5,
            run_total_seconds=2.0,
            run_count=1,
            historical=historical,
        )
        self.assertAlmostEqual(eta or 0.0, 20.0)

    def test_estimate_zero_remaining(self) -> None:
        historical = RollingTimeAverage()
        self.assertEqual(
            estimate_remaining_seconds(
                remaining=0,
                run_total_seconds=10.0,
                run_count=5,
                historical=historical,
            ),
            0.0,
        )

    def test_format_skills_stage_message(self) -> None:
        self.assertEqual(
            format_skills_stage_message(checked=0, total=0, eta_s=None),
            "Materializing skills and rescoring jobs",
        )
        msg = format_skills_stage_message(checked=50, total=200, eta_s=125)
        self.assertIn("25%", msg)
        self.assertIn("~2m 5s left", msg)
        self.assertTrue(msg.startswith("Materializing skills and rescoring jobs — "))
        done = format_skills_stage_message(checked=10, total=10, eta_s=0)
        self.assertIn("100%", done)
        self.assertNotIn("left", done)

    def test_format_descriptions_stage_message(self) -> None:
        self.assertEqual(
            format_descriptions_stage_message(checked=0, total=0, eta_s=None),
            DESCRIPTIONS_STAGE_MESSAGE,
        )
        msg = format_descriptions_stage_message(checked=50, total=200, eta_s=125)
        self.assertTrue(msg.startswith(f"{DESCRIPTIONS_STAGE_MESSAGE} — "))
        self.assertIn("25%", msg)
        self.assertIn("~2m 5s left", msg)
        done = format_descriptions_stage_message(checked=10, total=10, eta_s=0)
        self.assertIn("100%", done)
        self.assertNotIn("left", done)


class MaterializeEtaIntegrationTest(unittest.TestCase):
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills", return_value=[])
    def test_on_progress_receives_eta_and_persists_store(
        self, _mock_get_skills, mock_materialize
    ) -> None:
        mock_materialize.return_value = ("Python", "raw", True)
        rows = [{"id": i} for i in range(1, 4)]
        recorded: list[tuple] = []
        status_msgs: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = skills_eta_store_path(db_path)

            def slow_materialize(*_a, **_k):
                time.sleep(0.01)
                return ("Python", "raw", True)

            mock_materialize.side_effect = slow_materialize

            updated = materialize_jobs_skills(
                db_path,
                rows,
                on_progress=lambda *args: recorded.append(args),
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )

            self.assertEqual(updated, 3)
            self.assertEqual(len(recorded), 1)
            self.assertEqual(recorded[0][:3], (3, 3, 3))
            self.assertEqual(recorded[0][3], 0.0)  # remaining=0
            self.assertTrue(status_msgs)
            self.assertIn("100%", status_msgs[-1])
            self.assertTrue(os.path.isfile(store))
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["count"], 3)
            self.assertGreater(data["total_seconds"], 0)

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills", return_value=["Python"])
    def test_legacy_three_arg_on_progress_still_works(
        self, _mock_get_skills, mock_materialize
    ) -> None:
        rows = [{"id": i} for i in range(1, 4)]
        recorded: list[tuple[int, int, int]] = []

        materialize_jobs_skills(
            "unused.db",
            rows,
            skip_cached=True,
            on_progress=lambda c, t, u: recorded.append((c, t, u)),
        )
        mock_materialize.assert_not_called()
        self.assertEqual(recorded, [(3, 3, 0)])


class DescriptionsEtaIntegrationTest(unittest.TestCase):
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_refresh")
    def test_on_progress_receives_eta_and_persists_store(
        self, mock_refresh, _mock_enrich
    ) -> None:
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        recorded: list[tuple] = []
        status_msgs: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = descriptions_eta_store_path(db_path)

            updated, skipped = _generate_missing_descriptions_for_ingest(
                db_path,
                on_progress=lambda *args: recorded.append(args),
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )

            self.assertEqual(updated, 0)
            self.assertEqual(skipped, 3)
            self.assertEqual(len(recorded), 1)
            self.assertEqual(recorded[0][:3], (3, 3, 0))
            self.assertEqual(recorded[0][3], 0.0)
            self.assertTrue(status_msgs)
            self.assertIn("100%", status_msgs[-1])
            self.assertTrue(status_msgs[-1].startswith(DESCRIPTIONS_STAGE_MESSAGE))
            self.assertTrue(os.path.isfile(store))
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["count"], 3)
            self.assertGreaterEqual(data["total_seconds"], 0)

    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_refresh")
    def test_legacy_three_arg_on_progress_still_works(
        self, mock_refresh, _mock_enrich
    ) -> None:
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        recorded: list[tuple[int, int, int]] = []

        _generate_missing_descriptions_for_ingest(
            "unused.db",
            on_progress=lambda c, t, u: recorded.append((c, t, u)),
        )
        self.assertEqual(recorded, [(3, 3, 0)])


if __name__ == "__main__":
    unittest.main()
