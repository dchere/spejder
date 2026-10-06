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
    SKILLS_STAGE_MESSAGE,
    RollingTimeAverage,
    descriptions_eta_store_path,
    estimate_remaining_seconds,
    format_descriptions_stage_message,
    format_duration,
    format_eta_minutes_left,
    format_skills_stage_message,
    load_slow_samples,
    save_slow_samples,
    skills_eta_store_path,
)

_IN_SCOPE_FLAGS = {
    "viewed": 0,
    "applied": 0,
    "on_interview": 0,
    "interview_stopped": 0,
}


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
            save_slow_samples(path, [1.5, 2.5, 3.5])
            loaded = load_slow_samples(path)
            self.assertEqual(loaded, [1.5, 2.5, 3.5])
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["version"], 2)
            self.assertEqual(data["kind"], "slow")

    def test_save_keeps_last_32_samples(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "jobs.db.skills_eta.json")
            save_slow_samples(path, [float(i) for i in range(40)])
            loaded = load_slow_samples(path)
            self.assertEqual(len(loaded), 32)
            self.assertEqual(loaded[0], 8.0)
            self.assertEqual(loaded[-1], 39.0)

    def test_load_corrupt_missing_or_old_aggregate_returns_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            missing = os.path.join(tmp, "nope.json")
            self.assertEqual(load_slow_samples(missing), [])
            bad = os.path.join(tmp, "bad.json")
            with open(bad, "w", encoding="utf-8") as handle:
                handle.write("{not-json")
            self.assertEqual(load_slow_samples(bad), [])
            old = os.path.join(tmp, "old.json")
            with open(old, "w", encoding="utf-8") as handle:
                json.dump({"total_seconds": 5866, "count": 1793}, handle)
            self.assertEqual(load_slow_samples(old), [])

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

    def test_format_eta_minutes_left(self) -> None:
        self.assertEqual(format_eta_minutes_left(0), "less than a minute")
        self.assertEqual(format_eta_minutes_left(45), "less than a minute")
        self.assertEqual(format_eta_minutes_left(59.4), "less than a minute")
        self.assertEqual(format_eta_minutes_left(60), "1 minute")
        self.assertEqual(format_eta_minutes_left(89), "1 minute")
        self.assertEqual(format_eta_minutes_left(125), "2 minutes")
        self.assertEqual(format_eta_minutes_left(180), "3 minutes")

    def test_estimate_window_uses_last_eight(self) -> None:
        samples = [100.0, 50.0] + [2.0] * 8
        eta = estimate_remaining_seconds(
            remaining=4,
            samples=samples,
            historical=RollingTimeAverage(total_seconds=1000.0, count=10),
        )
        self.assertAlmostEqual(eta or 0.0, 8.0)

    def test_estimate_three_in_run_samples_beat_history(self) -> None:
        historical = RollingTimeAverage(total_seconds=100.0, count=10)  # 10s/pos
        eta = estimate_remaining_seconds(
            remaining=5,
            samples=[3.0, 3.0, 3.0],
            historical=historical,
        )
        self.assertAlmostEqual(eta or 0.0, 15.0)

    def test_estimate_one_sample_with_empty_history_is_none(self) -> None:
        self.assertIsNone(
            estimate_remaining_seconds(
                remaining=5,
                samples=[2.0],
                historical=RollingTimeAverage(),
            )
        )

    def test_estimate_one_sample_uses_historical_rate(self) -> None:
        historical = RollingTimeAverage(total_seconds=40.0, count=10)  # 4s/pos
        eta = estimate_remaining_seconds(
            remaining=5,
            samples=[2.0],
            historical=historical,
        )
        self.assertAlmostEqual(eta or 0.0, 20.0)

    def test_estimate_zero_remaining(self) -> None:
        self.assertIsNone(
            estimate_remaining_seconds(
                remaining=0,
                samples=[1.0, 2.0, 3.0],
                historical=RollingTimeAverage(total_seconds=40.0, count=10),
            )
        )

    def test_format_skills_stage_message(self) -> None:
        for eta_s in (None, 0):
            text = format_skills_stage_message(eta_s=eta_s)
            self.assertEqual(text, SKILLS_STAGE_MESSAGE)
            self.assertNotIn("%", text)
        msg = format_skills_stage_message(eta_s=180)
        self.assertEqual(
            msg,
            f"{SKILLS_STAGE_MESSAGE}. Estimated time left: 3 minutes.",
        )
        self.assertNotIn("%", msg)
        short_eta = format_skills_stage_message(eta_s=40)
        self.assertEqual(
            short_eta,
            f"{SKILLS_STAGE_MESSAGE}. Estimated time left: less than a minute.",
        )
        self.assertNotIn("%", short_eta)

    def test_format_descriptions_stage_message(self) -> None:
        for eta_s in (None, 0):
            text = format_descriptions_stage_message(eta_s=eta_s)
            self.assertEqual(text, DESCRIPTIONS_STAGE_MESSAGE)
            self.assertNotIn("%", text)
        msg = format_descriptions_stage_message(eta_s=180)
        self.assertEqual(
            msg,
            f"{DESCRIPTIONS_STAGE_MESSAGE}. Estimated time left: 3 minutes.",
        )
        self.assertNotIn("%", msg)
        short_eta = format_descriptions_stage_message(eta_s=40)
        self.assertIn("Estimated time left: less than a minute.", short_eta)
        self.assertNotIn("%", short_eta)


class MaterializeEtaIntegrationTest(unittest.TestCase):
    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_on_progress_receives_eta_and_persists_store(
        self, _mock_get_skills, mock_materialize, _mock_flags
    ) -> None:
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
            self.assertEqual([item[:3] for item in recorded], [(1, 3, 1), (2, 3, 2), (3, 3, 3)])
            self.assertIsNone(recorded[-1][3])
            self.assertTrue(status_msgs)
            self.assertEqual(status_msgs[0], SKILLS_STAGE_MESSAGE)
            self.assertEqual(status_msgs[-1], SKILLS_STAGE_MESSAGE)
            for message in status_msgs:
                self.assertNotIn("%", message)
            self.assertTrue(os.path.isfile(store))
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["version"], 2)
            self.assertEqual(data["kind"], "slow")
            self.assertEqual(len(data["samples"]), 3)
            self.assertNotIn("count", data)
            self.assertNotIn("total_seconds", data)

    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_old_aggregate_does_not_open_skills_eta(
        self, _mock_get_skills, mock_materialize, _mock_flags
    ) -> None:
        mock_materialize.return_value = ("Python", "raw", True)
        status_msgs: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = skills_eta_store_path(db_path)
            with open(store, "w", encoding="utf-8") as handle:
                json.dump({"total_seconds": 5866, "count": 1793}, handle)
            self.assertEqual(load_slow_samples(store), [])
            materialize_jobs_skills(
                db_path,
                [{"id": 1}],
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )
            self.assertEqual(status_msgs[0], SKILLS_STAGE_MESSAGE)
            self.assertNotIn("Estimated time left", status_msgs[0])
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["version"], 2)
            self.assertEqual(data["kind"], "slow")
            self.assertEqual(len(data["samples"]), 1)
            self.assertNotIn("count", data)

    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_versioned_samples_open_skills_eta(
        self, _mock_get_skills, mock_materialize, _mock_flags
    ) -> None:
        mock_materialize.return_value = ("Python", "raw", True)
        status_msgs: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = skills_eta_store_path(db_path)
            save_slow_samples(store, [60.0, 60.0, 60.0])
            materialize_jobs_skills(
                db_path,
                [{"id": 1}],
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )
            self.assertIn("Estimated time left:", status_msgs[0])
            self.assertNotIn("%", status_msgs[0])

    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs")
    def test_mixed_batch_counts_only_slow_rows(
        self, mock_get_skills, mock_materialize, _mock_flags
    ) -> None:
        def skills_for(_db_path: str, job_ids: list[int]) -> dict[int, list[str]]:
            return {int(job_id): (["Python"] if int(job_id) <= 2 else []) for job_id in job_ids}

        mock_get_skills.side_effect = skills_for

        def slow_materialize(*_a, **_k):
            time.sleep(0.01)
            return ("Python", "raw", True)

        mock_materialize.side_effect = slow_materialize
        rows = [{"id": i} for i in range(1, 7)]
        recorded: list[tuple] = []
        status_msgs: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = skills_eta_store_path(db_path)
            updated = materialize_jobs_skills(
                db_path,
                rows,
                skip_cached=True,
                on_progress=lambda *args: recorded.append(args),
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )

            self.assertEqual(updated, 4)
            self.assertEqual(mock_materialize.call_count, 4)
            self.assertEqual([item[0] for item in recorded], [1, 2, 3, 4])
            self.assertEqual([item[1] for item in recorded], [4, 4, 4, 4])
            called_ids = [call.args[1]["id"] for call in mock_materialize.call_args_list]
            self.assertEqual(called_ids, [3, 4, 5, 6])
            for call in mock_materialize.call_args_list:
                self.assertTrue(call.kwargs["first_materialize"])
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertIn(len(data["samples"]), (3, 4))
            self.assertTrue(all(sample >= 0.005 for sample in data["samples"]))
            # status[0] is the opening line; status[3] follows the 3rd slow job.
            self.assertIn("Estimated time left:", status_msgs[3])
            self.assertNotIn("%", status_msgs[3])
            self.assertEqual(status_msgs[-1], SKILLS_STAGE_MESSAGE)
            self.assertNotIn("%", status_msgs[-1])

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch(
        "spejder.db.get_job_skills_for_jobs",
        return_value={1: ["Python"], 2: ["Python"], 3: ["Python"]},
    )
    def test_legacy_three_arg_on_progress_not_called_when_all_cached(
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
        self.assertEqual(recorded, [])

    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_legacy_three_arg_on_progress_once_per_slow_job(
        self, _mock_get_skills, mock_materialize, _mock_flags
    ) -> None:
        mock_materialize.return_value = ("Python", "raw", True)
        recorded: list[tuple[int, int, int]] = []
        with tempfile.TemporaryDirectory() as tmp:
            materialize_jobs_skills(
                os.path.join(tmp, "jobs.db"),
                [{"id": i} for i in range(1, 4)],
                on_progress=lambda c, t, u: recorded.append((c, t, u)),
            )
        self.assertEqual(recorded, [(1, 3, 1), (2, 3, 2), (3, 3, 3)])


class DescriptionsEtaIntegrationTest(unittest.TestCase):
    @patch(
        "spejder.workflows.job_descriptions.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_triage")
    def test_on_progress_receives_eta_and_persists_store(
        self, mock_refresh, _mock_enrich, _mock_flags
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
            self.assertEqual(
                [item[:3] for item in recorded],
                [(1, 3, 0), (2, 3, 0), (3, 3, 0)],
            )
            self.assertIsNone(recorded[-1][3])
            self.assertTrue(status_msgs)
            for message in status_msgs:
                self.assertNotIn("%", message)
            self.assertEqual(status_msgs[-1], DESCRIPTIONS_STAGE_MESSAGE)
            self.assertTrue(os.path.isfile(store))
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["version"], 2)
            self.assertEqual(data["kind"], "slow")
            self.assertEqual(len(data["samples"]), 3)
            self.assertNotIn("count", data)
            self.assertNotIn("total_seconds", data)

    @patch(
        "spejder.workflows.job_descriptions.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_triage")
    def test_old_aggregate_does_not_open_descriptions_eta(
        self, mock_refresh, _mock_enrich, _mock_flags
    ) -> None:
        mock_refresh.return_value = [{"id": 1, "raw_text": ""}]
        status_msgs: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = descriptions_eta_store_path(db_path)
            with open(store, "w", encoding="utf-8") as handle:
                json.dump({"total_seconds": 2838, "count": 26}, handle)
            self.assertEqual(load_slow_samples(store), [])
            _generate_missing_descriptions_for_ingest(
                db_path,
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )
            self.assertEqual(status_msgs[0], DESCRIPTIONS_STAGE_MESSAGE)
            self.assertNotIn("Estimated time left", status_msgs[0])
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["version"], 2)
            self.assertEqual(data["kind"], "slow")
            self.assertNotIn("count", data)

    @patch(
        "spejder.workflows.job_descriptions.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_triage")
    def test_versioned_samples_open_descriptions_eta(
        self, mock_refresh, _mock_enrich, _mock_flags
    ) -> None:
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        status_msgs: list[str] = []
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = descriptions_eta_store_path(db_path)
            save_slow_samples(store, [60.0, 60.0, 60.0])
            _generate_missing_descriptions_for_ingest(
                db_path,
                on_status_message=status_msgs.append,
                eta_store_path=store,
            )
            self.assertIn("Estimated time left:", status_msgs[0])
            self.assertNotIn("%", status_msgs[0])

    @patch(
        "spejder.workflows.job_descriptions.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_triage")
    def test_legacy_three_arg_on_progress_still_works(
        self, mock_refresh, _mock_enrich, _mock_flags
    ) -> None:
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        recorded: list[tuple[int, int, int]] = []

        with tempfile.TemporaryDirectory() as tmp:
            _generate_missing_descriptions_for_ingest(
                os.path.join(tmp, "jobs.db"),
                on_progress=lambda c, t, u: recorded.append((c, t, u)),
            )
        self.assertEqual(recorded, [(1, 3, 0), (2, 3, 0), (3, 3, 0)])


if __name__ == "__main__":
    unittest.main()
