"""Tests for job ingest progress aggregation and quality gate."""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.jobs.ingestion import ingest_docs_to_db
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.workflows.progress_eta import (
    ingest_eta_store_path,
    load_slow_samples,
    save_slow_samples,
)


class IngestDocsProgressTest(unittest.TestCase):
    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_on_progress_is_cumulative_across_docs(self, mock_extract, _mock_upsert):
        mock_extract.side_effect = [
            [{"position_link": "https://www.linkedin.com/jobs/view/1", "title": "Dev"}],
            [{"position_link": "https://www.linkedin.com/jobs/view/2", "title": "Eng"}],
        ]
        recorded: list[tuple[int, int, int]] = []

        def on_progress(processed: int, inserted_new: int, skipped_existing: int) -> None:
            recorded.append((processed, inserted_new, skipped_existing))

        stats = ingest_docs_to_db(
            "/tmp/unused.db",
            [{"path": "a.eml"}, {"path": "b.eml"}],
            on_progress=on_progress,
        )

        self.assertTrue(recorded)
        self.assertEqual(
            recorded[-1],
            (stats["processed"], stats["inserted_new"], stats["skipped_existing"]),
        )
        self.assertEqual(recorded[-1], (2, 2, 0))
        self.assertEqual(stats["positions_by_file"][0]["status"], "ok")

    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_eta_sidecar_and_status_per_position(self, mock_extract, _mock_upsert):
        mock_extract.side_effect = [
            [{"position_link": "https://www.linkedin.com/jobs/view/1", "title": "Dev"}],
            [{"position_link": "https://www.linkedin.com/jobs/view/2", "title": "Eng"}],
            [{"position_link": "https://www.linkedin.com/jobs/view/3", "title": "Ops"}],
        ]
        recorded: list[tuple] = []
        status_msgs: list[str] = []

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = ingest_eta_store_path(db_path)
            save_slow_samples(store, [10.0, 10.0, 10.0], kind="position")

            def on_progress(
                processed: int,
                inserted_new: int,
                skipped_existing: int,
                eta_s=None,
            ) -> None:
                recorded.append((processed, inserted_new, skipped_existing, eta_s))

            with patch("spejder.jobs.ingestion.time.monotonic") as mock_mono:
                # One start/end pair per upsert (3 positions).
                mock_mono.side_effect = [0.0, 10.0, 10.0, 20.0, 20.0, 30.0]
                stats = ingest_docs_to_db(
                    db_path,
                    [{"path": "a.eml"}, {"path": "b.eml"}, {"path": "c.eml"}],
                    on_progress=on_progress,
                    on_status_message=status_msgs.append,
                    eta_store_path=store,
                )

            self.assertEqual(stats["processed"], 3)
            self.assertEqual(mock_extract.call_count, 3)
            self.assertTrue(status_msgs)
            # Initial status before position total is known: base only, no ETA.
            self.assertEqual(status_msgs[0], "Ingesting 3 inbox file(s)")
            self.assertNotIn("Estimated time left:", status_msgs[0])
            # After extract: historical rate × remaining positions.
            self.assertTrue(status_msgs[1].startswith("Ingesting 3 inbox file(s)"))
            self.assertIn("Estimated time left:", status_msgs[1])
            self.assertNotIn("%", status_msgs[1])
            with_eta = [row for row in recorded if row[3] is not None]
            self.assertTrue(with_eta)
            self.assertIsNone(recorded[-1][3])
            loaded = load_slow_samples(store, kind="position")
            self.assertEqual(len(loaded), 6)  # 3 historical + 3 new
            with open(store, encoding="utf-8") as handle:
                data = json.load(handle)
            self.assertEqual(data["kind"], "position")
            # Default slow loader must not see ingest samples.
            self.assertEqual(load_slow_samples(store), [])
            # Legacy kind=file sidecar must not load as position samples.
            save_slow_samples(store, [1.0, 2.0, 3.0], kind="file")
            self.assertEqual(load_slow_samples(store, kind="position"), [])

    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_eta_remaining_tracks_positions_not_files(
        self, mock_extract, _mock_upsert
    ):
        """Uneven position counts: remaining must follow upserts, not file boundaries."""
        mock_extract.side_effect = [
            [
                {
                    "position_link": f"https://www.linkedin.com/jobs/view/{i}",
                    "title": f"Job {i}",
                }
                for i in (1, 2, 3)
            ],
            [
                {
                    "position_link": "https://www.linkedin.com/jobs/view/4",
                    "title": "Job 4",
                }
            ],
        ]
        recorded: list[tuple] = []

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = ingest_eta_store_path(db_path)
            save_slow_samples(store, [10.0, 10.0, 10.0], kind="position")

            def on_progress(
                processed: int,
                inserted_new: int,
                skipped_existing: int,
                eta_s=None,
            ) -> None:
                recorded.append((processed, inserted_new, skipped_existing, eta_s))

            with patch("spejder.jobs.ingestion.time.monotonic") as mock_mono:
                mock_mono.side_effect = [
                    0.0,
                    10.0,
                    10.0,
                    20.0,
                    20.0,
                    30.0,
                    30.0,
                    40.0,
                ]
                ingest_docs_to_db(
                    db_path,
                    [{"path": "many.eml"}, {"path": "one.eml"}],
                    on_progress=on_progress,
                    eta_store_path=store,
                )

        # 4 positions total; historical 10s/pos → eta after each upsert.
        self.assertEqual(mock_extract.call_count, 2)
        self.assertEqual(len(recorded), 4)
        self.assertAlmostEqual(recorded[0][3] or 0.0, 30.0)  # 3 left
        self.assertAlmostEqual(recorded[1][3] or 0.0, 20.0)  # 2 left
        self.assertAlmostEqual(recorded[2][3] or 0.0, 10.0)  # 1 left
        self.assertIsNone(recorded[3][3])
        # File-based remaining would jump from ~20→10 after finishing many.eml
        # (first 3 upserts); position-based keeps stepping by one each time.
        self.assertNotEqual(recorded[0][3], recorded[2][3])

    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_legacy_three_arg_on_progress_still_works(
        self, mock_extract, _mock_upsert
    ):
        mock_extract.return_value = [
            {"position_link": "https://www.linkedin.com/jobs/view/1", "title": "Dev"}
        ]
        recorded: list[tuple[int, int, int]] = []

        def on_progress(processed: int, inserted_new: int, skipped_existing: int) -> None:
            recorded.append((processed, inserted_new, skipped_existing))

        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            store = ingest_eta_store_path(db_path)
            save_slow_samples(store, [5.0, 5.0, 5.0], kind="position")
            ingest_docs_to_db(
                db_path,
                [{"path": "a.eml"}],
                on_progress=on_progress,
                eta_store_path=store,
            )
        self.assertTrue(recorded)
        self.assertEqual(recorded[-1], (1, 1, 0))


class IngestQualityGateTest(unittest.TestCase):
    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_weak_entries_are_not_upserted(self, mock_extract, mock_upsert):
        mock_extract.return_value = [
            {
                "position_link": "https://www.linkedin.com/jobs/view/1",
                "title": "Apply here",
                "company": "Acme",
            }
        ]
        stats = ingest_docs_to_db("/tmp/unused.db", [{"path": "weak.eml"}])
        mock_upsert.assert_not_called()
        self.assertEqual(stats["processed"], 0)
        row = stats["positions_by_file"][0]
        self.assertEqual(row["found"], 0)
        self.assertEqual(row["weak_dropped"], 1)
        self.assertEqual(row["status"], "weak")
        self.assertIn("cta_title", row["quality"])

    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_all_weak_triggers_synth_when_enabled(self, mock_extract, mock_upsert):
        artifact = CareerAlertArtifact.model_validate(
            {
                "id": "synth_example_1",
                "match": {
                    "host_substrings": ["jobs.example.com"],
                    "path_includes": ["/job/"],
                },
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Example",
                    "source": "Example",
                },
                "source": "llm_synth",
            }
        )
        mock_extract.side_effect = [
            [
                {
                    "position_link": "https://jobs.example.com/job/1",
                    "title": "Apply now",
                    "company": "Example",
                }
            ],
            [
                {
                    "position_link": "https://jobs.example.com/job/1",
                    "title": "Backend Engineer",
                    "company": "Example",
                }
            ],
        ]
        profile = AppConfig(
            career_alert_artifacts_dir="/tmp/overlay",
            career_alert_synth_enabled=True,
            default_model="/models/test.gguf",
        )
        with patch(
            "spejder.jobs.ingestion.try_synthesize_artifact",
            return_value=(artifact, "ok"),
        ) as synth_mock:
            with patch(
                "spejder.jobs.ingestion.load_artifacts",
                return_value=[artifact],
            ):
                stats = ingest_docs_to_db(
                    "/tmp/unused.db",
                    [{"path": "weak.eml", "html": "<a>x</a>"}],
                    llm=MagicMock(),
                    runtime_profile=profile,
                )
        synth_mock.assert_called_once()
        mock_upsert.assert_called_once()
        # Overlay write succeeded → intentional second extract of the same doc.
        self.assertEqual(mock_extract.call_count, 2)
        self.assertEqual(stats["processed"], 1)
        self.assertEqual(stats["positions_by_file"][0]["status"], "synth_ok")
        self.assertEqual(stats["positions_by_file"][0]["synth_reason"], "ok")


if __name__ == "__main__":
    unittest.main()
