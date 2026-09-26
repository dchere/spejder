"""Tests for job ingest progress aggregation and quality gate."""

import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.jobs.ingestion import ingest_docs_to_db
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact


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
        self.assertEqual(stats["processed"], 1)
        self.assertEqual(stats["positions_by_file"][0]["status"], "synth_ok")
        self.assertEqual(stats["positions_by_file"][0]["synth_reason"], "ok")


if __name__ == "__main__":
    unittest.main()
