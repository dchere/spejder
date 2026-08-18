"""Tests for job ingest progress aggregation."""

import unittest
from unittest.mock import patch

from spejder.jobs.ingestion import ingest_docs_to_db


class IngestDocsProgressTest(unittest.TestCase):
    @patch("spejder.jobs.ingestion.upsert_job", return_value=True)
    @patch("spejder.jobs.ingestion._extract_for_doc")
    def test_on_progress_is_cumulative_across_docs(self, mock_extract, _mock_upsert):
        mock_extract.side_effect = [
            [{"position_link": "https://www.linkedin.com/jobs/view/1"}],
            [{"position_link": "https://www.linkedin.com/jobs/view/2"}],
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


if __name__ == "__main__":
    unittest.main()
