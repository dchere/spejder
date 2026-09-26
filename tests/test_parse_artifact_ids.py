"""P5: matched artifact ids on extract / ingest outcomes."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.jobs.ingestion import ingest_docs_to_db
from spejder.jobs.parsing.artifact_interpreter import interpret_artifacts
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.core import extract_job_entries
from spejder.workflows.ingest_utils import (
    log_ingest_parse_outcomes,
    quarantine_unparsed_inbox_files,
)
from spejder.workflows.sync_log import SyncRunLog


def _artifact(artifact_id: str, *, priority: int = 50) -> CareerAlertArtifact:
    return CareerAlertArtifact.model_validate(
        {
            "id": artifact_id,
            "priority": priority,
            "match": {
                "host_substrings": ["jobs.ids.test"],
                "path_includes": ["/job/"],
            },
            "fields": {
                "from_anchor": "jobs2web_middot_or_dash",
                "company": "Ids Co",
                "source": "Ids Co",
            },
        }
    )


class MatchedArtifactIdsTest(unittest.TestCase):
    def test_interpret_records_winning_ids(self):
        html = (
            '<a href="http://jobs.ids.test/job/Role/1">'
            "Role One - City</a>"
        )
        low = _artifact("low_ids", priority=1)
        high = _artifact("high_ids", priority=100)
        matched: list[str] = []
        by_link = interpret_artifacts(html, [low, high], matched_ids=matched)
        self.assertEqual(len(by_link), 1)
        self.assertEqual(matched, ["high_ids"])

    def test_extract_meta_out_lists_artifact_ids(self):
        html = (
            '<a href="http://jobs.ids.test/job/Role/1">'
            "Role One - City</a>"
        )
        meta: dict = {}
        entries = extract_job_entries(
            {"html": html, "text": "", "title": "", "links": []},
            artifacts=[_artifact("shipped_ids")],
            meta_out=meta,
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(meta.get("artifact_ids"), ["shipped_ids"])

    def test_ingest_positions_by_file_includes_artifact_ids(self):
        html = (
            '<a href="http://jobs.ids.test/job/Role/1">'
            "Role One - City</a>"
        )
        doc = {
            "html": html,
            "text": "",
            "title": "",
            "links": ["http://jobs.ids.test/job/Role/1"],
            "path": "ids.eml",
        }
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            ensure_db(db_path)
            overlay = os.path.join(tmp, "overlay")
            os.makedirs(overlay)
            profile = AppConfig(
                career_alert_artifacts_dir=overlay,
                career_alert_synth_enabled=False,
            )
            art = _artifact("overlay_ids")
            with patch(
                "spejder.jobs.ingestion._load_run_artifacts",
                return_value=[art],
            ):
                stats = ingest_docs_to_db(
                    db_path,
                    [doc],
                    runtime_profile=profile,
                )
            row = stats["positions_by_file"][0]
            self.assertEqual(row["artifact_ids"], ["overlay_ids"])
            self.assertGreaterEqual(int(row["found"]), 1)

    def test_quarantine_sidecar_and_sync_log_include_ids(self):
        with tempfile.TemporaryDirectory() as root:
            inbox = os.path.join(root, "inbox")
            quarantine = os.path.join(root, "outbox", "parse_quarantine")
            os.makedirs(inbox)
            target = os.path.join(inbox, "bad.eml")
            with open(target, "w", encoding="utf-8") as handle:
                handle.write("x")
            stats = {
                "positions_by_file": [
                    {
                        "file": target,
                        "found": 0,
                        "status": "weak",
                        "weak_dropped": 1,
                        "quality": "cta_title",
                        "synth_reason": "",
                        "artifact_ids": ["stale_cta", "jobs2web_danfoss"],
                    }
                ]
            }
            result = quarantine_unparsed_inbox_files(
                stats, inbox_root=inbox, quarantine_dir=quarantine
            )
            self.assertEqual(result["moved"], 1)
            with open(
                os.path.join(quarantine, "bad.eml.json"), encoding="utf-8"
            ) as handle:
                sidecar = json.load(handle)
            self.assertEqual(
                sidecar["artifact_ids"], ["stale_cta", "jobs2web_danfoss"]
            )

            log_path = os.path.join(root, "sync.log")
            sync_log = SyncRunLog.open(log_path, echo_stdout=False)
            sync_log.run_start(source="test")
            written = log_ingest_parse_outcomes(sync_log, stats)
            sync_log.run_end(status="complete")
            self.assertEqual(written, 1)
            with open(log_path, encoding="utf-8") as handle:
                body = handle.read()
            self.assertIn("artifact_ids=stale_cta,jobs2web_danfoss", body)


if __name__ == "__main__":
    unittest.main()
