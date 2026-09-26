"""Tests for delete_processed_inbox_files and parse quarantine."""

import json
import os
import tempfile
import unittest

from spejder.workflows.ingest_utils import (
    delete_processed_inbox_files,
    log_ingest_parse_outcomes,
    quarantine_unparsed_inbox_files,
)
from spejder.workflows.sync_log import SyncRunLog


def _stats(file_path: str, found: int = 1, **extra) -> dict:
    row = {"file": file_path, "found": found}
    row.update(extra)
    return {"positions_by_file": [row]}


class DeleteProcessedInboxFilesTest(unittest.TestCase):
    def test_inside_root_deleted(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "job.json")
            open(target, "w").close()

            result = delete_processed_inbox_files(_stats(target), inbox_root=root)

        self.assertEqual(result["eligible"], 1)
        self.assertEqual(result["deleted"], 1)
        self.assertEqual(result["missing"], 0)
        self.assertEqual(result["failed"], 0)

    def test_outside_root_skipped(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as other:
            target = os.path.join(other, "job.json")
            open(target, "w").close()

            result = delete_processed_inbox_files(_stats(target), inbox_root=root)

        self.assertEqual(result["eligible"], 0)
        self.assertEqual(result["deleted"], 0)

    def test_missing_file_inside_root_counted(self):
        with tempfile.TemporaryDirectory() as root:
            absent = os.path.join(root, "nonexistent_job.json")
            result = delete_processed_inbox_files(_stats(absent), inbox_root=root)

        self.assertEqual(result["eligible"], 1)
        self.assertEqual(result["missing"], 1)
        self.assertEqual(result["deleted"], 0)
        self.assertEqual(result["failed"], 0)

    def test_non_file_path_fails(self):
        with tempfile.TemporaryDirectory() as root:
            subdir = os.path.join(root, "nested")
            os.mkdir(subdir)
            result = delete_processed_inbox_files(_stats(subdir), inbox_root=root)

        self.assertEqual(result["eligible"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["deleted"], 0)
        self.assertEqual(result["missing"], 0)


class QuarantineUnparsedInboxFilesTest(unittest.TestCase):
    def test_moves_weak_file_with_sidecar(self):
        with tempfile.TemporaryDirectory() as root:
            inbox = os.path.join(root, "inbox")
            quarantine = os.path.join(root, "outbox", "parse_quarantine")
            os.makedirs(inbox)
            target = os.path.join(inbox, "bad.eml")
            with open(target, "w", encoding="utf-8") as handle:
                handle.write("x")
            stats = _stats(
                target,
                found=0,
                status="weak",
                weak_dropped=1,
                quality="cta_title",
                synth_reason="link_ratio",
            )
            result = quarantine_unparsed_inbox_files(
                stats, inbox_root=inbox, quarantine_dir=quarantine
            )
            self.assertEqual(result["moved"], 1)
            self.assertFalse(os.path.exists(target))
            dest = os.path.join(quarantine, "bad.eml")
            self.assertTrue(os.path.isfile(dest))
            with open(dest + ".json", encoding="utf-8") as handle:
                sidecar = json.load(handle)
            self.assertEqual(sidecar["status"], "weak")
            self.assertEqual(sidecar["quality"], "cta_title")

    def test_skips_files_with_found(self):
        with tempfile.TemporaryDirectory() as root:
            inbox = os.path.join(root, "inbox")
            quarantine = os.path.join(root, "outbox", "parse_quarantine")
            os.makedirs(inbox)
            target = os.path.join(inbox, "ok.eml")
            with open(target, "w", encoding="utf-8") as handle:
                handle.write("x")
            result = quarantine_unparsed_inbox_files(
                _stats(target, found=2, status="ok"),
                inbox_root=inbox,
                quarantine_dir=quarantine,
            )
            self.assertEqual(result["eligible"], 0)
            self.assertTrue(os.path.exists(target))


class LogIngestParseOutcomesTest(unittest.TestCase):
    def test_writes_non_ok_parse_file_events(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "sync.log")
            sync_log = SyncRunLog.open(log_path, echo_stdout=False)
            sync_log.run_start(source="test")
            written = log_ingest_parse_outcomes(
                sync_log,
                {
                    "positions_by_file": [
                        {"file": "a.eml", "status": "ok", "found": 1},
                        {
                            "file": "b.eml",
                            "status": "weak",
                            "found": 0,
                            "weak_dropped": 1,
                            "quality": "cta_title",
                            "synth_reason": "",
                        },
                    ]
                },
            )
            sync_log.run_end(status="complete")
            self.assertEqual(written, 1)
            with open(log_path, encoding="utf-8") as handle:
                body = handle.read()
            self.assertIn("event=parse_file", body)
            self.assertIn("status=weak", body)
            self.assertIn("b.eml", body)


if __name__ == "__main__":
    unittest.main()

