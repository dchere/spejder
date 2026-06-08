"""Tests for delete_processed_inbox_files."""

import os
import tempfile
import unittest

from spejder.workflows.ingest_utils import delete_processed_inbox_files


def _stats(file_path: str, found: int = 1) -> dict:
    return {"positions_by_file": [{"file": file_path, "found": found}]}


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


if __name__ == "__main__":
    unittest.main()
