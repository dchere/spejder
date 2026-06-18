"""Tests for email_parser (.eml ingestion)."""

import os
import tempfile
import unittest

from spejder.parsers import email_parser

_SAMPLE_EML = """\
From: alerts@example.com
To: me@example.com
Subject: Acme Corp - job alert
MIME-Version: 1.0
Content-Type: multipart/alternative; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset=utf-8

See jobs at https://example.com/plain-link

--boundary123
Content-Type: text/html; charset=utf-8

<html><body><a href="https://example.com/html-link">Apply</a></body></html>

--boundary123--
"""


class ParseEmailFileTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.eml_path = os.path.join(self._tmpdir.name, "alert.eml")
        with open(self.eml_path, "w", encoding="utf-8") as f:
            f.write(_SAMPLE_EML)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_parse_email_file_extracts_subject_text_html_and_links(self):
        doc = email_parser.parse_email_file(self.eml_path)

        self.assertEqual(doc["title"], "Acme Corp - job alert")
        self.assertIn("https://example.com/plain-link", doc["text"])
        self.assertIn("https://example.com/html-link", doc["html"])
        self.assertIn("https://example.com/plain-link", doc["links"])
        self.assertIn("https://example.com/html-link", doc["links"])
        self.assertEqual(doc["path"], os.path.abspath(self.eml_path))
        self.assertEqual(doc["id"], os.path.abspath(self.eml_path))

    def test_parse_email_file_rejects_non_eml(self):
        html_path = os.path.join(self._tmpdir.name, "alert.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write("<html><body>test</body></html>")

        with self.assertRaises(ValueError):
            email_parser.parse_email_file(html_path)

    def test_load_files_finds_eml_only(self):
        html_path = os.path.join(self._tmpdir.name, "ignored.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write("<html><body>ignored</body></html>")

        docs = email_parser.load_files(self._tmpdir.name)

        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["path"], os.path.abspath(self.eml_path))


if __name__ == "__main__":
    unittest.main()
