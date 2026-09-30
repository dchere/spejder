"""Tests for skill-extract input assembly: title presence, page budget, prefer-page."""

import unittest
from unittest.mock import patch

from spejder.parsers.web_parser import _append_page_context_to_raw_text
from spejder.workflows.job_text_enrichment import (
    PAGE_SUBSTANTIAL_MIN_CHARS,
    _enrich_raw_text_with_position_page,
    _raw_mostly_contained_in_page,
)
from spejder.workflows.text_prepend import (
    _prepend_title_to_raw_text,
    _title_already_present,
)


class TitlePresenceDedupeTest(unittest.TestCase):
    def test_skips_exact_title_prefix(self):
        raw = "Title: Platform Engineer\n\nWe need Kubernetes."
        self.assertTrue(_title_already_present("Platform Engineer", raw))
        self.assertEqual(
            _prepend_title_to_raw_text("Platform Engineer", raw),
            raw,
        )

    def test_skips_when_title_appears_mid_body(self):
        # IT-Day style: metadata lines, then an embedded Title: line.
        raw = (
            "Source: IT-Day\nCompany: Acme\n"
            "Title: Platform Engineer\n\n"
            "Build reliable systems with Kubernetes and Go."
        )
        self.assertTrue(_title_already_present("Platform Engineer", raw))
        out = _prepend_title_to_raw_text("Platform Engineer", raw)
        self.assertEqual(out.count("Title: Platform Engineer"), 1)
        self.assertFalse(out.lower().startswith("title: platform engineer\n\nsource:"))

    def test_skips_when_bare_title_phrase_present(self):
        raw = "Platform Engineer role in Aarhus. Requirements: Python, AWS."
        self.assertTrue(_title_already_present("Platform Engineer", raw))
        self.assertEqual(
            _prepend_title_to_raw_text("Platform Engineer", raw),
            raw,
        )

    def test_prepends_when_title_absent(self):
        raw = "We are hiring for a backend role using Python."
        out = _prepend_title_to_raw_text("Backend Engineer", raw)
        self.assertTrue(out.startswith("Title: Backend Engineer\n\n"))
        self.assertIn(raw, out)


class ReservePageBudgetTest(unittest.TestCase):
    def test_truncates_base_before_page_tail(self):
        link = "https://example.com/jobs/1"
        # Unique page tail that must survive the 9000-char cap.
        page_tail = "REQUIRED_SKILL_UNIQUE_TAIL_XYZ"
        page = ("PAGEBODY " * 200) + page_tail  # well under 3000
        # Oversized listing/title blob that would formerly push the page off the end.
        base = "LISTING " * 2000  # ~14000 chars
        merged = _append_page_context_to_raw_text(base, link, page, max_chars=9000)
        self.assertIn(page_tail, merged)
        self.assertIn(f"[POSITION_PAGE_CONTEXT {link}]", merged)
        self.assertLessEqual(len(merged), 9000)
        # Base was cut; page block intact.
        self.assertTrue(merged.endswith(page_tail) or page_tail in merged.split(link, 1)[-1])

    def test_idempotent_marker(self):
        link = "https://example.com/jobs/2"
        base = f"Title: X\n\n[POSITION_PAGE_CONTEXT {link}]\nalready"
        self.assertEqual(
            _append_page_context_to_raw_text(base, link, "new page text"),
            base,
        )

    def test_page_alone_when_base_empty(self):
        out = _append_page_context_to_raw_text("", "https://x.test/j", "hello page")
        self.assertEqual(out, "hello page")


class PreferPageEnrichmentTest(unittest.TestCase):
    def test_raw_mostly_contained(self):
        page = "We need Python and Kubernetes experience for Platform Engineer roles."
        raw = "Python and Kubernetes experience"
        self.assertTrue(_raw_mostly_contained_in_page(raw, page))
        self.assertFalse(
            _raw_mostly_contained_in_page(
                "Rust WASM blockchain niche requirement only here",
                page,
            )
        )

    @patch("spejder.workflows.job_text_enrichment._get_position_page_context")
    @patch("spejder.workflows.job_text_enrichment._get_title_english_for_row")
    def test_skill_path_drops_summary_and_prefers_page(
        self, mock_title, mock_page
    ):
        mock_title.return_value = "Platform Engineer"
        page = (
            "Platform Engineer at Acme. "
            + ("We build cloud platforms. " * 40)
            + "Must know Kubernetes Terraform Python. "
            + ("More duties. " * 20)
        )
        self.assertGreaterEqual(len(page), PAGE_SUBSTANTIAL_MIN_CHARS)
        mock_page.return_value = page

        row = {
            "id": 0,
            "raw_text": "Platform Engineer. Short listing: Python Kubernetes.",
            "summary": "Paraphrase of the listing that should not reach skill extract.",
            "position_link": "https://example.com/jobs/prefer",
            "place": "Aarhus",
            "title": "Platform Engineer",
        }
        out = _enrich_raw_text_with_position_page(
            ":memory:",
            row,
            include_summary=False,
            prefer_page=True,
        )
        self.assertNotIn("Summary:", out)
        self.assertNotIn("Paraphrase of the listing", out)
        # Listing raw dropped as mostly contained; page kept.
        self.assertIn("[POSITION_PAGE_CONTEXT https://example.com/jobs/prefer]", out)
        self.assertIn("Kubernetes Terraform Python", out)
        # Prefer-page dropped duplicated listing; at most one explicit Title: line.
        self.assertLessEqual(out.lower().count("title: platform engineer"), 1)

    @patch("spejder.workflows.job_text_enrichment._translate_text_to_english_if_needed")
    @patch("spejder.workflows.job_text_enrichment._get_position_page_context")
    @patch("spejder.workflows.job_text_enrichment._get_title_english_for_row")
    def test_description_path_still_includes_summary(
        self, mock_title, mock_page, mock_translate
    ):
        mock_title.return_value = "Backend Engineer"
        mock_page.return_value = ""
        mock_translate.side_effect = lambda text, runtime_profile=None: text
        row = {
            "id": 0,
            "raw_text": "Build APIs in Go.",
            "summary": "Go API engineer for payments.",
            "position_link": "",
            "place": "Copenhagen",
            "title": "Backend Engineer",
        }
        out = _enrich_raw_text_with_position_page(
            ":memory:",
            row,
            include_summary=True,
            prefer_page=False,
        )
        self.assertTrue(out.startswith("Summary: Go API engineer for payments."))
        self.assertIn("Title: Backend Engineer", out)

    @patch("spejder.workflows.job_text_enrichment._get_position_page_context")
    @patch("spejder.workflows.job_text_enrichment._get_title_english_for_row")
    def test_prefer_page_keeps_unique_listing_bits(
        self, mock_title, mock_page
    ):
        mock_title.return_value = "Data Engineer"
        page = ("Career page body about cloud data. " * 30) + "Spark Airflow SQL."
        self.assertGreaterEqual(len(page), PAGE_SUBSTANTIAL_MIN_CHARS)
        mock_page.return_value = page
        unique = "Listing-only cue: UNIQUE_INGEST_SKILL_ZZZ must appear."
        row = {
            "id": 0,
            "raw_text": unique,
            "summary": "Should be ignored on skill path.",
            "position_link": "https://example.com/jobs/unique",
            "place": "Odense",
            "title": "Data Engineer",
        }
        out = _enrich_raw_text_with_position_page(
            ":memory:",
            row,
            include_summary=False,
            prefer_page=True,
        )
        self.assertIn("UNIQUE_INGEST_SKILL_ZZZ", out)
        self.assertIn("Spark Airflow SQL", out)
        self.assertNotIn("Summary:", out)


if __name__ == "__main__":
    unittest.main()
