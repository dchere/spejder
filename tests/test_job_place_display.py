"""Tests for job place resolution and Danfoss page extraction."""

import unittest

from spejder.parsers.web_parser import _extract_place_from_page_text
from spejder.workflows.job_text_enrichment import _resolve_title_and_place


class ResolveTitleAndPlaceTests(unittest.TestCase):
    def test_splits_title_when_place_missing(self):
        title, place = _resolve_title_and_place(
            "Senior Engineer - Reynosa, MEX",
            "",
        )
        self.assertEqual(title, "Senior Engineer")
        self.assertEqual(place, "Reynosa, MEX")

    def test_keeps_existing_place(self):
        title, place = _resolve_title_and_place(
            "Senior Engineer - Reynosa, MEX",
            "Copenhagen",
        )
        self.assertEqual(title, "Senior Engineer - Reynosa, MEX")
        self.assertEqual(place, "Copenhagen")


class DanfossPlaceExtractionTests(unittest.TestCase):
    def test_extracts_job_location_short(self):
        page_text = (
            "Job Title: Senior Engineer Req ID: 49821 "
            "Job Location (Short): Reynosa, MEX Employment Type: Full Time "
            "Segment: Danfoss Power Solutions Segment"
        )
        place = _extract_place_from_page_text(
            "http://jobs.danfoss.com/job/Senior-Engineer/49821-en_GB",
            page_text,
        )
        self.assertEqual(place, "Reynosa, MEX")


if __name__ == "__main__":
    unittest.main()
