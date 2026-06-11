"""Tests for job place resolution and Danfoss page extraction."""

import unittest

from spejder.jobs.parsing.platforms import _extract_jobindex_entries_by_link
from spejder.parsers.web_parser import _extract_place_from_page_text
from spejder.workflows.job_text_enrichment import _resolve_title_and_place

JOBINDEX_EGAA_TITLE = (
    "Social- og sundhedsassistent søges som fast afløservikar til nattevagter "
    "hos en lille dreng på 3 år i Aarhus (Egå)"
)


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

    def test_does_not_split_hyphenated_role_prefix(self):
        title, place = _resolve_title_and_place(JOBINDEX_EGAA_TITLE, "")
        self.assertEqual(place, "Aarhus (Egå)")
        self.assertNotIn("sundhedsassistent", place)
        self.assertTrue(title.startswith("Social- og"))

    def test_extracts_trailing_i_city_suffix(self):
        title, place = _resolve_title_and_place(
            "Plejehjemsassistent søges til aftenhold i Odense",
            "",
        )
        self.assertEqual(title, "Plejehjemsassistent søges til aftenhold")
        self.assertEqual(place, "Odense")


class JobindexPlaceExtractionTests(unittest.TestCase):
    def test_jobindex_extracts_place_from_title_suffix(self):
        html = f"""<table><tr><td>
<a href="https://www.jobindex.dk/jobannonce/r13854231">{JOBINDEX_EGAA_TITLE}</a>
Attrives
8250 Egå
PUBLISHED: 10-06-2026
</td></tr></table>"""
        entries = _extract_jobindex_entries_by_link(html)
        entry = entries["https://www.jobindex.dk/jobannonce/r13854231"]
        self.assertEqual(entry["place"], "Aarhus (Egå)")


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
