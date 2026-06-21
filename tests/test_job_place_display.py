"""Tests for job place resolution and Danfoss page extraction."""

import unittest

from spejder.jobs.parsing.platforms import _extract_jobindex_entries_by_link
from spejder.jobs.parsing.utils import merge_jobindex_place, pick_jobindex_title
from spejder.parsers.web_parser import _extract_place_from_page_text
from spejder.workflows.job_text_enrichment import _resolve_title_and_place

JOBINDEX_EGAA_TITLE = (
    "Social- og sundhedsassistent søges som fast afløservikar til nattevagter "
    "hos en lille dreng på 3 år i Aarhus (Egå)"
)
JOBINDEX_H1675831_HTML = """
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1675831">
    Knorr-Bremse Rail Systems Denmark ApS
  </a>
  <a href="https://www.jobindex.dk/jobannonce/h1675831">
    Studentermedhjælper til IT i Aarhus
  </a>
  <a href="https://example.com/company">Knorr-Bremse Rail Systems Denmark ApS</a>
  8260 Tranbjerg J 10 min (ifølge dine indstillinger)
  PUBLISHED: 10-06-2026
</td></tr></table>
"""
JOBINDEX_H1675831_SINGLE_ANCHOR_HTML = """
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1675831">
    Knorr-Bremse Rail Systems Denmark ApS
  </a>
  <a href="https://example.com/company">Knorr-Bremse Rail Systems Denmark ApS</a>
  Studentermedhjælper til IT i Aarhus 8260 Tranbjerg J 10 min (ifølge dine indstillinger)
  PUBLISHED: 10-06-2026
</td></tr></table>
"""
JOBINDEX_H1675831_SINGLE_ANCHOR_NO_POSTCODE_HTML = """
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1675831">
    Knorr-Bremse Rail Systems Denmark ApS
  </a>
  <a href="https://example.com/company">Knorr-Bremse Rail Systems Denmark ApS</a>
  Studentermedhjælper til IT i Aarhus Tranbjerg J 10 min (ifølge dine indstillinger)
  PUBLISHED: 10-06-2026
</td></tr></table>
"""
JOBINDEX_H1675711_HTML = """
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1675711">
    Vestas Wind Systems A/S
  </a>
  <a href="https://www.jobindex.dk/jobannonce/h1675711">
    Patent Paralegal
  </a>
  <a href="https://example.com/company">Vestas Wind Systems A/S</a>
  Aarhus or Copenhagen 50 min (ifølge dine indstillinger)
  PUBLISHED: 10-06-2026
</td></tr></table>
"""
JOBINDEX_H1675711_SINGLE_ANCHOR_HTML = """
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1675711">
    Vestas Wind Systems A/S
  </a>
  <a href="https://example.com/company">Vestas Wind Systems A/S</a>
  Patent Paralegal Aarhus or Copenhagen 7 min (ifølge dine indstillinger)
  PUBLISHED: 10-06-2026
</td></tr></table>
"""


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
        self.assertEqual(title, "Plejehjemsassistent søges til aftenhold i Odense")
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

    def test_jobindex_two_anchor_prefers_title_over_company_anchor_h1675831(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1675831_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1675831"]
        self.assertEqual(entry["title"], "Studentermedhjælper til IT i Aarhus")
        self.assertEqual(entry["company"], "Knorr-Bremse Rail Systems Denmark ApS")
        self.assertEqual(entry["place"], "Aarhus Tranbjerg J")

    def test_jobindex_single_company_anchor_uses_compact_fallback_h1675831(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1675831_SINGLE_ANCHOR_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1675831"]
        self.assertEqual(entry["title"], "Studentermedhjælper til IT i Aarhus")
        self.assertEqual(entry["company"], "Knorr-Bremse Rail Systems Denmark ApS")
        self.assertEqual(entry["place"], "Aarhus Tranbjerg J")

    def test_jobindex_single_anchor_no_postcode_h1675831(self):
        entries = _extract_jobindex_entries_by_link(
            JOBINDEX_H1675831_SINGLE_ANCHOR_NO_POSTCODE_HTML
        )
        entry = entries["https://www.jobindex.dk/jobannonce/h1675831"]
        self.assertEqual(entry["title"], "Studentermedhjælper til IT i Aarhus")
        self.assertEqual(entry["place"], "Aarhus Tranbjerg J")

    def test_jobindex_two_anchor_prefers_title_over_company_anchor_h1675711(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1675711_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1675711"]
        self.assertEqual(entry["title"], "Patent Paralegal")
        self.assertEqual(entry["company"], "Vestas Wind Systems A/S")
        self.assertEqual(entry["place"], "Aarhus or Copenhagen")

    def test_jobindex_single_company_anchor_uses_compact_fallback_h1675711(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1675711_SINGLE_ANCHOR_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1675711"]
        self.assertEqual(entry["title"], "Patent Paralegal")
        self.assertEqual(entry["company"], "Vestas Wind Systems A/S")
        self.assertEqual(entry["place"], "Aarhus or Copenhagen")


class JobindexTitlePickTests(unittest.TestCase):
    def test_prefers_longest_non_company_candidate(self):
        company = "Knorr-Bremse Rail Systems Denmark ApS"
        title = pick_jobindex_title(
            [
                company,
                "Studentermedhjælper til IT i Aarhus",
            ],
            company=company,
        )
        self.assertEqual(title, "Studentermedhjælper til IT i Aarhus")

    def test_penalizes_company_suffix_anchor(self):
        company = "Vestas Wind Systems A/S"
        title = pick_jobindex_title(
            [
                "Very Long Example Company Group ApS",
                "Patent Paralegal",
            ],
            company=company,
        )
        self.assertEqual(title, "Patent Paralegal")


class JobindexPlaceMergeTests(unittest.TestCase):
    def test_merges_city_with_postcode_district(self):
        place = merge_jobindex_place(
            "Studentermedhjælper til IT i Aarhus",
            "8260 Tranbjerg J",
        )
        self.assertEqual(place, "Aarhus Tranbjerg J")

    def test_keeps_multi_location_listing(self):
        place = merge_jobindex_place(
            "Patent Paralegal",
            "Aarhus or Copenhagen",
        )
        self.assertEqual(place, "Aarhus or Copenhagen")

    def test_keeps_city_prefixed_listing(self):
        place = merge_jobindex_place(
            "Studentermedhjælper til IT i Aarhus",
            "Aarhus Tranbjerg J",
        )
        self.assertEqual(place, "Aarhus Tranbjerg J")

    def test_keeps_title_district_when_already_present(self):
        place = merge_jobindex_place(
            JOBINDEX_EGAA_TITLE,
            "8250 Egå",
        )
        self.assertEqual(place, "Aarhus (Egå)")


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
