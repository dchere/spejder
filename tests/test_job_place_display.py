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
JOBINDEX_H1681554_TITLE = (
    "YouSee søger salgsorienterede kunderådgivere til vores kundecenter i …"
)
JOBINDEX_H1681554_TWO_ANCHOR_HTML = f"""
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1681554">
    {JOBINDEX_H1681554_TITLE}
  </a>
  <a href="https://www.jobindex.dk/jobannonce/h1681554">
    Tranbjerg J
  </a>
  10 min (ifølge dine indstillinger)
  PUBLISHED: 08-07-2026
</td></tr></table>
"""
JOBINDEX_H1681554_SINGLE_ANCHOR_HTML = f"""
<table><tr><td>
  <a href="https://www.jobindex.dk/jobannonce/h1681554">
    {JOBINDEX_H1681554_TITLE}
  </a>
  Tranbjerg J 10 min (ifølge dine indstillinger)
  PUBLISHED: 08-07-2026
</td></tr></table>
"""
JOBINDEX_R13927484_TITLE = "Administrative medarbejdere til patientklager"
JOBINDEX_R13927484_COMPANY = "Styrelsen for Patientklager"
JOBINDEX_R13927484_HTML = f"""
<table><tr><td>
  Vil du arbejde med administration, service og retssikkerhed?
  <a href="https://www.jobindex.dk/jobannonce/r13927484">{JOBINDEX_R13927484_COMPANY}</a>
  <a href="https://www.jobindex.dk/jobannonce/r13927484">{JOBINDEX_R13927484_TITLE}</a>
  Aarhus N 35 min <a href="https://www.example.com/settings">(</a> settings )
  PUBLISHED: 01-08-2026
</td></tr></table>
"""
JOBINDEX_R13928009_HTML = """
<table><tr><td>
  Hvad slår en uddannelse i detail, der åbner en masse døre og byder på nye og sjove oplevelser?
  <a href="https://www.jobindex.dk/jobannonce/r13928009">Netto</a>
  <a href="https://www.jobindex.dk/jobannonce/r13928009">Butikselev - Lystrup</a>
  <a href="https://www.jobindex.dk/jobannonce/r13928009">Lystrup</a>
  40 min ( settings )
  PUBLISHED: 02-08-2026
</td></tr></table>
"""
JOBINDEX_R13928008_HTML = """
<table><tr><td>
  Som medarbejder hos føtex bliver din rygsæk fyldt med erfaringer – dem kan du bruge nu, sammen med os, men også senere hen i din karriere.
  <a href="https://www.jobindex.dk/jobannonce/r13928008">føtex</a>
  <a href="https://www.jobindex.dk/jobannonce/r13928008">Kasseassistent under 18 år - Højbjerg</a>
  <a href="https://www.jobindex.dk/jobannonce/r13928008">Højbjerg</a>
  15 min ( settings )
  PUBLISHED: 02-08-2026
</td></tr></table>
"""
JOBINDEX_R13928009_NO_BRAND_ANCHOR_HTML = """
<table><tr><td>
  Hvad slår en uddannelse i detail, der åbner en masse døre og byder på nye og sjove oplevelser?
  Netto
  <a href="https://www.jobindex.dk/jobannonce/r13928009">Butikselev - Lystrup</a>
  <a href="https://www.jobindex.dk/jobannonce/r13928009">Lystrup</a>
  40 min ( settings )
  PUBLISHED: 02-08-2026
</td></tr></table>
"""
JOBINDEX_H1686103_HTML = """
<table><tr><td>
  Skal du med til tops? Som Audit Trainee hos EY får du en uddannelse.
  <a href="https://www.jobindex.dk/jobannonce/h1686103">EY</a>
  <a href="https://www.jobindex.dk/jobannonce/h1686103">Audit Trainee i EY</a>
  30 min ( settings )
  PUBLISHED: 03-08-2026
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

    def test_jobindex_title_not_swapped_with_place_h1681554_two_anchor(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1681554_TWO_ANCHOR_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1681554"]
        self.assertEqual(entry["title"], JOBINDEX_H1681554_TITLE)
        self.assertEqual(entry["company"], "YouSee")
        self.assertEqual(entry["place"], "Tranbjerg J")

    def test_jobindex_title_not_swapped_with_place_h1681554_single_anchor(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1681554_SINGLE_ANCHOR_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1681554"]
        self.assertEqual(entry["title"], JOBINDEX_H1681554_TITLE)
        self.assertEqual(entry["company"], "YouSee")
        self.assertEqual(entry["place"], "Tranbjerg J")

    def test_jobindex_rejects_paren_company_and_title_in_place_r13927484(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_R13927484_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/r13927484"]
        self.assertEqual(entry["title"], JOBINDEX_R13927484_TITLE)
        self.assertEqual(entry["company"], JOBINDEX_R13927484_COMPANY)
        self.assertEqual(entry["place"], "Aarhus N")

    def test_jobindex_netto_butikselev_r13928009(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_R13928009_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/r13928009"]
        self.assertEqual(entry["company"], "Netto")
        self.assertEqual(entry["title"], "Butikselev - Lystrup")
        self.assertEqual(entry["place"], "Lystrup")

    def test_jobindex_fotex_kasseassistent_r13928008(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_R13928008_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/r13928008"]
        self.assertEqual(entry["company"], "føtex")
        self.assertEqual(entry["title"], "Kasseassistent under 18 år - Højbjerg")
        self.assertEqual(entry["place"], "Højbjerg")

    def test_jobindex_netto_brand_as_text_not_title_as_company(self):
        entries = _extract_jobindex_entries_by_link(
            JOBINDEX_R13928009_NO_BRAND_ANCHOR_HTML
        )
        entry = entries["https://www.jobindex.dk/jobannonce/r13928009"]
        self.assertEqual(entry["company"], "Netto")
        self.assertEqual(entry["title"], "Butikselev - Lystrup")
        self.assertEqual(entry["place"], "Lystrup")

    def test_jobindex_ey_trainee_not_swapped_h1686103(self):
        entries = _extract_jobindex_entries_by_link(JOBINDEX_H1686103_HTML)
        entry = entries["https://www.jobindex.dk/jobannonce/h1686103"]
        self.assertEqual(entry["company"], "EY")
        self.assertEqual(entry["title"], "Audit Trainee i EY")
        self.assertEqual(entry["place"], "")


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

    def test_penalizes_place_like_candidate(self):
        title = pick_jobindex_title(
            [
                JOBINDEX_H1681554_TITLE,
                "Tranbjerg J",
            ],
            listing_place_hint="Tranbjerg J",
        )
        self.assertEqual(title, JOBINDEX_H1681554_TITLE)

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
