"""Tests for career-alert email parsing (The Hub, Vestas, Oracle CX, Djinni)."""

import os
import unittest

from spejder.db import (
    _decode_mandrill_track_link,
    _is_djinni_position_link,
    _normalize_position_link,
    _provider_from_link,
)
from spejder.jobs.parsing.companies import sanitize_company_name
from spejder.jobs.parsing.core import extract_job_entries
from spejder.jobs.parsing.links import _is_job_link
from spejder.jobs.parsing.jobs2web import _parse_jobs2web_anchor_text
from spejder.jobs.parsing.platforms_career_alerts import (
    _extract_danfoss_entries_by_link,
    _extract_djinni_entries_by_link,
    _extract_novonordisk_entries_by_link,
    _extract_oracle_cx_entries_by_link,
    _extract_thehub_entries_by_link,
    _extract_vestas_entries_by_link,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "career_alerts")


def _read_fixture(name: str) -> str:
    path = os.path.join(FIXTURES, name)
    with open(path, encoding="utf-8") as f:
        return f.read()


class MandrillDecodeTest(unittest.TestCase):
    def test_decodes_thehub_track_link(self):
        raw = _read_fixture("mandrill_thehub_link.txt").strip()
        decoded = _decode_mandrill_track_link(raw)
        self.assertIn("thehub.io/jobs/", decoded)
        self.assertNotIn("mandrillapp.com", decoded)

    def test_decodes_djinni_track_link(self):
        raw = _read_fixture("mandrill_djinni_link.txt").strip()
        decoded = _decode_mandrill_track_link(raw)
        self.assertIn("djinni.co/jobs/", decoded)
        self.assertNotIn("mandrillapp.com", decoded)


class JobLinkRecognitionTest(unittest.TestCase):
    def test_danfoss_job_link(self):
        link = (
            "http://jobs.danfoss.com/job/Senior-Engineer/49821-en_GB"
            "?from=email&utm_source=J2WEmail"
        )
        normalized = _normalize_position_link(link)
        self.assertTrue(_is_job_link(normalized))
        self.assertNotIn("?", normalized)
        self.assertEqual(_provider_from_link(normalized), "Danfoss")

    def test_vestas_job_link(self):
        link = (
            "http://careers.vestas.com/job/Aarhus-N-Lead-Engineer-Regi-8200/1393178933"
            "?from=email&utm_source=J2WEmail"
        )
        normalized = _normalize_position_link(link)
        self.assertTrue(_is_job_link(normalized))
        self.assertNotIn("?", normalized)
        self.assertEqual(_provider_from_link(normalized), "Vestas")

    def test_novonordisk_job_link(self):
        link = (
            "http://careers.novonordisk.com/job/"
            "S%C3%B8borg-Lead-Software-Engineer-Capi/1402849933"
            "?from=email&utm_source=J2WEmail"
        )
        normalized = _normalize_position_link(link)
        self.assertTrue(_is_job_link(normalized))
        self.assertNotIn("?", normalized)
        self.assertEqual(_provider_from_link(normalized), "Novo Nordisk")

    def test_oracle_fa_job_link(self):
        link = (
            "https://hdjq.fa.us2.oraclecloud.com:443/hcmUI/CandidateExperience/"
            "en/sites/CX_1/job/26003444/?utm_medium=career+site"
        )
        normalized = _normalize_position_link(link)
        self.assertTrue(_is_job_link(normalized))
        self.assertNotIn(":443", normalized)
        self.assertEqual(_provider_from_link(normalized), "Emerson Career Site")

    def test_other_oracle_fa_host_still_oracle_cx(self):
        link = (
            "https://other.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/"
            "en/sites/CX_1/job/12345/"
        )
        normalized = _normalize_position_link(link)
        self.assertTrue(_is_job_link(normalized))
        self.assertEqual(_provider_from_link(normalized), "Oracle CX")

    def test_thehub_job_link_after_mandrill_decode(self):
        raw = _read_fixture("mandrill_thehub_link.txt").strip()
        normalized = _normalize_position_link(raw)
        self.assertRegex(normalized, r"https://thehub\.io/jobs/[0-9a-f]{12,}")
        self.assertTrue(_is_job_link(normalized))
        self.assertEqual(_provider_from_link(normalized), "The Hub")

    def test_djinni_job_link_after_mandrill_decode(self):
        raw = _read_fixture("mandrill_djinni_link.txt").strip()
        normalized = _normalize_position_link(raw)
        self.assertEqual(
            normalized,
            "https://djinni.co/jobs/830953-senior-power-bi-consultant-contract",
        )
        self.assertTrue(_is_job_link(normalized))
        self.assertNotIn("?", normalized)
        self.assertEqual(_provider_from_link(normalized), "Djinni")

    def test_djinni_listing_link_is_not_job_link(self):
        link = "https://djinni.co/jobs/?utm_medium=email"
        normalized = _normalize_position_link(link)
        self.assertFalse(_is_job_link(normalized))
        self.assertFalse(_is_djinni_position_link(normalized))

    def test_djinni_position_link_helper_matches_job_links(self):
        link = "https://djinni.co/jobs/830919-python-developer"
        self.assertTrue(_is_djinni_position_link(link))
        self.assertTrue(_is_job_link(link))


class PlatformExtractorTest(unittest.TestCase):
    def test_vestas_extractor(self):
        html = _read_fixture("vestas_snippet.html")
        by_link = _extract_vestas_entries_by_link(html)
        self.assertGreaterEqual(len(by_link), 1)
        entry = next(iter(by_link.values()))
        self.assertEqual(entry["company"], "Vestas")
        self.assertTrue(entry["title"])

    def test_vestas_extractor_ignores_danfoss_j2w_links(self):
        html = _read_fixture("danfoss_snippet.html")
        by_link = _extract_vestas_entries_by_link(html)
        self.assertEqual(by_link, {})

    def test_danfoss_extractor(self):
        html = _read_fixture("danfoss_snippet.html")
        by_link = _extract_danfoss_entries_by_link(html)
        self.assertEqual(len(by_link), 2)
        senior = by_link["http://jobs.danfoss.com/job/Senior-Engineer/49821-en_GB"]
        self.assertEqual(senior["company"], "Danfoss")
        self.assertEqual(senior["title"], "Senior Engineer")
        self.assertEqual(senior["place"], "Reynosa, MEX")
        self.assertEqual(senior["source"], "Danfoss")

    def test_novonordisk_extractor(self):
        html = _read_fixture("novonordisk_snippet.html")
        by_link = _extract_novonordisk_entries_by_link(html)
        self.assertEqual(len(by_link), 1)
        entry = next(iter(by_link.values()))
        self.assertEqual(entry["company"], "Novo Nordisk")
        self.assertEqual(entry["title"], "Lead Software Engineer")
        self.assertEqual(entry["place"], "Søborg, Capital Region of Denmark, DK")
        self.assertEqual(entry["source"], "Novo Nordisk")

    def test_thehub_digest_extractor(self):
        html = _read_fixture("thehub_digest_snippet.html")
        by_link = _extract_thehub_entries_by_link(html)
        self.assertGreaterEqual(len(by_link), 1)
        entry = next(iter(by_link.values()))
        self.assertEqual(entry["source"], "The Hub")
        self.assertIn("Engineer", entry["title"])

    def test_thehub_single_extractor(self):
        html = _read_fixture("thehub_single_snippet.html")
        by_link = _extract_thehub_entries_by_link(html)
        self.assertGreaterEqual(len(by_link), 1)

    def test_oracle_extractor(self):
        html = _read_fixture("oracle_snippet.html")
        by_link = _extract_oracle_cx_entries_by_link(html)
        self.assertGreaterEqual(len(by_link), 1)
        entry = next(iter(by_link.values()))
        self.assertEqual(entry["company"], "Emerson")
        self.assertEqual(entry["source"], "Emerson Career Site")
        self.assertIn("Engineer", entry["title"])

    def test_djinni_digest_extractor(self):
        html = _read_fixture("djinni_digest_snippet.html")
        by_link = _extract_djinni_entries_by_link(html)
        self.assertGreaterEqual(len(by_link), 2)
        by_title = {entry["title"]: entry for entry in by_link.values()}
        self.assertIn("Python Developer", by_title)
        door3 = by_title["Senior Power BI Consultant (Contract)"]
        self.assertEqual(door3["company"], "DOOR3")
        self.assertEqual(door3["work_type"], "Part-time")
        self.assertEqual(door3["place"], "Тільки віддалено")
        python_dev = by_title["Python Developer"]
        self.assertEqual(python_dev["company"], "Stafnear")
        self.assertEqual(python_dev["work_type"], "Part-time")
        motion = by_title["Freelance Motion Designer (6037 / Epica)"]
        self.assertEqual(motion["company"], "")
        for entry in by_link.values():
            self.assertEqual(entry["source"], "Djinni")
            self.assertTrue(entry["title"])


class SanitizeCompanyNameTest(unittest.TestCase):
    def test_strips_linkedin_selected_boilerplate(self):
        self.assertEqual(
            sanitize_company_name("Novo Nordisk according to your selected"),
            "Novo Nordisk",
        )

    def test_leaves_clean_company_unchanged(self):
        self.assertEqual(sanitize_company_name("Novo Nordisk"), "Novo Nordisk")

    def test_empty_input(self):
        self.assertEqual(sanitize_company_name(""), "")
        self.assertEqual(sanitize_company_name("   "), "")


class Jobs2WebAnchorParserTest(unittest.TestCase):
    def test_dash_format_splits_title_and_place(self):
        parsed = _parse_jobs2web_anchor_text("Senior Engineer - Reynosa, MEX")
        self.assertEqual(parsed["title"], "Senior Engineer")
        self.assertEqual(parsed["place"], "Reynosa, MEX")

    def test_middot_format_splits_title_place_and_work_type(self):
        parsed = _parse_jobs2web_anchor_text(
            "Lead Software Engineer · Novo Nordisk according to your selected · "
            "Søborg (On-site)"
        )
        self.assertEqual(parsed["title"], "Lead Software Engineer")
        self.assertEqual(parsed["place"], "Søborg")
        self.assertEqual(parsed["work_type"], "On-site")

    def test_single_middot_does_not_fall_through_to_dash_split(self):
        parsed = _parse_jobs2web_anchor_text(
            "Engineer · Remote - Copenhagen (On-site)"
        )
        self.assertEqual(parsed["title"], "Engineer · Remote - Copenhagen")
        self.assertEqual(parsed["place"], "")
        self.assertEqual(parsed["work_type"], "On-site")


class NovoNordiskLinkedInDigestTest(unittest.TestCase):
    def test_linkedin_style_digest_company_is_novo_nordisk(self):
        text = """Lead Software Engineer
Novo Nordisk according to your selected
Søborg, Denmark
View job: https://careers.novonordisk.com/job/S%C3%B8borg-Lead-Software-Engineer-Capi/1402849933
"""
        html = (
            '<a href="https://careers.novonordisk.com/job/'
            'S%C3%B8borg-Lead-Software-Engineer-Capi/1402849933">'
            "Lead Software Engineer · Novo Nordisk according to your selected · "
            "Søborg (On-site)</a>"
        )
        link = (
            "https://careers.novonordisk.com/job/"
            "S%C3%B8borg-Lead-Software-Engineer-Capi/1402849933"
        )
        doc = {"html": html, "text": text, "title": "", "links": [link]}
        entries = extract_job_entries(doc)
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["company"], "Novo Nordisk")
        self.assertEqual(entry["title"], "Lead Software Engineer")
        self.assertEqual(entry["place"], "Søborg")
        self.assertEqual(entry["work_type"], "On-site")
        self.assertEqual(entry["source"], "Novo Nordisk")


class CareerAlertIntegrationTest(unittest.TestCase):
    def test_fixture_snippets_produce_entries(self):
        for name in (
            "vestas_snippet.html",
            "danfoss_snippet.html",
            "thehub_digest_snippet.html",
            "thehub_single_snippet.html",
            "oracle_snippet.html",
            "djinni_digest_snippet.html",
            "novonordisk_snippet.html",
        ):
            html = _read_fixture(name)
            doc = {"html": html, "text": "", "title": "", "links": []}
            # Collect links from HTML so the links loop can fire.
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            doc["links"] = [a.get("href") for a in soup.find_all("a", href=True)]
            entries = extract_job_entries(doc)
            with self.subTest(fixture=name):
                self.assertGreater(len(entries), 0, msg=f"No entries from {name}")
                if name == "djinni_digest_snippet.html":
                    self.assertTrue(all(e.get("source") == "Djinni" for e in entries))
                if name == "danfoss_snippet.html":
                    self.assertTrue(all(e.get("company") == "Danfoss" for e in entries))
                    self.assertTrue(all(e.get("source") == "Danfoss" for e in entries))
                if name == "novonordisk_snippet.html":
                    self.assertTrue(all(e.get("company") == "Novo Nordisk" for e in entries))
                    self.assertTrue(all(e.get("source") == "Novo Nordisk" for e in entries))


if __name__ == "__main__":
    unittest.main()
