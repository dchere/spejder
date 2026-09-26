"""Tests for platform registry, CTA heuristic expansion, and track unwrap."""

from __future__ import annotations

import os
import tempfile
import unittest

from spejder.config import AppConfig
from spejder.jobs.parsing.artifact_heuristic import (
    cta_labels_from_artifacts,
    draft_cta_ancestor_artifact,
)
from spejder.jobs.parsing.artifact_interpreter import href_matches_artifact
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_synth import try_synthesize_artifact
from spejder.jobs.parsing.core import extract_job_entries
from spejder.jobs.parsing.links import unwrap_track_link
from spejder.jobs.parsing.platform_registry import registered_platforms
from spejder.parsers import email_parser

_FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "career_alerts")


def _read_fixture(name: str) -> str:
    with open(os.path.join(_FIXTURES, name), encoding="utf-8") as handle:
        return handle.read()


class PlatformRegistryTest(unittest.TestCase):
    def test_registered_order_matches_merge_path(self):
        names = [spec.name for spec in registered_platforms()]
        self.assertEqual(
            names,
            ["google", "thehub", "djinni", "oracle_cx", "demant", "jobindex"],
        )

    def test_jobindex_has_merge_transform(self):
        by_name = {spec.name: spec for spec in registered_platforms()}
        self.assertIsNotNone(by_name["jobindex"].merge_transform)
        stripped = by_name["jobindex"].merge_transform(
            {"title": "X", "work_type": "Full-time", "source": "Jobindex"}
        )
        self.assertEqual(stripped, {"title": "X"})

    def test_extract_job_entries_still_finds_google(self):
        link = (
            "https://www.google.com/about/careers/applications/"
            "jobs/results/12345-software-engineer-munich"
        )
        html = f'<a href="{link}">Software Engineer</a>'
        entries = extract_job_entries(
            {"html": html, "text": "", "title": "", "links": [link]}
        )
        self.assertTrue(any(e.get("source") == "Google Careers" for e in entries))


class UnwrapTrackLinkTest(unittest.TestCase):
    def test_unwrap_mandrill_thehub(self):
        raw = _read_fixture("mandrill_thehub_link.txt").strip()
        unwrapped = unwrap_track_link(raw)
        self.assertIn("thehub.io/jobs/", unwrapped)
        self.assertNotIn("mandrillapp.com", unwrapped)

    def test_href_match_uses_unwrapped_host(self):
        raw = _read_fixture("mandrill_thehub_link.txt").strip()
        artifact = CareerAlertArtifact.model_validate(
            {
                "id": "unwrap_test",
                "version": 1,
                "priority": 10,
                "enabled": True,
                "match": {
                    "host_substrings": ["thehub.io"],
                    "path_includes": ["/jobs/"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {"from_anchor": "anchor_text_compact", "company": "X"},
                "source": "manual",
            }
        )
        self.assertTrue(href_matches_artifact(raw, artifact))
        self.assertFalse(
            href_matches_artifact("https://mandrillapp.com/track/click/1/x", artifact)
        )


class CtaHeuristicTest(unittest.TestCase):
    def test_single_card_digest_drafts_artifact(self):
        html = """
        <html><body>
          <table><tr>
            <td>
              <div><strong>Regional Segment Lead for Power Generation Systems</strong></div>
              <p>Lead regional initiatives that shape the future of power generation.</p>
            </td>
            <td>
              <a href="https://clicks.icims.eu/f/a/onlytoken~~/x"><strong>Apply here</strong></a>
            </td>
          </tr></table>
        </body></html>
        """
        draft = draft_cta_ancestor_artifact(
            html,
            title_hint="Schneider Electric - job alert notification",
            from_hint="Schneider Electric Careers <noreply@example.com>",
        )
        self.assertIsNotNone(draft)
        self.assertEqual(draft.match.anchor_text_equals, ["Apply here"])
        self.assertEqual(draft.fields.company, "Schneider Electric")

    def test_learns_cta_label_from_overlay_artifacts(self):
        learned = CareerAlertArtifact.model_validate(
            {
                "id": "overlay_cta",
                "version": 1,
                "priority": 50,
                "enabled": True,
                "match": {
                    "host_substrings": ["example.com"],
                    "path_includes": ["/jobs/"],
                    "anchor_text_equals": ["Submit application"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "ancestor_strong_or_first_line",
                    "company": "Acme",
                },
                "source": "llm_synth",
            }
        )
        labels = cta_labels_from_artifacts([learned])
        self.assertIn("submit application", labels)
        html = """
        <html><body>
          <table><tr>
            <td>
              <div><strong>Backend Platform Engineer for Distributed Systems</strong></div>
              <p>Build reliable services for our customers worldwide.</p>
            </td>
            <td>
              <a href="https://careers.example.com/jobs/42">
                <strong>Submit application</strong>
              </a>
            </td>
          </tr></table>
        </body></html>
        """
        draft = draft_cta_ancestor_artifact(html, known_cta_labels=labels)
        self.assertIsNotNone(draft)
        self.assertEqual(draft.match.anchor_text_equals, ["Submit application"])

    def test_company_from_subject_not_hardcoded_list(self):
        html = """
        <html><body>
          <table><tr>
            <td>
              <div><strong>Cloud Architect for Enterprise Platforms</strong></div>
              <p>Design cloud architecture across product lines.</p>
            </td>
            <td><a href="https://clicks.icims.eu/f/a/t1~~/x"><strong>Apply now</strong></a></td>
          </tr>
          <tr>
            <td>
              <div><strong>SRE Lead for Customer Reliability</strong></div>
              <p>Own reliability for customer-facing services.</p>
            </td>
            <td><a href="https://clicks.icims.eu/f/a/t2~~/x"><strong>Apply now</strong></a></td>
          </tr></table>
        </body></html>
        """
        draft = draft_cta_ancestor_artifact(
            html,
            title_hint="Acme Robotics - job alert notification",
        )
        self.assertIsNotNone(draft)
        self.assertEqual(draft.fields.company, "Acme Robotics")

    def test_heuristic_synth_single_card_without_llm(self):
        html = """
        <html><body>
          <table><tr>
            <td>
              <div><strong>Regional Segment Lead - Power Generation</strong></div>
              <p>Lead regional initiatives that shape the future of power generation.</p>
            </td>
            <td>
              <a href="https://clicks.icims.eu/f/a/job1token~~/AAAfxhA~/payload1">
                <strong>Apply here</strong>
              </a>
            </td>
          </tr></table>
        </body></html>
        """
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(career_alert_artifacts_dir=tmp)
            saved, reason = try_synthesize_artifact(
                html,
                None,
                profile,
                overlay_dir=tmp,
                title_hint="Schneider Electric - job alert notification",
            )
            self.assertEqual(reason, "ok")
            self.assertIsNotNone(saved)
            self.assertEqual(saved.source, "heuristic")


class EmailFromHeaderTest(unittest.TestCase):
    def test_parse_email_file_includes_from(self):
        sample = """\
From: alerts@example.com
To: me@example.com
Subject: Acme Corp - job alert
MIME-Version: 1.0
Content-Type: text/plain; charset=utf-8

See https://example.com/job
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "alert.eml")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(sample)
            doc = email_parser.parse_email_file(path)
            self.assertEqual(doc["from"], "alerts@example.com")
            self.assertEqual(doc["title"], "Acme Corp - job alert")


if __name__ == "__main__":
    unittest.main()
