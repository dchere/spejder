"""P4 synth depth: budget, format-drift disable, prev_sibling_text opcode."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.jobs.parsing.artifact_interpreter import interpret_artifact
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import (
    list_loaded_artifacts,
    save_overlay_artifact,
)
from spejder.jobs.parsing.artifact_synth import try_synthesize_artifact
from spejder.jobs.parsing.artifact_synth_budget import (
    consume_synth_attempt,
    host_budget_key,
    primary_host_from_html,
    remaining_synth_attempts,
)
from spejder.jobs.parsing.artifact_synth_drift import find_stale_artifacts
from spejder.jobs.parsing.core import extract_job_entries


class SynthBudgetTest(unittest.TestCase):
    def test_host_key_and_primary_host(self):
        self.assertEqual(host_budget_key("Jobs.Example.COM"), "jobsexamplecom")
        html = (
            '<a href="https://jobs.example.com/job/1">A</a>'
            '<a href="https://jobs.example.com/job/2">B</a>'
            '<a href="https://other.test/x">C</a>'
        )
        self.assertEqual(primary_host_from_html(html), "jobsexamplecom")

    def test_consume_caps_per_day(self):
        with tempfile.TemporaryDirectory() as tmp:
            now = datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc)
            for _ in range(3):
                ok, reason = consume_synth_attempt(
                    tmp, "jobs.example.com", max_per_day=3, now=now
                )
                self.assertTrue(ok)
                self.assertEqual(reason, "ok")
            ok, reason = consume_synth_attempt(
                tmp, "jobs.example.com", max_per_day=3, now=now
            )
            self.assertFalse(ok)
            self.assertEqual(reason, "host_budget")
            self.assertEqual(
                remaining_synth_attempts(
                    tmp, "jobs.example.com", max_per_day=3, now=now
                ),
                0,
            )
            # Other hosts stay independent.
            ok, reason = consume_synth_attempt(
                tmp, "other.test", max_per_day=3, now=now
            )
            self.assertTrue(ok)

    def test_zero_max_disables_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(5):
                ok, reason = consume_synth_attempt(
                    tmp, "jobs.example.com", max_per_day=0
                )
                self.assertTrue(ok)
                self.assertEqual(reason, "ok")

    def test_try_synthesize_respects_host_budget(self):
        html = (
            '<a href="http://jobs.budget.test/job/Role/1">'
            "Role One - City</a>"
        )
        artifact_body = {
            "id": "ignored",
            "match": {
                "host_substrings": ["jobs.budget.test"],
                "path_includes": ["/job/"],
            },
            "extract": {"mode": "filtered_links"},
            "fields": {
                "from_anchor": "jobs2web_middot_or_dash",
                "company": "Budget Co",
                "source": "Budget Co",
            },
        }
        payload = {
            "artifact": artifact_body,
            "positions": [
                {
                    "position_link": "http://jobs.budget.test/job/Role/1",
                    "title": "Role One",
                }
            ],
        }
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(payload)
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(
                career_alert_artifacts_dir=tmp,
                career_alert_synth_max_per_host_day=1,
            )
            with patch(
                "spejder.jobs.parsing.artifact_synth.draft_cta_ancestor_artifact",
                return_value=None,
            ):
                saved, reason = try_synthesize_artifact(
                    html, llm, profile, overlay_dir=tmp
                )
                self.assertEqual(reason, "ok")
                self.assertIsNotNone(saved)
                saved2, reason2 = try_synthesize_artifact(
                    html, llm, profile, overlay_dir=tmp
                )
            self.assertIsNone(saved2)
            self.assertEqual(reason2, "host_budget")
            self.assertEqual(llm.generate.call_count, 1)


class PrevSiblingOpcodeTest(unittest.TestCase):
    def test_prev_sibling_text_title(self):
        html = """
        <table>
          <tr>
            <td><strong>Platform Engineer</strong></td>
            <td><a href="https://careers.example.com/f/a/123">Apply here</a></td>
          </tr>
        </table>
        """
        artifact = CareerAlertArtifact.model_validate(
            {
                "id": "sibling_demo",
                "match": {
                    "host_substrings": ["careers.example.com"],
                    "path_includes": ["/f/a/"],
                    "anchor_text_equals": ["Apply here"],
                },
                "fields": {
                    "from_anchor": "prev_sibling_text",
                    "company": "Example",
                    "source": "Example",
                },
            }
        )
        recovered = interpret_artifact(html, artifact)
        self.assertEqual(len(recovered), 1)
        title = next(iter(recovered.values()))["title"]
        self.assertEqual(title, "Platform Engineer")
        entries = extract_job_entries(
            {"html": html, "text": "", "title": "", "links": []},
            artifacts=[artifact],
        )
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["title"], "Platform Engineer")


class FormatDriftTest(unittest.TestCase):
    def test_find_stale_and_disable_on_synth(self):
        # Stale overlay: wrong path so it prefilters via links list? Use matching
        # path but wrong from_anchor so titles stay CTA-only / empty.
        html = """
        <div>
          <span>Cloud Architect</span>
          <a href="https://clicks.drift.test/f/a/99">Apply here</a>
        </div>
        """
        stale = CareerAlertArtifact.model_validate(
            {
                "id": "synth_clicksdrifttest_old",
                "priority": 80,
                "match": {
                    "host_substrings": ["clicks.drift.test"],
                    "path_includes": ["/f/a/"],
                    "anchor_text_equals": ["Apply here"],
                },
                "fields": {
                    # Anchor text is CTA → no usable title.
                    "from_anchor": "anchor_text_compact",
                    "company": "Drift Co",
                    "source": "Drift Co",
                },
                "source": "llm_synth",
            }
        )
        good_body = {
            "id": "ignored",
            "priority": 90,
            "match": {
                "host_substrings": ["clicks.drift.test"],
                "path_includes": ["/f/a/"],
                "anchor_text_equals": ["Apply here"],
            },
            "extract": {"mode": "filtered_links"},
            "fields": {
                "from_anchor": "prev_sibling_text",
                "company": "Drift Co",
                "source": "Drift Co",
            },
        }
        payload = {
            "artifact": good_body,
            "positions": [
                {
                    "position_link": "https://clicks.drift.test/f/a/99",
                    "title": "Cloud Architect",
                }
            ],
        }
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(payload)
        with tempfile.TemporaryDirectory() as tmp:
            save_overlay_artifact(stale, overlay_dir=tmp)
            found_stale = find_stale_artifacts(
                html,
                [stale],
                links=["https://clicks.drift.test/f/a/99"],
            )
            self.assertEqual([a.id for a in found_stale], [stale.id])
            profile = AppConfig(
                career_alert_artifacts_dir=tmp,
                career_alert_synth_max_per_host_day=5,
            )
            with patch(
                "spejder.jobs.parsing.artifact_synth.draft_cta_ancestor_artifact",
                return_value=None,
            ):
                saved, reason = try_synthesize_artifact(
                    html,
                    llm,
                    profile,
                    overlay_dir=tmp,
                    links=["https://clicks.drift.test/f/a/99"],
                    existing_artifacts=[stale],
                )
            self.assertEqual(reason, "ok")
            self.assertIsNotNone(saved)
            loaded = list_loaded_artifacts(overlay_dir=tmp, include_disabled=True)
            by_id = {item.artifact.id: item.artifact for item in loaded}
            self.assertFalse(by_id[stale.id].enabled)
            self.assertTrue(by_id[saved.id].enabled)
            # Prompt should mention drift when stale exists.
            prompt = llm.generate.call_args[0][0]
            self.assertIn("format drift", prompt.casefold())
            self.assertIn(stale.id, prompt)


if __name__ == "__main__":
    unittest.main()
