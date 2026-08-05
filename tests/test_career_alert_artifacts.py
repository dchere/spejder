"""Tests for career-alert artifact interpreter, store, and synthesizer."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.jobs.ingestion import ingest_docs_to_db
from spejder.jobs.parsing.artifact_interpreter import (
    href_matches_artifact,
    interpret_artifact,
    interpret_artifacts,
)
from spejder.jobs.parsing.artifact_schema import (
    CareerAlertArtifact,
    ExtractConfig,
    FieldRecipes,
    MatchConfig,
    compile_safe_path_regex,
)
from spejder.jobs.parsing.artifact_store import load_artifacts, save_overlay_artifact
from spejder.jobs.parsing.artifact_synth import (
    _extract_json_object,
    try_synthesize_artifact,
    validate_synth_thresholds,
)
from spejder.jobs.parsing.core import extract_job_entries
from spejder.jobs.parsing.html_shrink import shrink_html_for_prompt
from spejder.jobs.parsing.platforms_career_alerts import (
    _extract_danfoss_entries_by_link,
    _extract_novonordisk_entries_by_link,
    _extract_vestas_entries_by_link,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures", "career_alerts")
SHIPPED = os.path.join(
    os.path.dirname(__file__),
    "..",
    "jobs",
    "parsing",
    "artifacts",
)


def _read_fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as handle:
        return handle.read()


def _load_shipped(artifact_id: str) -> CareerAlertArtifact:
    path = os.path.join(SHIPPED, f"{artifact_id}.json")
    with open(path, encoding="utf-8") as handle:
        return CareerAlertArtifact.model_validate(json.load(handle))


class Jobs2WebArtifactParityTest(unittest.TestCase):
    def test_danfoss_parity(self):
        html = _read_fixture("danfoss_snippet.html")
        artifact = _load_shipped("jobs2web_danfoss")
        expected = _extract_danfoss_entries_by_link(html)
        got = interpret_artifact(html, artifact)
        self.assertEqual(set(got), set(expected))
        for link, fields in expected.items():
            for key in ("title", "company", "place", "work_type", "source"):
                self.assertEqual(got[link].get(key), fields.get(key), msg=key)

    def test_vestas_parity(self):
        html = _read_fixture("vestas_snippet.html")
        artifact = _load_shipped("jobs2web_vestas")
        expected = _extract_vestas_entries_by_link(html)
        got = interpret_artifact(html, artifact)
        self.assertEqual(set(got), set(expected))
        for link, fields in expected.items():
            for key in ("title", "company", "place", "work_type", "source"):
                self.assertEqual(got[link].get(key), fields.get(key), msg=key)

    def test_novonordisk_parity(self):
        html = _read_fixture("novonordisk_snippet.html")
        artifact = _load_shipped("jobs2web_novonordisk")
        expected = _extract_novonordisk_entries_by_link(html)
        got = interpret_artifact(html, artifact)
        self.assertEqual(set(got), set(expected))
        for link, fields in expected.items():
            for key in ("title", "company", "place", "work_type", "source"):
                self.assertEqual(got[link].get(key), fields.get(key), msg=key)

    def test_extract_job_entries_artifacts_only_fallback(self):
        """With no built-in match for a custom host, artifact fills entries."""
        artifact = CareerAlertArtifact.model_validate(
            {
                "id": "jobs2web_custom",
                "priority": 50,
                "match": {
                    "host_substrings": ["jobs.example-corp.test"],
                    "path_includes": ["/job/"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Example Corp",
                    "source": "Example Corp",
                },
            }
        )
        html = (
            '<a href="http://jobs.example-corp.test/job/Senior-Engineer/1">'
            "Senior Engineer - Reynosa, MEX</a>"
        )
        link = "http://jobs.example-corp.test/job/Senior-Engineer/1"
        doc = {"html": html, "text": "", "title": "", "links": [link]}
        entries = extract_job_entries(doc, artifacts=[artifact])
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["company"], "Example Corp")
        self.assertEqual(entries[0]["title"], "Senior Engineer")
        self.assertEqual(entries[0]["place"], "Reynosa, MEX")
        self.assertEqual(entries[0]["source"], "Example Corp")

    def test_builtin_fields_win_over_artifact(self):
        """Non-empty built-in fields are not overwritten by artifact recipes."""
        html = _read_fixture("danfoss_snippet.html")
        base = _load_shipped("jobs2web_danfoss")
        artifact = base.model_copy(
            update={
                "fields": base.fields.model_copy(
                    update={"company": "Artifact Override Co", "source": "Artifact Src"}
                )
            }
        )
        builtin = _extract_danfoss_entries_by_link(html)
        self.assertTrue(builtin)
        link = next(iter(builtin))
        doc = {
            "html": html,
            "text": f"Danfoss\n{builtin[link]['title']}\n{link}",
            "title": "",
            "links": [link],
        }
        entries = extract_job_entries(doc, artifacts=[artifact])
        by_link = {e["position_link"]: e for e in entries}
        self.assertIn(link, by_link)
        self.assertEqual(by_link[link]["company"], "Danfoss")
        self.assertEqual(by_link[link]["source"], "Danfoss")


class ArtifactStoreTest(unittest.TestCase):
    def test_overlay_overrides_shipped_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = _load_shipped("jobs2web_danfoss")
            override = base.model_copy(
                update={
                    "priority": 999,
                    "fields": base.fields.model_copy(update={"company": "Override Co"}),
                }
            )
            save_overlay_artifact(override, overlay_dir=tmp)
            loaded = load_artifacts(overlay_dir=tmp, shipped_dir=SHIPPED)
            by_id = {a.id: a for a in loaded}
            self.assertEqual(by_id["jobs2web_danfoss"].fields.company, "Override Co")
            self.assertEqual(by_id["jobs2web_danfoss"].priority, 999)

    def test_disabled_ids_skipped(self):
        loaded = load_artifacts(
            overlay_dir="/tmp/spejder-missing-overlay",
            shipped_dir=SHIPPED,
            disabled_ids=["jobs2web_danfoss"],
        )
        ids = {a.id for a in loaded}
        self.assertNotIn("jobs2web_danfoss", ids)
        self.assertIn("jobs2web_vestas", ids)

    def test_overlay_dir_none_loads_shipped_only(self):
        """Without overlay_dir, do not merge default ./career_alert_artifacts."""
        with tempfile.TemporaryDirectory() as tmp:
            base = _load_shipped("jobs2web_danfoss")
            override = base.model_copy(
                update={
                    "fields": base.fields.model_copy(update={"company": "Local Overlay Co"}),
                }
            )
            cwd = os.getcwd()
            try:
                os.chdir(tmp)
                os.makedirs("career_alert_artifacts", exist_ok=True)
                save_overlay_artifact(override, overlay_dir="career_alert_artifacts")
                loaded = load_artifacts(overlay_dir=None, shipped_dir=SHIPPED)
            finally:
                os.chdir(cwd)
            by_id = {a.id: a for a in loaded}
            self.assertEqual(by_id["jobs2web_danfoss"].fields.company, "Danfoss")


def _artifact_with_raw_match(*, host_substrings: list[str], path_includes: list[str]) -> CareerAlertArtifact:
    """Bypass schema validators to exercise interpreter fail-closed paths."""
    return CareerAlertArtifact.model_construct(
        id="raw_match",
        version=1,
        priority=0,
        enabled=True,
        match=MatchConfig.model_construct(
            host_substrings=host_substrings,
            path_includes=path_includes,
        ),
        extract=ExtractConfig(),
        fields=FieldRecipes(from_anchor="anchor_text_compact"),
        source="manual",
    )


class ArtifactMatchSafetyTest(unittest.TestCase):
    def test_blank_host_or_path_substrings_match_nothing(self):
        """Interpreter fails closed when only blank match strings remain."""
        href = "http://jobs.example.com/job/1"
        self.assertFalse(
            href_matches_artifact(
                href,
                _artifact_with_raw_match(host_substrings=[""], path_includes=["/job/"]),
            )
        )
        self.assertFalse(
            href_matches_artifact(
                href,
                _artifact_with_raw_match(
                    host_substrings=["jobs.example.com"], path_includes=[""]
                ),
            )
        )

    def test_match_config_rejects_empty_or_blank_lists(self):
        with self.assertRaises(Exception):
            MatchConfig(host_substrings=[], path_includes=["/job/"])
        with self.assertRaises(Exception):
            MatchConfig(host_substrings=[""], path_includes=["/job/"])
        with self.assertRaises(Exception):
            MatchConfig(host_substrings=["jobs.example.com"], path_includes=[])
        with self.assertRaises(Exception):
            MatchConfig(host_substrings=["jobs.example.com"], path_includes=[""])

    def test_require_path_regex_rejects_nested_quantifiers(self):
        self.assertIsNone(compile_safe_path_regex("(a+)+"))
        self.assertIsNone(compile_safe_path_regex("x" * 81))
        self.assertIsNotNone(compile_safe_path_regex(r"/job/.+/\d+"))
        with self.assertRaises(Exception):
            MatchConfig(host_substrings=["x"], require_path_regex="(a+)+")


class ArtifactSynthTest(unittest.TestCase):
    def test_validate_thresholds_accept(self):
        proposed = {"http://jobs.example.com/job/1": "Senior Engineer"}
        recovered = {
            "http://jobs.example.com/job/1": {
                "title": "Senior Engineer",
                "company": "Example",
            }
        }
        ok, reason = validate_synth_thresholds(
            proposed, recovered, link_ratio=0.8, title_ratio=0.8
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_validate_thresholds_accepts_full_anchor_titles(self):
        """LLM often returns full Jobs2Web anchor text; interpreter splits title/place."""
        proposed = {
            "http://careers.capgemini.com/job/A/1": "AI Data Architect - Copenhagen, DK",
        }
        recovered = {
            "http://careers.capgemini.com/job/A/1": {
                "title": "AI Data Architect",
                "raw_text": "AI Data Architect - Copenhagen, DK",
            },
        }
        ok, reason = validate_synth_thresholds(
            proposed, recovered, link_ratio=0.8, title_ratio=0.8
        )
        self.assertTrue(ok)
        self.assertEqual(reason, "ok")

    def test_validate_thresholds_reject_link_ratio(self):
        proposed = {
            "http://jobs.example.com/job/1": "A",
            "http://jobs.example.com/job/2": "B",
        }
        recovered = {"http://jobs.example.com/job/1": {"title": "A"}}
        ok, reason = validate_synth_thresholds(
            proposed, recovered, link_ratio=0.8, title_ratio=0.8
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "link_ratio")

    def test_synth_persists_on_accept(self):
        html = _read_fixture("danfoss_snippet.html")
        artifact = _load_shipped("jobs2web_danfoss")
        # LLM may invent an id; synth always rewrites to synth_<host>_<hash6>.
        synth_body = artifact.model_copy(
            update={"id": "synth_danfoss_test", "source": "llm_synth"}
        ).model_dump(mode="json")
        positions = [
            {"position_link": link, "title": fields["title"]}
            for link, fields in _extract_danfoss_entries_by_link(html).items()
        ]
        payload = {"positions": positions, "artifact": synth_body}
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(payload)
        digest = hashlib.sha1(html.encode("utf-8", errors="ignore")).hexdigest()[:6]
        expected_id = f"synth_jobsdanfosscom_{digest}"
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(
                career_alert_artifacts_dir=tmp,
                career_alert_synth_enabled=True,
            )
            saved, reason = try_synthesize_artifact(html, llm, profile, overlay_dir=tmp)
            self.assertEqual(reason, "ok")
            self.assertIsNotNone(saved)
            self.assertEqual(saved.id, expected_id)
            self.assertTrue(os.path.exists(os.path.join(tmp, f"{expected_id}.json")))

    def test_synth_rejects_empty_match_rules(self):
        """Empty match lists fail schema validation before persist."""
        html = _read_fixture("danfoss_snippet.html")
        positions = [
            {"position_link": link, "title": fields["title"]}
            for link, fields in _extract_danfoss_entries_by_link(html).items()
        ]
        payload = {
            "positions": positions,
            "artifact": {
                "id": "synth_broad",
                "match": {"host_substrings": [], "path_includes": []},
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Danfoss",
                    "source": "Danfoss",
                },
            },
        }
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(payload)
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(career_alert_artifacts_dir=tmp)
            saved, reason = try_synthesize_artifact(html, llm, profile, overlay_dir=tmp)
            self.assertIsNone(saved)
            self.assertEqual(reason, "schema")
            self.assertEqual(os.listdir(tmp), [])

    def test_synth_rejects_match_too_broad(self):
        """Proposed ≪ recovered under passing ratios → match_too_broad, no overlay."""
        anchors = [
            (
                f"http://jobs.broad.test/job/Role-{i}/{i}",
                f"Role {i} - City {i}",
            )
            for i in range(1, 6)
        ]
        html = "".join(
            f'<a href="{href}">{title}</a>' for href, title in anchors
        )
        # One proposed link that the artifact will recover among five.
        positions = [{"position_link": anchors[0][0], "title": "Role 1"}]
        payload = {
            "positions": positions,
            "artifact": {
                "id": "synth_broad_match",
                "match": {
                    "host_substrings": ["jobs.broad.test"],
                    "path_includes": ["/job/"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Broad Co",
                    "source": "Broad Co",
                },
            },
        }
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(payload)
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(career_alert_artifacts_dir=tmp)
            saved, reason = try_synthesize_artifact(html, llm, profile, overlay_dir=tmp)
            self.assertIsNone(saved)
            self.assertEqual(reason, "match_too_broad")
            self.assertEqual(os.listdir(tmp), [])

    def test_synth_always_rewrites_llm_id(self):
        html = _read_fixture("danfoss_snippet.html")
        artifact = _load_shipped("jobs2web_danfoss")
        synth_body = artifact.model_copy(
            update={"id": "synth_chosen_by_llm", "source": "llm_synth"}
        ).model_dump(mode="json")
        positions = [
            {"position_link": link, "title": fields["title"]}
            for link, fields in _extract_danfoss_entries_by_link(html).items()
        ]
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(
            {"positions": positions, "artifact": synth_body}
        )
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(career_alert_artifacts_dir=tmp)
            saved, reason = try_synthesize_artifact(html, llm, profile, overlay_dir=tmp)
            self.assertEqual(reason, "ok")
            self.assertIsNotNone(saved)
            self.assertNotEqual(saved.id, "synth_chosen_by_llm")
            self.assertTrue(saved.id.startswith("synth_jobsdanfosscom_"))

    def test_synth_no_persist_on_reject(self):
        html = _read_fixture("danfoss_snippet.html")
        bad = {
            "positions": [
                {"position_link": "http://jobs.danfoss.com/job/Missing/1", "title": "X"}
            ],
            "artifact": {
                "id": "synth_bad",
                "match": {
                    "host_substrings": ["jobs.danfoss.com"],
                    "path_includes": ["/job/"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Danfoss",
                    "source": "Danfoss",
                },
            },
        }
        llm = MagicMock()
        llm.model_path = "/models/test.gguf"
        llm.generate.return_value = json.dumps(bad)
        with tempfile.TemporaryDirectory() as tmp:
            profile = AppConfig(career_alert_artifacts_dir=tmp)
            saved, reason = try_synthesize_artifact(html, llm, profile, overlay_dir=tmp)
            self.assertIsNone(saved)
            self.assertNotEqual(reason, "ok")
            self.assertEqual(os.listdir(tmp), [])


class IngestSynthHookTest(unittest.TestCase):
    def test_found_zero_synth_success_reextracts(self):
        html = (
            '<a href="http://jobs.example-corp.test/job/Senior-Engineer/1">'
            "Senior Engineer - Reynosa, MEX</a>"
        )
        link = "http://jobs.example-corp.test/job/Senior-Engineer/1"
        doc = {
            "html": html,
            "text": "",
            "title": "",
            "links": [link],
            "path": "mail.eml",
        }
        artifact = CareerAlertArtifact.model_validate(
            {
                "id": "synth_example_abcdef",
                "match": {
                    "host_substrings": ["jobs.example-corp.test"],
                    "path_includes": ["/job/"],
                },
                "extract": {"mode": "filtered_links"},
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Example Corp",
                    "source": "Example Corp",
                },
                "source": "llm_synth",
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            overlay = os.path.join(tmp, "overlay")
            os.makedirs(overlay)
            ensure_db(db_path)
            profile = AppConfig(
                career_alert_artifacts_dir=overlay,
                career_alert_synth_enabled=True,
                default_model="/models/test.gguf",
            )
            llm = MagicMock()
            with patch(
                "spejder.jobs.ingestion.try_synthesize_artifact",
                return_value=(artifact, "ok"),
            ) as synth_mock:
                with patch(
                    "spejder.jobs.ingestion.load_artifacts",
                    side_effect=[
                        [],  # initial cache: no artifacts → found=0
                        [artifact],  # after synth reload
                    ],
                ):
                    stats = ingest_docs_to_db(
                        db_path,
                        [doc],
                        llm=llm,
                        runtime_profile=profile,
                    )
            synth_mock.assert_called_once()
            self.assertEqual(stats["processed"], 1)
            self.assertEqual(stats["inserted_new"], 1)

    def test_failed_synth_leaves_empty_overlay_no_upsert(self):
        html = '<a href="http://jobs.unknown.test/job/X/1">X - Y</a>'
        doc = {
            "html": html,
            "text": "",
            "title": "",
            "links": ["http://jobs.unknown.test/job/X/1"],
            "path": "unknown.eml",
        }
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            overlay = os.path.join(tmp, "overlay")
            os.makedirs(overlay)
            ensure_db(db_path)
            profile = AppConfig(
                career_alert_artifacts_dir=overlay,
                career_alert_synth_enabled=True,
                default_model="/models/test.gguf",
            )
            llm = MagicMock()
            with patch(
                "spejder.jobs.ingestion.try_synthesize_artifact",
                return_value=(None, "link_ratio"),
            ):
                with patch(
                    "spejder.jobs.ingestion.load_artifacts",
                    return_value=[],
                ):
                    stats = ingest_docs_to_db(
                        db_path,
                        [doc],
                        llm=llm,
                        runtime_profile=profile,
                    )
            self.assertEqual(stats["processed"], 0)
            self.assertEqual(stats["inserted_new"], 0)
            self.assertEqual(os.listdir(overlay), [])

    def test_synth_enabled_without_model_prints_no_model(self):
        doc = {
            "html": "<p>no jobs</p>",
            "text": "",
            "title": "",
            "links": [],
            "path": "empty.eml",
        }
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "jobs.db")
            overlay = os.path.join(tmp, "overlay")
            os.makedirs(overlay)
            ensure_db(db_path)
            profile = AppConfig(
                career_alert_artifacts_dir=overlay,
                career_alert_synth_enabled=True,
                default_model="",
            )
            with patch("spejder.jobs.ingestion.load_artifacts", return_value=[]):
                with patch("builtins.print") as mock_print:
                    stats = ingest_docs_to_db(
                        db_path,
                        [doc],
                        llm=None,
                        runtime_profile=profile,
                    )
            self.assertEqual(stats["processed"], 0)
            printed = " ".join(str(c) for c in mock_print.call_args_list)
            self.assertIn("no_model", printed)


class InterpretPriorityTest(unittest.TestCase):
    def test_higher_priority_wins(self):
        low = CareerAlertArtifact.model_validate(
            {
                "id": "low",
                "priority": 1,
                "match": {
                    "host_substrings": ["jobs.danfoss.com"],
                    "path_includes": ["/job/"],
                },
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "Low",
                    "source": "Low",
                },
            }
        )
        high = CareerAlertArtifact.model_validate(
            {
                "id": "high",
                "priority": 100,
                "match": {
                    "host_substrings": ["jobs.danfoss.com"],
                    "path_includes": ["/job/"],
                },
                "fields": {
                    "from_anchor": "jobs2web_middot_or_dash",
                    "company": "High",
                    "source": "High",
                },
            }
        )
        html = _read_fixture("danfoss_snippet.html")
        merged = interpret_artifacts(html, [low, high])
        self.assertTrue(merged)
        self.assertTrue(all(v["company"] == "High" for v in merged.values()))

    def test_html_shrink_strips_query_from_hrefs(self):
        html = (
            '<a href="http://careers.capgemini.com/job/Role/1'
            '?from=email&utm_source=J2WEmail">Role - City, DK</a>'
        )
        shrunk = shrink_html_for_prompt(html)
        self.assertIn('href="http://careers.capgemini.com/job/Role/1"', shrunk)
        self.assertNotIn("utm_source", shrunk)
        self.assertNotIn("from=email", shrunk)

    def test_extract_json_recovers_truncated_positions(self):
        truncated = (
            '{\n'
            '  "artifact": {\n'
            '    "id": "synth_demo",\n'
            '    "version": 1,\n'
            '    "priority": 50,\n'
            '    "enabled": true,\n'
            '    "match": {"host_substrings": ["careers.capgemini.com"], '
            '"path_includes": ["/job/"]},\n'
            '    "extract": {"mode": "filtered_links"},\n'
            '    "fields": {"from_anchor": "jobs2web_middot_or_dash", '
            '"company": "Capgemini", "source": "Capgemini"}\n'
            '  },\n'
            '  "positions": [\n'
            '    {"position_link": "http://careers.capgemini.com/job/A/1", '
            '"title": "A"},\n'
            '    {"position_link": "http://careers.capgemini.com/job/B/2", '
            '"title": "B cut'
        )
        payload = _extract_json_object(truncated)
        self.assertIn("artifact", payload)
        self.assertEqual(payload["artifact"]["fields"]["company"], "Capgemini")
        self.assertEqual(len(payload.get("positions") or []), 1)
        self.assertEqual(
            payload["positions"][0]["position_link"],
            "http://careers.capgemini.com/job/A/1",
        )


if __name__ == "__main__":
    unittest.main()
