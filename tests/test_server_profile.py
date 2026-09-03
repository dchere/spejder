"""API and metadata tests for the profile editor."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.managers.dashboard_templates import jinja_env
from spejder.managers.profile_editor import (
    PROFILE_FIELD_META,
    READONLY_FIELDS,
    assert_field_meta_complete,
    merge_profile_updates,
)
from spejder.server import create_app


def _minimal_dashboard_context():
    return {
        "title": "Test dashboard",
        "relevant_total_count": 0,
        "not_relevant_total_count": 0,
        "viewed_total": 0,
        "len_relevant_items": 0,
        "len_not_relevant_items": 0,
        "len_viewed_today_items": 0,
        "len_applied_items": 0,
        "len_interview_items": 0,
        "len_stopped_items": 0,
        "len_hidden_items": 0,
        "len_skills_items": 0,
        "relevant_cards": "",
        "not_relevant_cards": "",
        "viewed_today_cards": "",
        "applied_cards": "",
        "interview_cards": "",
        "stopped_cards": "",
        "hidden_cards": "",
        "skills_table_html": '<p class="empty">No skills found.</p>',
        "skills_empty_added_at_sort": "0000",
        "portrait_text": "",
        "has_portrait": False,
        "report_mtime": "Wed, 01 Jan 2025 00:00:00 GMT",
    }


def _read_template(name: str) -> str:
    templates_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "templates",
    )
    path = os.path.join(templates_dir, name)
    with open(path, encoding="utf-8") as handle:
        return handle.read()


class ProfileFieldMetaTest(unittest.TestCase):
    def test_meta_covers_every_appconfig_field(self):
        assert_field_meta_complete()
        self.assertEqual(set(PROFILE_FIELD_META), set(AppConfig.model_fields))
        for name in READONLY_FIELDS:
            self.assertTrue(PROFILE_FIELD_META[name]["readonly"])


class ProfileEditorMergeTest(unittest.TestCase):
    def test_rejects_unknown_keys(self):
        profile = AppConfig(min_score=2.0)
        with self.assertRaises(ValueError) as ctx:
            merge_profile_updates(profile, {"not_a_field": 1})
        self.assertIn("unknown profile fields", str(ctx.exception))

    def test_readonly_retained(self):
        profile = AppConfig(
            skill_bigram_toxicity_threshold=0.42,
            bad_cloud_seeded=True,
            min_score=2.0,
        )
        updated = merge_profile_updates(
            profile,
            {
                "min_score": 3.5,
                "skill_bigram_toxicity_threshold": 9.9,
                "bad_cloud_seeded": False,
            },
        )
        self.assertEqual(updated.min_score, 3.5)
        self.assertEqual(updated.skill_bigram_toxicity_threshold, 0.42)
        self.assertTrue(updated.bad_cloud_seeded)

    def test_list_and_skill_pattern_parsing(self):
        profile = AppConfig()
        updated = merge_profile_updates(
            profile,
            {
                "include_keywords": ["python", "backend"],
                "career_alert_artifacts_disabled": ["jobs2web_danfoss"],
                "known_skill_patterns": [
                    {"name": "Python", "pattern": r"\bpython\b"},
                ],
            },
        )
        self.assertEqual(updated.include_keywords, ["python", "backend"])
        self.assertEqual(updated.career_alert_artifacts_disabled, ["jobs2web_danfoss"])
        self.assertEqual(
            updated.known_skill_patterns,
            [{"name": "Python", "pattern": r"\bpython\b"}],
        )

    def test_validation_rejects_bad_types(self):
        profile = AppConfig()
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            merge_profile_updates(profile, {"min_score": "not-a-number"})


def create_profile_test_app(db_path: str, report_dir: str, profile_path: str, runtime_profile: AppConfig):
    def _reload():
        fresh = AppConfig.load(profile_path)
        for key, value in fresh.model_dump().items():
            setattr(runtime_profile, key, value)

    def _persist():
        runtime_profile.save(profile_path)

    app = create_app(
        db_path=db_path,
        profile_path=profile_path,
        runtime_profile=runtime_profile,
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=_persist,
        reload_runtime_profile=_reload,
        queue_dashboard_rebuild=lambda reason="": None,
        cli_verbose=False,
    )
    return app


class ServerProfileApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)
        self.runtime_profile = AppConfig(
            min_score=2.0,
            include_keywords=["python"],
            skill_bigram_toxicity_threshold=0.11,
            bad_cloud_seeded=True,
        )
        self.runtime_profile.save(self.profile_path)
        self.app = create_profile_test_app(
            self.db_path, self.report_dir, self.profile_path, self.runtime_profile
        )
        self.client = TestClient(self.app)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_get_profile_includes_values_and_metadata(self):
        response = self.client.get("/api/profile")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["values"]["min_score"], 2.0)
        field_names = {item["name"] for item in payload["fields"]}
        self.assertEqual(field_names, set(AppConfig.model_fields))
        group_ids = [item["id"] for item in payload["groups"]]
        self.assertIn("keywords_scoring", group_ids)
        self.assertIn("auto_written", group_ids)

    def test_save_roundtrip_reloads_runtime(self):
        response = self.client.post(
            "/api/profile/save",
            json={
                "min_score": 4.25,
                "include_keywords": ["python", "rust"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["values"]["min_score"], 4.25)
        self.assertEqual(self.runtime_profile.min_score, 4.25)
        self.assertEqual(self.runtime_profile.include_keywords, ["python", "rust"])
        with open(self.profile_path, encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual(saved["min_score"], 4.25)

        response = self.client.get("/api/profile")
        self.assertEqual(response.json()["values"]["min_score"], 4.25)

    def test_save_list_and_skill_pattern_roundtrip(self):
        response = self.client.post(
            "/api/profile/save",
            json={
                "career_alert_artifacts_disabled": ["jobs2web_danfoss", "other_id"],
                "known_skill_patterns": [
                    {"name": "Python", "pattern": r"\bpython\b"},
                    {"name": "Rust", "pattern": r"\brust\b"},
                ],
                "exclude_keywords": ["intern", "junior"],
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            payload["values"]["career_alert_artifacts_disabled"],
            ["jobs2web_danfoss", "other_id"],
        )
        self.assertEqual(
            payload["values"]["known_skill_patterns"],
            [
                {"name": "Python", "pattern": r"\bpython\b"},
                {"name": "Rust", "pattern": r"\brust\b"},
            ],
        )
        self.assertEqual(payload["values"]["exclude_keywords"], ["intern", "junior"])
        self.assertEqual(
            self.runtime_profile.career_alert_artifacts_disabled,
            ["jobs2web_danfoss", "other_id"],
        )
        self.assertEqual(
            self.runtime_profile.known_skill_patterns,
            [
                {"name": "Python", "pattern": r"\bpython\b"},
                {"name": "Rust", "pattern": r"\brust\b"},
            ],
        )
        self.assertEqual(self.runtime_profile.exclude_keywords, ["intern", "junior"])
        with open(self.profile_path, encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual(
            saved["career_alert_artifacts_disabled"],
            ["jobs2web_danfoss", "other_id"],
        )
        self.assertEqual(
            saved["known_skill_patterns"],
            [
                {"name": "Python", "pattern": r"\bpython\b"},
                {"name": "Rust", "pattern": r"\brust\b"},
            ],
        )
        self.assertEqual(saved["exclude_keywords"], ["intern", "junior"])

    def test_save_rejects_non_object_body(self):
        response = self.client.post("/api/profile/save", json=["not", "an", "object"])
        self.assertEqual(response.status_code, 400)
        self.assertIn("JSON object", response.json()["error"])

    def test_save_rejects_unknown_and_bad_types(self):
        response = self.client.post("/api/profile/save", json={"nope": 1})
        self.assertEqual(response.status_code, 400)
        self.assertIn("unknown", response.json()["error"])

        response = self.client.post("/api/profile/save", json={"server_port": "abc"})
        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertIn("errors", payload)
        self.assertIn("server_port", payload["errors"])

    def test_save_ignores_readonly_client_values(self):
        response = self.client.post(
            "/api/profile/save",
            json={
                "skill_bigram_toxicity_threshold": 99.0,
                "bad_cloud_seeded": False,
                "min_score": 5.0,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.runtime_profile.min_score, 5.0)
        self.assertEqual(self.runtime_profile.skill_bigram_toxicity_threshold, 0.11)
        self.assertTrue(self.runtime_profile.bad_cloud_seeded)

    def test_partial_save_preserves_runtime_sibling_field(self):
        sibling = ["from-skills-tab"]
        self.runtime_profile.user_skills = sibling
        response = self.client.post("/api/profile/save", json={"min_score": 9.0})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["values"]["min_score"], 9.0)
        self.assertEqual(payload["values"]["user_skills"], sibling)
        self.assertEqual(self.runtime_profile.min_score, 9.0)
        self.assertEqual(self.runtime_profile.user_skills, sibling)
        with open(self.profile_path, encoding="utf-8") as handle:
            saved = json.load(handle)
        self.assertEqual(saved["min_score"], 9.0)
        self.assertEqual(saved["user_skills"], sibling)
        get_values = self.client.get("/api/profile").json()["values"]
        self.assertEqual(get_values["min_score"], 9.0)
        self.assertEqual(get_values["user_skills"], sibling)

    def test_save_oserror_returns_500_envelope(self):
        with patch(
            "spejder.managers.profile_editor.AppConfig.save",
            side_effect=OSError("disk full"),
        ):
            response = self.client.post("/api/profile/save", json={"min_score": 3.0})
        self.assertEqual(response.status_code, 500)
        payload = response.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "failed to write profile")
        self.assertEqual(self.runtime_profile.min_score, 2.0)


class ProfileDashboardTemplateTest(unittest.TestCase):
    def test_main_dashboard_has_profile_left_of_sync(self):
        html = jinja_env.get_template("dashboard.html").render(**_minimal_dashboard_context())
        profile_idx = html.find('id="btn-profile"')
        sync_idx = html.find('id="btn-sync-inbox"')
        self.assertGreater(profile_idx, 0)
        self.assertGreater(sync_idx, profile_idx)
        self.assertIn('id="panel-profile"', html)
        self.assertIn("setMode('profile')", html)

    def test_company_dashboard_lacks_profile(self):
        text = _read_template("company_dashboard.html")
        self.assertNotIn('id="btn-profile"', text)
        self.assertNotIn('id="panel-profile"', text)
        self.assertNotIn("/api/profile", text)


if __name__ == "__main__":
    unittest.main()
