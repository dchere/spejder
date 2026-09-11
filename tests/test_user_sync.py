"""Tests for CV user-skill sync merge behavior."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.user_sync import sync_user_skills


class SyncUserSkillsUnwantedMergeTest(unittest.TestCase):
    def test_extracted_unwanted_skill_not_merged_into_user_skills(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            profile_path = os.path.join(tmpdir, "profile.json")
            seed = AppConfig(user_skills=["python"], unwanted_skills=["sales"])
            seed.save(profile_path)
            captured = {}
            real_validate = AppConfig.model_validate

            def capture_validate(data):
                captured["user_skills"] = list(data.get("user_skills") or [])
                return real_validate(data)

            with patch(
                "spejder.extractors.skill_extractor.user_sync.load_cv_text",
                return_value="cv text",
            ), patch(
                "spejder.extractors.skill_extractor.user_sync._translate_text_to_english_if_needed",
                return_value="cv text",
            ), patch(
                "spejder.extractors.skill_extractor.user_sync._extract_user_skills_from_cv",
                return_value=["sales", "kubernetes"],
            ), patch(
                "spejder.extractors.skill_extractor.user_sync.ensure_db",
            ), patch(
                "spejder.extractors.skill_extractor.user_sync._ensure_skill_pattern_seed_migration",
            ), patch(
                "spejder.extractors.skill_extractor.user_sync.load_profile",
                return_value=AppConfig.load(profile_path),
            ), patch(
                "spejder.extractors.skill_extractor.user_sync.AppConfig.model_validate",
                side_effect=capture_validate,
            ):
                sync_user_skills(
                    profile=profile_path,
                    db=os.path.join(tmpdir, "jobs.db"),
                    cv=os.path.join(tmpdir, "CV"),
                    llm=MagicMock(),
                )

            merged = [s.lower() for s in captured["user_skills"]]
            self.assertNotIn("sales", merged)
            self.assertIn("python", merged)
            self.assertIn("kubernetes", merged)
            reloaded = AppConfig.load(profile_path)
            self.assertNotIn("sales", [s.lower() for s in reloaded.user_skills])
            self.assertIn("kubernetes", [s.lower() for s in reloaded.user_skills])


if __name__ == "__main__":
    unittest.main()
