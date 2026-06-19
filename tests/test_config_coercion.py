"""Tests for skill_antipattern_good_skills_count coercion, load/construct, and save round-trips.

Translation-focused config tests live in test_translation_config.py.
"""

import json
import os
import tempfile
import unittest

from spejder.config import (
    AppConfig,
    SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
    SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX,
    coerce_skill_antipattern_good_skills_count,
)


class CoerceSkillAntipatternGoodSkillsCountTests(unittest.TestCase):
    def test_coerce_matrix(self):
        cases = [
            (True, None),
            (False, None),
            (2.7, None),
            ("50", None),
            ("not-a-number", None),
            ([], None),
            ({}, None),
            (None, None),
            (float("nan"), None),
            (float("inf"), None),
            (0, 1),
            (-5, 1),
            (500, SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX),
            (
                SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
                SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
            ),
            (
                float(SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT),
                SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
            ),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(
                    coerce_skill_antipattern_good_skills_count(raw),
                    expected,
                )


class AppConfigLoadCoercionTests(unittest.TestCase):
    def test_load_clamps_below_minimum(self):
        cases = [(0, 1), (-5, 1)]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as handle:
                    json.dump({"skill_antipattern_good_skills_count": raw}, handle)
                    profile_path = handle.name
                try:
                    profile = AppConfig.load(profile_path)
                    self.assertEqual(
                        profile.skill_antipattern_good_skills_count,
                        expected,
                    )
                finally:
                    os.unlink(profile_path)

    def test_load_accepts_integer_float(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as handle:
            json.dump({"skill_antipattern_good_skills_count": 20.0}, handle)
            profile_path = handle.name
        try:
            profile = AppConfig.load(profile_path)
            self.assertEqual(
                profile.skill_antipattern_good_skills_count,
                SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
            )
        finally:
            os.unlink(profile_path)


class AppConfigConstructCoercionTests(unittest.TestCase):
    def test_construct_invalid_uses_default(self):
        cases = [True, False, 2.7, "50"]
        for raw in cases:
            with self.subTest(raw=raw):
                profile = AppConfig(skill_antipattern_good_skills_count=raw)
                self.assertEqual(
                    profile.skill_antipattern_good_skills_count,
                    SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT,
                )

    def test_construct_clamps_out_of_range(self):
        cases = [(0, 1), (500, SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX)]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                profile = AppConfig(skill_antipattern_good_skills_count=raw)
                self.assertEqual(
                    profile.skill_antipattern_good_skills_count,
                    expected,
                )

    def test_construct_preserves_in_range_value(self):
        cases = [(5, 5)]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                profile = AppConfig(skill_antipattern_good_skills_count=raw)
                self.assertEqual(
                    profile.skill_antipattern_good_skills_count,
                    expected,
                )


class SkillAntipatternGoodSkillsCountSaveRoundTripTests(unittest.TestCase):
    def test_save_writes_coerced_int(self):
        cases = [
            ("50", SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT),
            (False, SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_DEFAULT),
            (500, SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX),
            (0, 1),
        ]
        for raw, expected_in_memory in cases:
            with self.subTest(raw=raw):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", delete=False
                ) as handle:
                    json.dump({"skill_antipattern_good_skills_count": raw}, handle)
                    profile_path = handle.name
                try:
                    profile = AppConfig.load(profile_path)
                    self.assertEqual(
                        profile.skill_antipattern_good_skills_count,
                        expected_in_memory,
                    )
                    profile.save(profile_path)
                    with open(profile_path, "r", encoding="utf-8") as handle:
                        saved = json.load(handle)
                    saved_value = saved["skill_antipattern_good_skills_count"]
                    self.assertIsInstance(saved_value, int)
                    self.assertEqual(saved_value, expected_in_memory)
                    self.assertNotEqual(saved_value, raw)
                    if (raw, expected_in_memory) == (0, 1):
                        reloaded = AppConfig.load(profile_path)
                        self.assertEqual(
                            reloaded.skill_antipattern_good_skills_count,
                            1,
                        )
                        with open(profile_path, "r", encoding="utf-8") as handle:
                            re_saved = json.load(handle)
                        self.assertEqual(
                            re_saved["skill_antipattern_good_skills_count"],
                            1,
                        )
                finally:
                    os.unlink(profile_path)


if __name__ == "__main__":
    unittest.main()
