"""Tests that skill learning includes parked Hidden relevant jobs."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.learning import (
    _learn_skill_patterns_from_positions,
)


class SkillLearningHiddenTest(unittest.TestCase):
    @patch(
        "spejder.extractors.skill_extractor.learning._get_skill_patterns",
        return_value=[],
    )
    @patch(
        "spejder.extractors.skill_extractor.learning.get_jobs_by_category",
        return_value=[],
    )
    @patch(
        "spejder.extractors.skill_extractor.learning.get_all_applied_jobs",
        return_value=[],
    )
    def test_learn_passes_exclude_hidden_false(
        self, _mock_applied, mock_by_category, _mock_patterns
    ):
        _learn_skill_patterns_from_positions("/tmp/unused.db", AppConfig())

        mock_by_category.assert_called_once()
        _args, kwargs = mock_by_category.call_args
        self.assertEqual(_args[1], "relevant")
        self.assertEqual(kwargs.get("exclude_hidden"), False)
        self.assertEqual(kwargs.get("unviewed_only"), False)
        self.assertEqual(kwargs.get("limit"), 0)


if __name__ == "__main__":
    unittest.main()
