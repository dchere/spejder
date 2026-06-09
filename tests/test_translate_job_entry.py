"""Tests for make_translate_job_entry_for_storage."""

import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.workflows.job_enrichment import make_translate_job_entry_for_storage


def _make_transform():
    return make_translate_job_entry_for_storage(AppConfig(), {}, {})


class TranslateJobEntryForStorageTest(unittest.TestCase):
    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_happy_path_both_translators_succeed(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_text.return_value = "EN body"
        mock_translate_title.return_value = "EN Title"
        mock_finalize_title.return_value = "EN Title Final"
        mock_normalize_key.side_effect = ["entitlefinal", "engineer"]
        entry = {"title": "Engineer", "raw_text": "Hej verden"}
        original_entry = dict(entry)

        result = _make_transform()(entry)

        self.assertEqual(result["raw_text"], "EN body")
        self.assertEqual(result["title_english"], "EN Title Final")
        self.assertEqual(entry, original_entry)

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_title_translator_raises_falls_back_to_text_translator(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_title.side_effect = Exception("title translation failed")
        mock_translate_text.side_effect = ["EN body", "EN Title Fallback"]
        mock_finalize_title.return_value = "EN Title Fallback"
        mock_normalize_key.side_effect = ["entitlefallback", "engineer"]
        entry = {"title": "Engineer", "raw_text": "Hej verden"}

        result = _make_transform()(entry)

        self.assertEqual(result["raw_text"], "EN body")
        self.assertEqual(result["title_english"], "EN Title Fallback")

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_both_title_translators_raise_uses_raw_title_value(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_title.side_effect = Exception("title translator failed")
        mock_translate_text.side_effect = ["EN body", Exception("text translator failed")]
        mock_finalize_title.return_value = "Software Engineer"
        mock_normalize_key.side_effect = ["softwareengineerx", "softwareengineer"]
        entry = {"title": "Software Engineer", "raw_text": "Hej verden"}

        result = _make_transform()(entry)

        self.assertEqual(result["title_english"], "Software Engineer")
        mock_finalize_title.assert_called_once_with("Software Engineer", "Software Engineer")

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_normalize_match_blanks_title_english(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_text.return_value = "EN body"
        mock_translate_title.return_value = "Engineer"
        mock_finalize_title.return_value = "Engineer"
        mock_normalize_key.side_effect = ["samekey", "samekey"]
        entry = {"title": "Engineer", "raw_text": "Hej verden"}

        result = _make_transform()(entry)

        self.assertEqual(result["title_english"], "")

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_entry_copy_does_not_mutate_original(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_text.return_value = "EN body"
        mock_translate_title.return_value = "EN Title"
        mock_finalize_title.return_value = "EN Title Final"
        mock_normalize_key.side_effect = ["entitlefinal", "engineer"]
        entry = {"id": 1, "title": "Engineer", "raw_text": "Hej verden"}
        original_entry = dict(entry)

        result = _make_transform()(entry)
        result["raw_text"] = "CHANGED"
        result["title_english"] = "CHANGED"

        self.assertEqual(entry, original_entry)
        self.assertNotEqual(result, entry)

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_missing_raw_text_and_title_keys_handled_gracefully(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        mock_translate_text.return_value = ""
        mock_translate_title.return_value = ""
        mock_finalize_title.return_value = ""
        mock_normalize_key.side_effect = ["", ""]
        entry = {"id": 1}

        result = _make_transform()(entry)

        self.assertEqual(result["raw_text"], "")
        self.assertEqual(result["title_english"], "")

    @patch("spejder.workflows.job_translation._normalize_title_compare_key")
    @patch("spejder.workflows.job_translation._finalize_title_english")
    @patch("spejder.workflows.job_translation._translate_title_to_english")
    @patch("spejder.workflows.job_translation._translate_text_to_english_if_needed")
    def test_caches_forwarded_to_correct_translators(
        self,
        mock_translate_text,
        mock_translate_title,
        mock_finalize_title,
        mock_normalize_key,
    ):
        text_cache = {"text": "cache"}
        title_cache = {"title": "cache"}
        transform = make_translate_job_entry_for_storage(AppConfig(), text_cache, title_cache)
        mock_translate_text.return_value = "EN body"
        mock_translate_title.return_value = "EN Title"
        mock_finalize_title.return_value = "EN Title Final"
        mock_normalize_key.side_effect = ["entitlefinal", "engineer"]
        entry = {"title": "Engineer", "raw_text": "Hej verden"}

        transform(entry)

        raw_text_call = mock_translate_text.call_args_list[0]
        self.assertEqual(raw_text_call.kwargs["translation_cache"], text_cache)
        title_call = mock_translate_title.call_args
        self.assertEqual(title_call.kwargs["title_translation_cache"], title_cache)


if __name__ == "__main__":
    unittest.main()
