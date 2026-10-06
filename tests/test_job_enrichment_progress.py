"""Regression: skills progress ignores cache skips; descriptions tick every selected row.

Also covers live mid-batch active-rescore scope re-checks.
"""

import io
import json
import os
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

from spejder.db import (
    delete_jobs,
    ensure_db,
    set_job_applied,
    set_job_hidden,
    set_job_viewed,
    upsert_job,
)
from spejder.db.connection import _connect
from spejder.workflows.job_descriptions import _generate_missing_descriptions_for_ingest
from spejder.workflows.job_skills_materialize import materialize_jobs_skills
from spejder.workflows.progress_eta import (
    descriptions_eta_store_path,
    skills_eta_store_path,
)

_IN_SCOPE_FLAGS = {
    "viewed": 0,
    "applied": 0,
    "on_interview": 0,
    "interview_stopped": 0,
}


def _insert_job(db_path: str, link: str, *, title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
            "position_link": link,
            "raw_text": "Requires python and docker for backend work.",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


class MaterializeJobsSkillsProgressTest(unittest.TestCase):
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch(
        "spejder.db.get_job_skills_for_jobs",
        return_value={1: ["Python"], 2: ["Python"], 3: ["Python"]},
    )
    def test_on_progress_not_called_when_skip_cached(
        self, _mock_get_skills, mock_materialize
    ):
        rows = [{"id": i} for i in range(1, 4)]
        recorded: list[tuple[int, int, int]] = []

        updated = materialize_jobs_skills(
            "unused.db",
            rows,
            skip_cached=True,
            on_progress=lambda checked, total, upd: recorded.append(
                (checked, total, upd)
            ),
        )

        self.assertEqual(updated, 0)
        mock_materialize.assert_not_called()
        self.assertEqual(recorded, [])

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch(
        "spejder.db.get_job_skills_for_jobs",
        return_value={1: ["Python"], 2: ["Python"], 3: ["Python"]},
    )
    def test_skip_cached_does_not_print_progress_label(
        self, _mock_get_skills, mock_materialize
    ):
        rows = [{"id": i} for i in range(1, 4)]
        buf = io.StringIO()
        with redirect_stdout(buf):
            materialize_jobs_skills(
                "unused.db",
                rows,
                skip_cached=True,
                progress_label="Background sync: skills",
            )
        mock_materialize.assert_not_called()
        self.assertEqual(buf.getvalue(), "")

    @patch(
        "spejder.workflows.job_skills_materialize.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_missing_or_non_positive_id_dropped_before_slow_loop(
        self, _mock_get_skills, mock_materialize, _mock_flags
    ):
        mock_materialize.return_value = ("Python", "raw", True)
        recorded: list[tuple[int, int, int]] = []

        with tempfile.TemporaryDirectory() as tmp:
            updated = materialize_jobs_skills(
                os.path.join(tmp, "jobs.db"),
                [{}, {"id": 0}, {"id": 1}],
                on_progress=lambda checked, total, upd: recorded.append(
                    (checked, total, upd)
                ),
            )

        self.assertEqual(updated, 1)
        self.assertEqual(mock_materialize.call_count, 1)
        self.assertEqual(mock_materialize.call_args.args[1]["id"], 1)
        self.assertEqual(recorded, [(1, 1, 1)])


class GenerateMissingDescriptionsProgressTest(unittest.TestCase):
    @patch(
        "spejder.workflows.job_descriptions.get_job_scope_flags",
        return_value=_IN_SCOPE_FLAGS,
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="",
    )
    @patch("spejder.workflows.job_descriptions.get_jobs_for_description_triage")
    def test_on_progress_fires_at_total_when_rows_continue_early(
        self, mock_refresh, _mock_enrich, _mock_flags
    ):
        mock_refresh.return_value = [
            {"id": 1, "raw_text": ""},
            {"id": 2, "raw_text": ""},
            {"id": 3, "raw_text": ""},
        ]
        recorded: list[tuple[int, int, int]] = []

        with tempfile.TemporaryDirectory() as tmp:
            updated, skipped = _generate_missing_descriptions_for_ingest(
                os.path.join(tmp, "jobs.db"),
                on_progress=lambda checked, total, upd: recorded.append(
                    (checked, total, upd)
                ),
            )

        self.assertEqual(updated, 0)
        self.assertEqual(skipped, 3)
        self.assertEqual(recorded, [(1, 3, 0), (2, 3, 0), (3, 3, 0)])


class LiveScopeRecheckSkillsTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_mid_batch_viewed_skips_expensive_work(
        self, _mock_get_skills, mock_materialize
    ):
        job_a = _insert_job(self.db_path, "https://example.com/a", title="A")
        job_b = _insert_job(self.db_path, "https://example.com/b", title="B")
        rows = [{"id": job_a}, {"id": job_b}]
        progress: list[tuple[int, int, int]] = []
        status_messages: list[str] = []

        def materialize_side_effect(db_path, row, **_kwargs):
            if int(row["id"]) == job_a:
                set_job_viewed(db_path, job_b, True)
            time.sleep(0.01)
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect
        updated = materialize_jobs_skills(
            self.db_path,
            rows,
            on_progress=lambda checked, total, upd: progress.append(
                (checked, total, upd)
            ),
            on_status_message=status_messages.append,
        )
        self.assertEqual(updated, 1)
        self.assertEqual(mock_materialize.call_count, 1)
        self.assertEqual(mock_materialize.call_args.args[1]["id"], job_a)
        self.assertEqual(progress, [(1, 2, 1), (2, 2, 1)])
        self.assertGreaterEqual(len(status_messages), 3)
        self.assertTrue(all(isinstance(msg, str) and msg for msg in status_messages))

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_hidden_unviewed_still_materialized(
        self, _mock_get_skills, mock_materialize
    ):
        mock_materialize.return_value = ("Python", "raw", True)
        job_id = _insert_job(self.db_path, "https://example.com/hidden")
        set_job_hidden(self.db_path, job_id, True)
        updated = materialize_jobs_skills(self.db_path, [{"id": job_id}])
        self.assertEqual(updated, 1)
        mock_materialize.assert_called_once()

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_viewed_applied_still_materialized(
        self, _mock_get_skills, mock_materialize
    ):
        mock_materialize.return_value = ("Python", "raw", True)
        job_id = _insert_job(self.db_path, "https://example.com/applied")
        set_job_viewed(self.db_path, job_id, True)
        set_job_applied(self.db_path, job_id, True)
        updated = materialize_jobs_skills(self.db_path, [{"id": job_id}])
        self.assertEqual(updated, 1)
        mock_materialize.assert_called_once()

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_status_skip_does_not_pollute_skills_eta(
        self, _mock_get_skills, mock_materialize
    ):
        job_a = _insert_job(self.db_path, "https://example.com/eta-a", title="EtaA")
        job_b = _insert_job(self.db_path, "https://example.com/eta-b", title="EtaB")
        store = skills_eta_store_path(self.db_path)

        def materialize_side_effect(db_path, row, **_kwargs):
            if int(row["id"]) == job_a:
                set_job_viewed(db_path, job_b, True)
            time.sleep(0.02)
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect
        materialize_jobs_skills(
            self.db_path,
            [{"id": job_a}, {"id": job_b}],
            eta_store_path=store,
        )
        with open(store, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(len(data["samples"]), 1)
        self.assertGreaterEqual(data["samples"][0], 0.01)
        self.assertLess(data["samples"][0], 1.0)

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_mid_batch_deleted_skips_expensive_work(
        self, _mock_get_skills, mock_materialize
    ):
        job_a = _insert_job(self.db_path, "https://example.com/del-a", title="DelA")
        job_b = _insert_job(self.db_path, "https://example.com/del-b", title="DelB")
        store = skills_eta_store_path(self.db_path)

        def materialize_side_effect(db_path, row, **_kwargs):
            if int(row["id"]) == job_a:
                delete_jobs(db_path, [job_b])
            time.sleep(0.02)
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect
        updated = materialize_jobs_skills(
            self.db_path,
            [{"id": job_a}, {"id": job_b}],
            eta_store_path=store,
        )
        self.assertEqual(updated, 1)
        self.assertEqual(mock_materialize.call_count, 1)
        self.assertEqual(mock_materialize.call_args.args[1]["id"], job_a)
        with open(store, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(len(data["samples"]), 1)


class LiveScopeRecheckDescriptionsTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.job_descriptions._get_title_english_for_row",
        return_value="Engineer",
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="enriched page text for description",
    )
    @patch(
        "spejder.workflows.job_descriptions._build_description_summary",
        return_value="A solid backend role needing python and docker.",
    )
    def test_mid_batch_viewed_skips_page_and_llm(
        self, mock_build, mock_enrich, _mock_title
    ):
        job_a = _insert_job(self.db_path, "https://example.com/desc-a", title="DescA")
        job_b = _insert_job(self.db_path, "https://example.com/desc-b", title="DescB")
        other_by_id = {job_a: job_b, job_b: job_a}
        progress: list[tuple[int, int, int]] = []
        status_messages: list[str] = []

        def enrich_side_effect(db_path, row, **_kwargs):
            set_job_viewed(db_path, other_by_id[int(row["id"])], True)
            return "enriched page text for description"

        mock_enrich.side_effect = enrich_side_effect
        updated, skipped = _generate_missing_descriptions_for_ingest(
            self.db_path,
            on_progress=lambda checked, total, upd: progress.append(
                (checked, total, upd)
            ),
            on_status_message=status_messages.append,
        )
        self.assertEqual(updated, 1)
        self.assertEqual(skipped, 1)
        self.assertEqual(mock_enrich.call_count, 1)
        self.assertEqual(mock_build.call_count, 1)
        self.assertEqual(progress, [(1, 2, 1), (2, 2, 1)])
        self.assertGreaterEqual(len(status_messages), 3)
        self.assertTrue(all(isinstance(msg, str) and msg for msg in status_messages))

    @patch(
        "spejder.workflows.job_descriptions._get_title_english_for_row",
        return_value="Engineer",
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="enriched page text for description",
    )
    @patch(
        "spejder.workflows.job_descriptions._build_description_summary",
        return_value="A solid backend role needing python and docker.",
    )
    def test_hidden_unviewed_still_generates(
        self, mock_build, mock_enrich, _mock_title
    ):
        job_id = _insert_job(self.db_path, "https://example.com/desc-hidden")
        set_job_hidden(self.db_path, job_id, True)
        updated, skipped = _generate_missing_descriptions_for_ingest(self.db_path)
        self.assertEqual(updated, 1)
        self.assertEqual(skipped, 0)
        mock_enrich.assert_called_once()
        mock_build.assert_called_once()

    @patch(
        "spejder.workflows.job_descriptions._get_title_english_for_row",
        return_value="Engineer",
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="enriched page text for description",
    )
    @patch(
        "spejder.workflows.job_descriptions._build_description_summary",
        return_value="A solid backend role needing python and docker.",
    )
    def test_status_skip_does_not_pollute_descriptions_eta(
        self, mock_build, mock_enrich, _mock_title
    ):
        job_a = _insert_job(
            self.db_path, "https://example.com/desc-eta-a", title="DescEtaA"
        )
        job_b = _insert_job(
            self.db_path, "https://example.com/desc-eta-b", title="DescEtaB"
        )
        other_by_id = {job_a: job_b, job_b: job_a}
        store = descriptions_eta_store_path(self.db_path)

        def enrich_side_effect(db_path, row, **_kwargs):
            set_job_viewed(db_path, other_by_id[int(row["id"])], True)
            time.sleep(0.02)
            return "enriched page text for description"

        mock_enrich.side_effect = enrich_side_effect
        _generate_missing_descriptions_for_ingest(
            self.db_path,
            eta_store_path=store,
        )
        with open(store, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(len(data["samples"]), 1)
        self.assertGreaterEqual(data["samples"][0], 0.01)
        self.assertLess(data["samples"][0], 1.0)
        self.assertEqual(mock_build.call_count, 1)

    @patch(
        "spejder.workflows.job_descriptions._get_title_english_for_row",
        return_value="Engineer",
    )
    @patch(
        "spejder.workflows.job_descriptions._enrich_raw_text_with_position_page",
        return_value="enriched page text for description",
    )
    @patch(
        "spejder.workflows.job_descriptions._build_description_summary",
        return_value="A solid backend role needing python and docker.",
    )
    def test_mid_batch_deleted_skips_page_and_llm(
        self, mock_build, mock_enrich, _mock_title
    ):
        job_a = _insert_job(
            self.db_path, "https://example.com/desc-del-a", title="DescDelA"
        )
        job_b = _insert_job(
            self.db_path, "https://example.com/desc-del-b", title="DescDelB"
        )
        other_by_id = {job_a: job_b, job_b: job_a}
        store = descriptions_eta_store_path(self.db_path)

        def enrich_side_effect(db_path, row, **_kwargs):
            delete_jobs(db_path, [other_by_id[int(row["id"])]])
            time.sleep(0.02)
            return "enriched page text for description"

        mock_enrich.side_effect = enrich_side_effect
        updated, skipped = _generate_missing_descriptions_for_ingest(
            self.db_path,
            eta_store_path=store,
        )
        self.assertEqual(updated, 1)
        self.assertEqual(skipped, 1)
        self.assertEqual(mock_enrich.call_count, 1)
        self.assertEqual(mock_build.call_count, 1)
        with open(store, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(len(data["samples"]), 1)


if __name__ == "__main__":
    unittest.main()
