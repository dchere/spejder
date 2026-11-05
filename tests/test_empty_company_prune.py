"""Tests for shared late-sync empty-company prune stage."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from spejder.config import AppConfig
from spejder.db import ensure_db, upsert_job
from spejder.db.companies import company_alias_key, ensure_companies_from_jobs
from spejder.db.connection import _connect
from spejder.workflows.company_catalog import run_empty_company_prune
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync
from spejder.workflows.inbox_workflow import process_inbox
from spejder.workflows.profile_learning import ProfileLearningResult
from spejder.workflows.skill_hygiene import SkillHygieneResult
from spejder.workflows.sync_log import SyncRunLog

_REAL_SYNC_RUN_LOG_OPEN = SyncRunLog.open.__func__

_LEARN_EMPTY = {
    "considered_positions": 0,
    "new_skill_patterns": 0,
    "total_known_skill_patterns": 0,
}

_PROFILE_LEARNING_EMPTY = ProfileLearningResult(
    learning_info={
        "labeled_count": 0,
        "learned_include_count": 0,
        "learned_exclude_count": 0,
        "missing_skills_count": 0,
    },
    keywords_changed=False,
    suggestions_changed=False,
)

_HYGIENE_EMPTY = SkillHygieneResult(
    blocked_cleanup={
        "skills_processed": 0,
        "skill_rows_deleted": 0,
        "job_skill_links_deleted": 0,
        "affected_job_ids": [],
    },
    blocked_rescored=0,
    stale_cleanup={
        "skills_deleted": 0,
        "skill_rows_deleted": 0,
        "job_skill_links_deleted": 0,
        "affected_job_ids": [],
        "profile_removed": 0,
        "deleted_skill_names": [],
    },
    stale_rescored=0,
    cloud_stats={"seeded": False, "pruned": []},
    new_threshold=0.1,
    previous_threshold=None,
    threshold_changed=False,
)


def _fake_hygiene(db_path, profile, *, on_stage=None):
    if on_stage is not None:
        on_stage("blocked_skills", "Cleaning blocked skills from database")
        on_stage("stale_skills", "Cleaning stale low-share skills")
        on_stage("bad_cloud", "Initializing bad cloud")
    return _HYGIENE_EMPTY


def _fake_profile_learning(db_path, profile_path, *, on_stage=None):
    if on_stage is not None:
        on_stage("profile_learning", "Learning profile keywords")
    return _PROFILE_LEARNING_EMPTY


def _insert_empty_company(db_path: str, display_name: str) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO companies (display_name, created_at, updated_at) "
            "VALUES (?, datetime('now'), datetime('now'))",
            (display_name,),
        )
        cid = int(cur.lastrowid)
        cur.execute(
            "INSERT INTO company_aliases (company_id, alias_raw, alias_key, created_at) "
            "VALUES (?, ?, ?, datetime('now'))",
            (cid, display_name, company_alias_key(display_name)),
        )
        conn.commit()
        return cid
    finally:
        conn.close()


def _company_exists(db_path: str, company_id: int) -> bool:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM companies WHERE id=?", (company_id,))
        return cur.fetchone() is not None
    finally:
        conn.close()


def _open_sync_log_quiet(path: str, *, echo_stdout: bool = True, **kwargs):
    return _REAL_SYNC_RUN_LOG_OPEN(SyncRunLog, path, echo_stdout=False, **kwargs)


class RunEmptyCompanyPruneTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_prunes_zero_job_companies_keeps_with_jobs_idempotent_stage_once(self):
        empty_id = _insert_empty_company(self.db_path, "EmptyCo")
        upsert_job(
            self.db_path,
            {
                "source": "Test",
                "company": "KeptCo",
                "title": "Engineer",
                "position_link": "https://example.com/kept",
                "raw_text": "raw",
            },
        )
        ensure_companies_from_jobs(self.db_path)
        kept_id = None
        conn = _connect(self.db_path)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT c.id FROM companies c "
                "JOIN company_aliases a ON a.company_id=c.id "
                "WHERE a.alias_key=?",
                (company_alias_key("KeptCo"),),
            )
            kept_id = int(cur.fetchone()[0])
        finally:
            conn.close()

        stages: list[tuple[str, str]] = []
        first = run_empty_company_prune(
            self.db_path,
            on_stage=lambda sid, msg: stages.append((sid, msg)),
        )
        self.assertEqual(stages, [("empty_companies", "Pruning empty companies")])
        self.assertEqual(first, [empty_id])
        self.assertFalse(_company_exists(self.db_path, empty_id))
        self.assertTrue(_company_exists(self.db_path, kept_id))

        stages.clear()
        second = run_empty_company_prune(
            self.db_path,
            on_stage=lambda sid, msg: stages.append((sid, msg)),
        )
        self.assertEqual(stages, [("empty_companies", "Pruning empty companies")])
        self.assertEqual(second, [])
        self.assertTrue(_company_exists(self.db_path, kept_id))


class GuiSyncEmptyCompanyPruneOrderTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.rebuild_reasons: list[str] = []
        db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(db_path)
        self.context = GuiSyncContext(
            db_path=db_path,
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(
                reason
            ),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    @patch("spejder.workflows.gui_sync.run_empty_company_prune", return_value=[])
    @patch(
        "spejder.workflows.gui_sync.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch(
        "spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch(
        "spejder.workflows.gui_sync.get_jobs_for_description_triage",
        return_value=[{"id": 1}],
    )
    def test_patterns_hygiene_profile_learning_then_empty_prune_order(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        mock_learn,
        mock_hygiene,
        mock_profile_learning,
        mock_prune,
        _portal,
    ):
        order: list[str] = []
        mock_learn.side_effect = lambda *a, **k: (
            order.append("patterns"),
            _LEARN_EMPTY,
        )[1]
        mock_hygiene.side_effect = lambda *a, **k: (
            order.append("hygiene"),
            _HYGIENE_EMPTY,
        )[1]
        mock_profile_learning.side_effect = lambda *a, **k: (
            order.append("profile_learning"),
            _PROFILE_LEARNING_EMPTY,
        )[1]
        mock_prune.side_effect = lambda *a, **k: (
            order.append("empty_companies"),
            [],
        )[1]
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        self.assertEqual(
            order,
            ["patterns", "hygiene", "profile_learning", "empty_companies"],
        )

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    @patch("spejder.workflows.gui_sync.run_empty_company_prune")
    @patch(
        "spejder.workflows.gui_sync.get_jobs_for_description_triage",
        return_value=[],
    )
    def test_skips_empty_prune_when_sync_skipped(
        self,
        _desc_refresh,
        mock_prune,
        _portal,
    ):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "skipped")
        mock_prune.assert_not_called()

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    @patch(
        "spejder.workflows.gui_sync.run_empty_company_prune",
        return_value=[42, 43],
    )
    @patch(
        "spejder.workflows.gui_sync.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch(
        "spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch(
        "spejder.workflows.gui_sync.get_jobs_for_description_triage",
        return_value=[{"id": 1}],
    )
    def test_queues_rebuild_when_empty_companies_pruned(self, *_mocks):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        self.assertIn("empty companies pruned (count=2)", self.rebuild_reasons)


class ProcessInboxEmptyCompanyPruneOrderTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.inbox = os.path.join(self._tmpdir.name, "inbox")
        os.makedirs(self.inbox)
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir)
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        with open(self.profile_path, "w", encoding="utf-8") as handle:
            handle.write('{"itday_portal_sync_enabled": false}')
        self._echo_patcher = patch(
            "spejder.workflows.inbox_workflow.SyncRunLog.open",
            side_effect=_open_sync_log_quiet,
        )
        self._echo_patcher.start()
        self.addCleanup(self._echo_patcher.stop)

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.inbox_workflow.write_inbox_dashboard_report",
        return_value=None,
    )
    @patch(
        "spejder.workflows.inbox_workflow.summarize_relevant_jobs_for_inbox",
        return_value=[],
    )
    @patch("spejder.workflows.inbox_workflow.run_empty_company_prune", return_value=[])
    @patch(
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.inbox_workflow._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch("spejder.workflows.inbox_workflow.get_relevant_jobs", return_value=[])
    @patch("spejder.workflows.inbox_workflow.materialize_relevant_and_applied_skills")
    @patch(
        "spejder.workflows.inbox_workflow._generate_missing_descriptions_for_ingest",
        return_value=(0, 0),
    )
    @patch(
        "spejder.workflows.inbox_workflow.delete_processed_inbox_files",
        return_value={"eligible": 0, "deleted": 0, "missing": 0, "failed": 0},
    )
    @patch("spejder.workflows.inbox_workflow.print_ingest_file_stats")
    @patch(
        "spejder.workflows.inbox_workflow.ingest_docs_to_db",
        return_value={
            "processed": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "files": [],
        },
    )
    @patch(
        "spejder.workflows.inbox_workflow.LocalLLM",
        return_value=MagicMock(),
    )
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
        return_value=[{"id": 1}],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    def test_patterns_hygiene_profile_learning_then_empty_prune_order(
        self,
        _portal,
        _desc_refresh,
        _llm,
        _ingest,
        _print_stats,
        _delete_files,
        _gen_desc,
        _materialize,
        _relevant,
        mock_learn,
        mock_hygiene,
        mock_profile_learning,
        mock_prune,
        _summarize,
        _report,
    ):
        order: list[str] = []
        mock_learn.side_effect = lambda *a, **k: (
            order.append("patterns"),
            _LEARN_EMPTY,
        )[1]
        mock_hygiene.side_effect = lambda *a, **k: (
            order.append("hygiene"),
            _HYGIENE_EMPTY,
        )[1]
        mock_profile_learning.side_effect = lambda *a, **k: (
            order.append("profile_learning"),
            _PROFILE_LEARNING_EMPTY,
        )[1]
        mock_prune.side_effect = lambda *a, **k: (
            order.append("empty_companies"),
            [],
        )[1]
        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="/fake/model.gguf",
        )
        self.assertEqual(
            order,
            ["patterns", "hygiene", "profile_learning", "empty_companies"],
        )

    @patch(
        "spejder.workflows.inbox_workflow.run_empty_company_prune",
    )
    @patch(
        "spejder.workflows.inbox_workflow.run_profile_keyword_learning",
        side_effect=_fake_profile_learning,
    )
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_triage",
        return_value=[],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={
            "found": 0,
            "inserted_new": 0,
            "skipped_existing": 0,
            "processed": 0,
        },
    )
    def test_skips_empty_prune_when_inbox_skipped(
        self, _portal, _desc_refresh, mock_profile_learning, mock_prune
    ):
        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            report_dir=self.report_dir,
            model="",
        )
        mock_profile_learning.assert_not_called()
        mock_prune.assert_not_called()


if __name__ == "__main__":
    unittest.main()
