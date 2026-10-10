"""Tests for run_inbox_sync pipeline orchestration."""

import os
import tempfile
import unittest
from unittest.mock import ANY, MagicMock, patch

from spejder.config import AppConfig
from spejder.workflows.gui_sync import GuiSyncContext, run_inbox_sync
from spejder.workflows.profile_learning import ProfileLearningResult
from spejder.workflows.progress_eta import ingest_eta_store_path
from spejder.workflows.skill_hygiene import SkillHygieneResult

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


def _fake_profile_learning(db_path, profile_path, *, on_stage=None):
    if on_stage is not None:
        on_stage("profile_learning", "Learning profile keywords")
    return _PROFILE_LEARNING_EMPTY


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


class RunInboxSyncRebuildTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.rebuild_reasons: list[str] = []
        self.context = GuiSyncContext(
            db_path=os.path.join(self._tmpdir.name, "jobs.db"),
            inbox_path=os.path.join(self._tmpdir.name, "inbox"),
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch("spejder.workflows.gui_sync.rescore_active_jobs", return_value=0)
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
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[{"id": 1}])
    def test_skips_skills_rebuild_when_nothing_updated(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        mock_hygiene,
        mock_profile_learning,
        mock_rescore,
        _portal,
    ):
        reloads: list[int] = []
        context = GuiSyncContext(
            db_path=self.context.db_path,
            inbox_path=self.context.inbox_path,
            model_path="",
            profile_path=self.context.profile_path,
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: reloads.append(1),
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )
        result = run_inbox_sync(context)
        self.assertEqual(result.status, "done")
        mock_hygiene.assert_called_once()
        self.assertEqual(mock_hygiene.call_args.args[0], context.db_path)
        self.assertIs(mock_hygiene.call_args.args[1], context.runtime_profile)
        mock_profile_learning.assert_called_once_with(
            context.db_path,
            context.profile_path,
            on_stage=ANY,
        )
        # Always reload after learning write, even when lists did not change.
        self.assertEqual(len(reloads), 1)
        mock_rescore.assert_not_called()
        self.assertFalse(
            any("skills materialized" in reason for reason in self.rebuild_reasons),
            msg=f"unexpected rebuild reasons: {self.rebuild_reasons}",
        )
        self.assertFalse(
            any("profile keywords learned" in reason for reason in self.rebuild_reasons),
            msg=f"unexpected rebuild reasons: {self.rebuild_reasons}",
        )

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
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
        return_value={
            "considered_positions": 1,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 1,
        },
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[{"id": 1}])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[{"id": 1}])
    def test_queues_skills_rebuild_when_jobs_updated(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        mock_hygiene,
        mock_profile_learning,
        _portal,
    ):
        context = GuiSyncContext(
            db_path=self.context.db_path,
            inbox_path=self.context.inbox_path,
            model_path="",
            profile_path=self.context.profile_path,
            runtime_profile=AppConfig(),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 3,
        )
        result = run_inbox_sync(context)
        self.assertEqual(result.status, "done")
        mock_hygiene.assert_called_once_with(
            context.db_path,
            context.runtime_profile,
            on_stage=ANY,
        )
        mock_profile_learning.assert_called_once()
        self.assertIn("skills materialized=3", self.rebuild_reasons)

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch("spejder.workflows.gui_sync.run_profile_keyword_learning")
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[])
    def test_skips_profile_learning_when_sync_skipped(
        self,
        _desc_refresh,
        mock_profile_learning,
        _portal,
    ):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "skipped")
        mock_profile_learning.assert_not_called()

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch("spejder.workflows.gui_sync.rescore_active_jobs")
    @patch(
        "spejder.workflows.gui_sync.run_profile_keyword_learning",
        return_value=ProfileLearningResult(
            learning_info={
                "labeled_count": 2,
                "learned_include_count": 1,
                "learned_exclude_count": 0,
                "missing_skills_count": 0,
            },
            keywords_changed=True,
            suggestions_changed=False,
        ),
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[{"id": 1}])
    def test_queues_rebuild_when_profile_learning_changes_lists(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        _hygiene,
        mock_profile_learning,
        mock_rescore,
        _portal,
    ):
        # Spy: reload mutates runtime_profile in place (like gui.py setattr);
        # rescore must see those attrs — proves reload-then-rescore order.
        runtime_profile = AppConfig()
        self.assertEqual(runtime_profile.learned_include_keywords, [])

        def _reload() -> None:
            runtime_profile.learned_include_keywords = ["from-reload"]
            runtime_profile.learned_exclude_keywords = ["exclude-from-reload"]

        def _rescore(db_path, profile):
            self.assertIs(profile, runtime_profile)
            self.assertEqual(profile.learned_include_keywords, ["from-reload"])
            self.assertEqual(profile.learned_exclude_keywords, ["exclude-from-reload"])
            return 2

        mock_rescore.side_effect = _rescore
        context = GuiSyncContext(
            db_path=self.context.db_path,
            inbox_path=self.context.inbox_path,
            model_path="",
            profile_path=self.context.profile_path,
            runtime_profile=runtime_profile,
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": self.rebuild_reasons.append(reason),
            reload_runtime_profile=_reload,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
        )
        result = run_inbox_sync(context)
        self.assertEqual(result.status, "done")
        mock_profile_learning.assert_called_once()
        mock_rescore.assert_called_once_with(context.db_path, runtime_profile)
        self.assertIn("profile keywords learned", self.rebuild_reasons)

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    @patch("spejder.workflows.gui_sync.rescore_active_jobs", return_value=0)
    @patch(
        "spejder.workflows.gui_sync.run_profile_keyword_learning",
        return_value=ProfileLearningResult(
            learning_info={
                "labeled_count": 1,
                "learned_include_count": 0,
                "learned_exclude_count": 0,
                "missing_skills_count": 2,
            },
            keywords_changed=False,
            suggestions_changed=True,
        ),
    )
    @patch(
        "spejder.workflows.gui_sync.run_skill_hygiene_stages",
        side_effect=_fake_hygiene,
    )
    @patch(
        "spejder.workflows.gui_sync._learn_skill_patterns_from_positions",
        return_value=_LEARN_EMPTY,
    )
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[{"id": 1}])
    def test_rebuild_without_rescore_when_only_suggestions_change(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        _hygiene,
        mock_profile_learning,
        mock_rescore,
        _portal,
    ):
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        mock_profile_learning.assert_called_once()
        mock_rescore.assert_not_called()
        self.assertIn("profile keywords learned", self.rebuild_reasons)

    @patch(
        "spejder.workflows.gui_sync.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
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
    @patch("spejder.workflows.gui_sync._generate_missing_descriptions_for_ingest", return_value=(0, 0))
    @patch("spejder.workflows.gui_sync.run_cross_source_dedupe", return_value={})
    @patch("spejder.workflows.gui_sync.delete_processed_inbox_files", return_value={})
    @patch("spejder.workflows.gui_sync.get_jobs_for_active_rescore", return_value=[])
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[{"id": 1}])
    def test_patterns_hygiene_profile_learning_order(
        self,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        mock_learn,
        mock_hygiene,
        mock_profile_learning,
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
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        self.assertEqual(order, ["patterns", "hygiene", "profile_learning"])


class RunInboxSyncIngestEtaWiringTest(unittest.TestCase):
    """Focused: gui_sync passes ingest ETA store/status and clears eta_s on finish."""

    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.inbox_path = os.path.join(self._tmpdir.name, "inbox")
        os.makedirs(self.inbox_path)
        self.stage_messages: list[tuple[str, str]] = []
        self.sync_log = MagicMock()
        self.sync_log.closed = False
        self.progress_calls: list[dict] = []

        def _progress(stage, *, checked, total, **metrics):
            self.progress_calls.append(
                {"stage": stage, "checked": checked, "total": total, **metrics}
            )

        self.sync_log.progress.side_effect = _progress
        self.context = GuiSyncContext(
            db_path=self.db_path,
            inbox_path=self.inbox_path,
            model_path="",
            profile_path=os.path.join(self._tmpdir.name, "profile.json"),
            runtime_profile=AppConfig(itday_portal_sync_enabled=False),
            cli_verbose=False,
            queue_dashboard_rebuild=lambda *, reason="": None,
            reload_runtime_profile=lambda: None,
            populate_missing_dashboard_skills=lambda *args, **kwargs: 0,
            on_stage=lambda sid, msg: self.stage_messages.append((sid, msg)),
            sync_log=self.sync_log,
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
    @patch("spejder.workflows.gui_sync.get_jobs_for_description_triage", return_value=[])
    @patch(
        "spejder.workflows.gui_sync.email_parser.load_files",
        return_value=[{"path": "/tmp/one.eml", "html": ""}],
    )
    @patch("spejder.workflows.gui_sync.ingest_docs_to_db")
    def test_ingest_eta_store_status_and_clears_final_eta(
        self,
        mock_ingest,
        _load_files,
        _desc_refresh,
        _active_rescore,
        _delete_files,
        _dedupe,
        _gen_desc,
        _learn,
        _hygiene,
        _profile_learning,
        _portal,
    ):
        def _ingest(_db, _docs, **kwargs):
            on_progress = kwargs["on_progress"]
            on_status = kwargs["on_status_message"]
            self.assertIsNotNone(on_status)
            self.assertEqual(
                kwargs.get("eta_store_path"),
                ingest_eta_store_path(self.db_path),
            )
            on_status("Ingesting 1 inbox file. Estimated time left: about 2 minutes.")
            # Mid-run ETA emit, then skip + clear (counts don't trip tracker).
            # Clear must emit once and advance tracker so needs_final is not a duplicate.
            on_progress(1, 1, 0, 90.0)
            on_progress(2, 1, 1, None)
            return {
                "processed": 2,
                "inserted_new": 1,
                "skipped_existing": 1,
                "positions_by_file": [],
            }

        mock_ingest.side_effect = _ingest
        result = run_inbox_sync(self.context)
        self.assertEqual(result.status, "done")
        mock_ingest.assert_called_once()
        self.assertTrue(
            any(
                sid == "ingest" and "Estimated time left" in msg
                for sid, msg in self.stage_messages
            ),
            msg=f"expected status→on_stage wiring in {self.stage_messages!r}",
        )
        ingest_progress = [
            call for call in self.progress_calls if call.get("stage") == "ingest"
        ]
        self.assertTrue(ingest_progress, msg="expected at least one ingest progress tick")
        with_eta = [c for c in ingest_progress if "eta_s" in c]
        self.assertTrue(with_eta, msg=f"expected mid-run eta_s in {ingest_progress!r}")
        self.assertEqual(with_eta[0]["eta_s"], 90.0)
        # Exactly one clear tick after mid-run ETA (no needs_final duplicate).
        self.assertEqual(
            len(ingest_progress),
            2,
            msg=f"expected mid-run + clear only, got {ingest_progress!r}",
        )
        after_clear = ingest_progress[ingest_progress.index(with_eta[0]) + 1 :]
        self.assertEqual(len(after_clear), 1)
        for call in after_clear:
            self.assertNotIn("eta_s", call, msg=f"stale eta after clear: {call!r}")
            self.assertNotIn("eta", call, msg=f"stale eta after clear: {call!r}")
            self.assertEqual(call.get("checked"), 2)
            self.assertEqual(call.get("total"), 0)


if __name__ == "__main__":
    unittest.main()
