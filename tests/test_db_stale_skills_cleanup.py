"""Tests for cleanup_stale_low_share_skills_from_db and skill hygiene."""

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import (
    cleanup_stale_low_share_skills_from_db,
    ensure_db,
    get_job_skills,
    get_skill_patterns,
    position_pct,
    set_job_skills,
    upsert_job,
    upsert_skill_pattern,
)
from spejder.db.connection import _connect
from spejder.db.skills_cleanup import SKILL_STALE_JOB_SHARE_PCT_THRESHOLD
from spejder.tests.skill_test_utils import stamp_skill_patterns_created_at
from spejder.workflows.inbox_workflow import process_inbox
from spejder.workflows.skill_hygiene import run_stale_skill_cleanup


def _insert_job(db_path: str, link: str, title: str = "Engineer") -> int:
    upsert_job(
        db_path,
        {
            "source": "Test",
            "company": "Acme",
            "title": title,
            "position_link": link,
            "raw_text": "raw",
        },
    )
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE position_link=?", (link,))
        row = cur.fetchone()
        if row is None:
            raise AssertionError(f"job not found for link={link!r}")
        return int(row[0])
    finally:
        conn.close()


def _old_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=120)).isoformat()


def _young_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()


class StaleSkillsCleanupTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_deletes_old_unflagged_zero_share(self):
        upsert_skill_pattern(
            self.db_path, name="Obsolete", pattern=r"\bObsolete\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="Python", pattern=r"\bPython\b", source="test"
        )
        stamp_skill_patterns_created_at(
            self.db_path,
            {"obsolete": _old_iso(), "python": _old_iso()},
        )
        job_id = _insert_job(self.db_path, "https://example.com/py-job")
        set_job_skills(self.db_path, job_id, ["Python"])

        stats = cleanup_stale_low_share_skills_from_db(self.db_path, protected_keys=set())

        self.assertEqual(stats["skills_deleted"], 1)
        self.assertIn("Obsolete", stats["deleted_skill_names"])
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("Obsolete", names)
        self.assertIn("Python", names)
        self.assertEqual(get_job_skills(self.db_path, job_id), ["Python"])

    def test_keeps_flagged_skills(self):
        upsert_skill_pattern(
            self.db_path, name="Flagged", pattern=r"\bFlagged\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"flagged": _old_iso()})

        stats = cleanup_stale_low_share_skills_from_db(
            self.db_path, protected_keys={"flagged"}
        )
        self.assertEqual(stats["skills_deleted"], 0)
        names = {
            row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)
        }
        self.assertIn("Flagged", names)

    def test_keeps_young_and_unparseable_created_at(self):
        upsert_skill_pattern(
            self.db_path, name="Young", pattern=r"\bYoung\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="BadDate", pattern=r"\bBadDate\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="EmptyDate", pattern=r"\bEmptyDate\b", source="test"
        )
        stamp_skill_patterns_created_at(
            self.db_path,
            {
                "young": _young_iso(),
                "baddate": "not-a-date",
                "emptydate": "",
            },
        )

        stats = cleanup_stale_low_share_skills_from_db(self.db_path, protected_keys=set())
        self.assertEqual(stats["skills_deleted"], 0)
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertEqual(names, {"Young", "BadDate", "EmptyDate"})

    def test_rounding_boundary_keeps_exactly_zero_point_one(self):
        # 1 / 1000 → round(..., 1) == 0.1 → keep
        upsert_skill_pattern(
            self.db_path, name="Boundary", pattern=r"\bBoundary\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="Filler", pattern=r"\bFiller\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"boundary": _old_iso()})
        for i in range(1000):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/boundary-{i}",
                title=f"Engineer {i}",
            )
            if i == 0:
                set_job_skills(self.db_path, job_id, ["Boundary", "Filler"])
            else:
                set_job_skills(self.db_path, job_id, ["Filler"])

        self.assertEqual(position_pct(1, 1000), 0.1)
        self.assertGreaterEqual(
            position_pct(1, 1000), SKILL_STALE_JOB_SHARE_PCT_THRESHOLD
        )

        stats = cleanup_stale_low_share_skills_from_db(self.db_path, protected_keys=set())
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertIn("Boundary", names)
        self.assertNotIn("Boundary", stats["deleted_skill_names"])

    def test_deletes_old_nonzero_share_that_rounds_below_threshold(self):
        # 1 / 2001 → round(..., 1) == 0.0 (< 0.1) with position_count >= 1 → delete
        denom = 2001
        upsert_skill_pattern(
            self.db_path, name="Rare", pattern=r"\bRare\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="Filler", pattern=r"\bFiller\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"rare": _old_iso()})
        for i in range(denom):
            job_id = _insert_job(
                self.db_path,
                f"https://example.com/rare-{i}",
                title=f"Engineer {i}",
            )
            if i == 0:
                set_job_skills(self.db_path, job_id, ["Rare", "Filler"])
            else:
                set_job_skills(self.db_path, job_id, ["Filler"])

        self.assertEqual(position_pct(1, denom), 0.0)
        self.assertLess(position_pct(1, denom), SKILL_STALE_JOB_SHARE_PCT_THRESHOLD)

        stats = cleanup_stale_low_share_skills_from_db(self.db_path, protected_keys=set())
        self.assertIn("Rare", stats["deleted_skill_names"])
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("Rare", names)

    def test_rounding_zero_share_deletes_when_old(self):
        upsert_skill_pattern(
            self.db_path, name="ZeroShare", pattern=r"\bZeroShare\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="Anchor", pattern=r"\bAnchor\b", source="test"
        )
        stamp_skill_patterns_created_at(
            self.db_path,
            {"zeroshare": _old_iso(), "anchor": _old_iso()},
        )
        job_id = _insert_job(self.db_path, "https://example.com/anchor")
        set_job_skills(self.db_path, job_id, ["Anchor"])
        self.assertEqual(position_pct(0, 1), 0.0)

        stats = cleanup_stale_low_share_skills_from_db(self.db_path, protected_keys=set())
        self.assertIn("ZeroShare", stats["deleted_skill_names"])
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertNotIn("ZeroShare", names)
        self.assertIn("Anchor", names)

    def test_does_not_add_to_blocked_skills_via_hygiene(self):
        upsert_skill_pattern(
            self.db_path, name="Gone", pattern=r"\bGone\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"gone": _old_iso()})
        profile = AppConfig(
            known_skill_patterns=[{"name": "Gone", "pattern": r"\bGone\b"}],
            blocked_skills=[],
        )

        stats = run_stale_skill_cleanup(self.db_path, profile)

        self.assertEqual(stats["skills_deleted"], 1)
        self.assertGreaterEqual(stats["profile_removed"], 1)
        self.assertEqual(profile.blocked_skills, [])
        self.assertEqual(profile.known_skill_patterns, [])

    def test_hygiene_protects_user_skills(self):
        upsert_skill_pattern(
            self.db_path, name="KeepMe", pattern=r"\bKeepMe\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"keepme": _old_iso()})
        profile = AppConfig(user_skills=["KeepMe"])
        stats = run_stale_skill_cleanup(self.db_path, profile)
        self.assertEqual(stats["skills_deleted"], 0)
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertIn("KeepMe", names)

    def test_hygiene_protects_blocked_skills(self):
        upsert_skill_pattern(
            self.db_path, name="KeepBlocked", pattern=r"\bKeepBlocked\b", source="test"
        )
        stamp_skill_patterns_created_at(self.db_path, {"keepblocked": _old_iso()})
        profile = AppConfig(blocked_skills=["KeepBlocked"])
        stats = run_stale_skill_cleanup(self.db_path, profile)
        self.assertEqual(stats["skills_deleted"], 0)
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertIn("KeepBlocked", names)

    def test_hygiene_protects_missing_and_unwanted(self):
        upsert_skill_pattern(
            self.db_path, name="Learn", pattern=r"\bLearn\b", source="test"
        )
        upsert_skill_pattern(
            self.db_path, name="Unwanted", pattern=r"\bUnwanted\b", source="test"
        )
        stamp_skill_patterns_created_at(
            self.db_path,
            {"learn": _old_iso(), "unwanted": _old_iso()},
        )
        profile = AppConfig(
            missing_skills_suggestions=["Learn"],
            unwanted_skills=["Unwanted"],
        )
        stats = run_stale_skill_cleanup(self.db_path, profile)
        self.assertEqual(stats["skills_deleted"], 0)
        names = {row["name"] for row in get_skill_patterns(self.db_path, enabled_only=False)}
        self.assertEqual(names, {"Learn", "Unwanted"})


class ProcessInboxStaleCleanupOrderTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.inbox = os.path.join(self._tmpdir.name, "inbox")
        os.makedirs(self.inbox)
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.profile_path = os.path.join(self._tmpdir.name, "profile.json")
        with open(self.profile_path, "w", encoding="utf-8") as handle:
            json.dump({}, handle)

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
    @patch(
        "spejder.workflows.inbox_workflow.update_profile_from_db_signals",
        return_value={
            "labeled_count": 0,
            "learned_include_count": 0,
            "learned_exclude_count": 0,
            "missing_skills_count": 0,
        },
    )
    @patch("spejder.workflows.inbox_workflow.rescore_jobs_if_active", return_value=0)
    @patch(
        "spejder.workflows.inbox_workflow.run_stale_skill_cleanup",
        return_value={
            "skills_deleted": 0,
            "skill_rows_deleted": 0,
            "job_skill_links_deleted": 0,
            "affected_job_ids": [],
            "profile_removed": 0,
            "deleted_skill_names": [],
        },
    )
    @patch(
        "spejder.workflows.inbox_workflow._learn_skill_patterns_from_positions",
        return_value={
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": 0,
        },
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
    @patch("spejder.workflows.inbox_workflow.LocalLLM")
    @patch(
        "spejder.workflows.inbox_workflow.get_jobs_for_description_refresh",
        return_value=[{"id": 1}],
    )
    @patch(
        "spejder.workflows.inbox_workflow.sync_itday_portal",
        return_value={"found": 0, "inserted_new": 0, "skipped_existing": 0, "processed": 0},
    )
    def test_hygiene_runs_after_learning_before_profile_signals(
        self,
        _portal,
        _desc_refresh,
        mock_llm,
        _ingest,
        _print_stats,
        _delete_files,
        _gen_desc,
        _materialize,
        _relevant,
        mock_learn,
        mock_stale,
        _rescore,
        mock_signals,
        _summarize,
        _report,
    ):
        order: list[str] = []
        mock_llm.return_value = object()
        mock_learn.side_effect = lambda *a, **k: (
            order.append("learn"),
            {
                "considered_positions": 0,
                "new_skill_patterns": 0,
                "total_known_skill_patterns": 0,
            },
        )[1]
        mock_stale.side_effect = lambda *a, **k: (
            order.append("stale"),
            {
                "skills_deleted": 0,
                "skill_rows_deleted": 0,
                "job_skill_links_deleted": 0,
                "affected_job_ids": [],
                "profile_removed": 0,
                "deleted_skill_names": [],
            },
        )[1]
        mock_signals.side_effect = lambda *a, **k: (
            order.append("signals"),
            {
                "labeled_count": 0,
                "learned_include_count": 0,
                "learned_exclude_count": 0,
                "missing_skills_count": 0,
            },
        )[1]

        process_inbox(
            inbox=self.inbox,
            db=self.db_path,
            profile=self.profile_path,
            model="/fake/model.gguf",
        )

        self.assertEqual(order, ["learn", "stale", "signals"])
        mock_stale.assert_called_once()
        self.assertEqual(mock_stale.call_args.args[0], self.db_path)
        self.assertIsInstance(mock_stale.call_args.args[1], AppConfig)


if __name__ == "__main__":
    unittest.main()
