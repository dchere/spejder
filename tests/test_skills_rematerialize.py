"""Coordinator + append-to-tail tests for skills rematerialize."""

import os
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from spejder.config import AppConfig
from spejder.db import ensure_db, set_job_viewed, upsert_job
from spejder.db.connection import _connect
from spejder.workflows.job_skills_materialize import materialize_jobs_skills
from spejder.workflows.skills_rematerialize import SkillsRematerializeCoordinator


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
        return int(cur.fetchone()[0])
    finally:
        conn.close()


class SkillsRematerializeCoordinatorTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        ensure_db(self.db_path)
        self.rebuild_calls = []
        self.coord = SkillsRematerializeCoordinator(
            db_path=self.db_path,
            runtime_profile=AppConfig(),
            model_path="",
            cli_verbose=False,
            queue_dashboard_rebuild=lambda reason="": self.rebuild_calls.append(reason),
        )

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_ensure_joins_active_batch(self):
        job_id = _insert_job(self.db_path, "https://example.com/join")
        self.coord.begin_active_batch()
        try:
            self.assertEqual(self.coord.ensure(job_id), "joined_batch")
            self.assertEqual(self.coord.ensure(job_id), "already_pending")
            self.assertEqual(self.coord.drain_pending(), [job_id])
        finally:
            self.coord.end_active_batch()

    def test_ensure_starts_dedicated_worker(self):
        job_id = _insert_job(self.db_path, "https://example.com/start")
        done = threading.Event()

        def fake_one(jid, llm):
            done.set()
            return True

        with patch.object(self.coord, "_materialize_one", side_effect=fake_one):
            self.assertEqual(self.coord.ensure(job_id), "started")
            self.assertTrue(done.wait(2.0))
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if self.rebuild_calls:
                break
            time.sleep(0.02)
        self.assertTrue(any("skills rematerialized" in r for r in self.rebuild_calls))

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_materialize_appends_pending_mid_batch(
        self, _mock_skills, mock_materialize
    ):
        job_a = _insert_job(self.db_path, "https://example.com/batch-a", title="A")
        job_b = _insert_job(self.db_path, "https://example.com/batch-b", title="B")
        started = threading.Event()
        release = threading.Event()
        seen_ids = []

        def materialize_side_effect(db_path, row, **kwargs):
            jid = int(row["id"])
            seen_ids.append(jid)
            if jid == job_a:
                started.set()
                self.assertTrue(release.wait(2.0))
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect

        def run_batch():
            materialize_jobs_skills(
                self.db_path,
                [{"id": job_a}],
                rematerialize=self.coord,
            )

        worker = threading.Thread(target=run_batch, daemon=True)
        worker.start()
        self.assertTrue(started.wait(2.0))
        self.assertEqual(self.coord.ensure(job_b), "joined_batch")
        release.set()
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive())
        self.assertEqual(seen_ids, [job_a, job_b])
        self.assertTrue(
            any(
                call.kwargs.get("first_materialize")
                for call in mock_materialize.call_args_list
                if int(call.args[1]["id"]) == job_b
            )
        )

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_materialize_reappends_processed_job_mid_batch(
        self, _mock_skills, mock_materialize
    ):
        """After A finishes, ensure(A) mid-batch must materialize A again."""
        job_a = _insert_job(self.db_path, "https://example.com/re-a", title="A")
        job_b = _insert_job(self.db_path, "https://example.com/re-b", title="B")
        started_b = threading.Event()
        release_b = threading.Event()
        seen_ids = []

        def materialize_side_effect(db_path, row, **kwargs):
            jid = int(row["id"])
            seen_ids.append(jid)
            if jid == job_b:
                started_b.set()
                self.assertTrue(release_b.wait(2.0))
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect

        def run_batch():
            materialize_jobs_skills(
                self.db_path,
                [{"id": job_a}, {"id": job_b}],
                rematerialize=self.coord,
            )

        worker = threading.Thread(target=run_batch, daemon=True)
        worker.start()
        self.assertTrue(started_b.wait(2.0))
        self.assertEqual(self.coord.ensure(job_a), "joined_batch")
        release_b.set()
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive())
        self.assertEqual(seen_ids, [job_a, job_b, job_a])
        second_a = [
            call
            for call in mock_materialize.call_args_list
            if int(call.args[1]["id"]) == job_a
        ]
        self.assertEqual(len(second_a), 2)
        self.assertTrue(second_a[1].kwargs.get("first_materialize"))

    @patch("spejder.workflows.job_skills_materialize.materialize_job_skills")
    @patch("spejder.db.get_job_skills_for_jobs", return_value={})
    def test_materialize_pending_skips_live_scope_gate(
        self, _mock_skills, mock_materialize
    ):
        """Coordinator-appended jobs materialize even after viewed=1."""
        job_a = _insert_job(self.db_path, "https://example.com/scope-a", title="A")
        job_b = _insert_job(self.db_path, "https://example.com/scope-b", title="B")
        started = threading.Event()
        release = threading.Event()
        seen_ids = []

        def materialize_side_effect(db_path, row, **kwargs):
            jid = int(row["id"])
            seen_ids.append(jid)
            if jid == job_a:
                started.set()
                self.assertTrue(release.wait(2.0))
            return ("Python", "raw", True)

        mock_materialize.side_effect = materialize_side_effect

        def run_batch():
            materialize_jobs_skills(
                self.db_path,
                [{"id": job_a}],
                rematerialize=self.coord,
            )

        worker = threading.Thread(target=run_batch, daemon=True)
        worker.start()
        self.assertTrue(started.wait(2.0))
        self.assertEqual(self.coord.ensure(job_b), "joined_batch")
        set_job_viewed(self.db_path, job_b, viewed=True)
        release.set()
        worker.join(timeout=5.0)
        self.assertFalse(worker.is_alive())
        self.assertIn(job_b, seen_ids)

    def test_worker_continues_after_per_job_failure(self):
        job_1 = _insert_job(
            self.db_path, "https://example.com/worker-fail-1", title="One"
        )
        job_2 = _insert_job(
            self.db_path, "https://example.com/worker-fail-2", title="Two"
        )
        job_3 = _insert_job(
            self.db_path, "https://example.com/worker-fail-3", title="Three"
        )
        seen = []
        fail_job_2_once = True

        def fake_one(jid, llm):
            nonlocal fail_job_2_once
            seen.append(jid)
            if jid == job_2 and fail_job_2_once:
                fail_job_2_once = False
                raise RuntimeError("boom")
            return True

        with self.coord._lock:
            for jid in (job_1, job_2, job_3):
                self.coord._pending[jid] = None
            self.coord._worker_claimed = True

        with patch.object(self.coord, "_materialize_one", side_effect=fake_one):
            with patch.object(self.coord, "queue_dashboard_rebuild"):
                self.coord._worker_loop()

        self.assertEqual(seen, [job_1, job_2, job_3, job_2])
        with self.coord._lock:
            self.assertNotIn(job_2, self.coord._pending)
            self.assertNotIn(job_2, self.coord._in_flight)

    def test_ensure_while_in_flight_requeues(self):
        job_id = _insert_job(self.db_path, "https://example.com/requeue")
        self.coord.begin_active_batch()
        try:
            self.assertEqual(self.coord.ensure(job_id), "joined_batch")
            drained = self.coord.drain_pending()
            self.assertEqual(drained, [job_id])
            self.assertEqual(self.coord.ensure(job_id), "already_pending")
            self.coord.complete(job_id)
            self.assertEqual(self.coord.drain_pending(), [job_id])
        finally:
            self.coord.end_active_batch()


if __name__ == "__main__":
    unittest.main()
