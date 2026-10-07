"""Coordinate post-clean-skills rematerialize without a full inbox sync.

When a skills materialize batch is active, ``ensure`` appends job ids to a
pending tail that the batch drains mid-loop. Otherwise a dedicated background
worker materializes under the process-wide materialize mutex.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Callable, Literal, Optional

from spejder.config import AppConfig
from spejder.db import get_job_by_id
from spejder.llm import LocalLLM

EnsureResult = Literal["joined_batch", "started", "already_pending"]

# Process-wide: at most one LLM skills materialize path at a time.
MATERIALIZE_LOCK = threading.Lock()


class SkillsRematerializeCoordinator:
    """Thread-safe pending job-id queue + dedicated rematerialize worker."""

    def __init__(
        self,
        *,
        db_path: str,
        runtime_profile: AppConfig,
        model_path: str,
        cli_verbose: bool,
        queue_dashboard_rebuild: Callable[..., None],
    ) -> None:
        self.db_path = db_path
        self.runtime_profile = runtime_profile
        self.model_path = model_path
        self.cli_verbose = cli_verbose
        self.queue_dashboard_rebuild = queue_dashboard_rebuild
        self._lock = threading.Lock()
        self._pending: OrderedDict[int, None] = OrderedDict()
        self._in_flight: set[int] = set()
        self._requeue: set[int] = set()
        self._active_batch = False
        self._worker_claimed = False

    @property
    def active_batch(self) -> bool:
        with self._lock:
            return self._active_batch

    def ensure(self, job_id: int) -> EnsureResult:
        jid = int(job_id)
        if jid <= 0:
            return "already_pending"
        should_start = False
        with self._lock:
            if jid in self._in_flight:
                # Clean again while a pass is running: run once more after complete.
                self._requeue.add(jid)
                return "already_pending"
            if jid in self._pending:
                if self._active_batch or self._worker_claimed:
                    return "already_pending"
                # Orphaned pending (e.g. prior worker failed before drain).
                self._worker_claimed = True
                should_start = True
            else:
                self._pending[jid] = None
                if self._active_batch:
                    return "joined_batch"
                if not self._worker_claimed:
                    self._worker_claimed = True
                    should_start = True
        if should_start:
            threading.Thread(
                target=self._worker_loop,
                name="spejder-skills-rematerialize",
                daemon=True,
            ).start()
        return "started"

    def begin_active_batch(self) -> None:
        with self._lock:
            self._active_batch = True

    def end_active_batch(self) -> None:
        """Clear active_batch; start dedicated worker if pending remains."""
        should_start = False
        with self._lock:
            self._active_batch = False
            if self._pending and not self._worker_claimed:
                self._worker_claimed = True
                should_start = True
        if should_start:
            threading.Thread(
                target=self._worker_loop,
                name="spejder-skills-rematerialize",
                daemon=True,
            ).start()

    def drain_pending(self) -> list[int]:
        """Move pending ids to in-flight (ordered). Caller must ``complete`` each."""
        with self._lock:
            ids = list(self._pending.keys())
            self._pending.clear()
            for jid in ids:
                self._in_flight.add(jid)
            return ids

    def complete(self, job_id: int) -> None:
        with self._lock:
            jid = int(job_id)
            self._in_flight.discard(jid)
            if jid in self._requeue:
                self._requeue.discard(jid)
                self._pending[jid] = None

    def _repend_job(self, job_id: int) -> None:
        with self._lock:
            jid = int(job_id)
            self._in_flight.discard(jid)
            self._pending[jid] = None

    def _make_llm(self) -> Optional[LocalLLM]:
        if not self.model_path:
            return None
        return LocalLLM(
            model_path=self.model_path,
            n_ctx=int(self.runtime_profile.n_ctx),
            verbose=self.cli_verbose,
        )

    def _materialize_one(self, job_id: int, llm: Optional[LocalLLM]) -> bool:
        from spejder.workflows.job_skills_materialize import materialize_job_skills

        row = get_job_by_id(self.db_path, job_id)
        if row is None:
            return False
        skills_text, _, skills_changed = materialize_job_skills(
            self.db_path,
            row,
            llm=llm,
            runtime_profile=self.runtime_profile,
            rescore=True,
            first_materialize=True,
        )
        return bool(skills_text or skills_changed)

    def _worker_loop(self) -> None:
        try:
            while True:
                with MATERIALIZE_LOCK:
                    # Init LLM before drain so a construct failure leaves ids pending.
                    llm = self._make_llm()
                    ids = self.drain_pending()
                    if not ids:
                        with self._lock:
                            if self._pending:
                                continue
                            self._worker_claimed = False
                        return
                    updated = 0
                    for jid in ids:
                        try:
                            if self._materialize_one(jid, llm):
                                updated += 1
                            self.complete(jid)
                        except Exception as exc:
                            print(
                                f"Skills rematerialize job {jid} failed: {exc}"
                            )
                            self._repend_job(jid)
                    if updated > 0:
                        self.queue_dashboard_rebuild(
                            reason=f"skills rematerialized={updated}"
                        )
        except Exception as exc:
            with self._lock:
                for jid in list(self._in_flight):
                    self._in_flight.discard(jid)
                    self._pending[jid] = None
                for jid in list(self._requeue):
                    self._requeue.discard(jid)
                    self._pending[jid] = None
                self._worker_claimed = False
            print(f"Skills rematerialize worker failed: {exc}")
