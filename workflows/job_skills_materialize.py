import time
from typing import TYPE_CHECKING, Callable, Optional, Union

from spejder.config import AppConfig
from spejder.db import get_job_by_id, get_job_scope_flags, get_jobs_for_active_rescore
from spejder.extractors.skill_extractor import _get_or_extract_job_skills
from spejder.jobs.scoring import job_in_active_rescore_scope, rescore_job_by_id
from spejder.llm import LocalLLM
from spejder.workflows.job_text_enrichment import _enrich_raw_text_with_position_page
from spejder.workflows.progress_eta import (
    SKILLS_STAGE_MESSAGE,
    estimate_remaining_seconds,
    format_skills_stage_message,
    historical_from_slow_samples,
    load_slow_samples,
    save_slow_samples,
    skills_eta_store_path,
)
from spejder.workflows.skills_rematerialize import MATERIALIZE_LOCK

if TYPE_CHECKING:
    from spejder.workflows.skills_rematerialize import SkillsRematerializeCoordinator

# Legacy: (checked, total, updated). Extended: same + optional eta_s (seconds or None).
ProgressCallback = Union[
    Callable[[int, int, int], None],
    Callable[[int, int, int, Optional[float]], None],
]


def materialize_job_skills(
    db_path: str,
    row: dict,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    page_context_cache: Optional[dict] = None,
    title_translation_cache: Optional[dict] = None,
    rescore: bool = False,
    first_materialize: bool = False,
) -> tuple[str, str, bool]:
    """Enrich job text, extract skills, persist to job_skills. Returns (skills_text, enriched_raw, skills_changed)."""
    job_id = int(row.get("id", 0) or 0)
    raw_text = _enrich_raw_text_with_position_page(
        db_path,
        row,
        page_context_cache=page_context_cache,
        llm=llm,
        runtime_profile=runtime_profile,
        title_translation_cache=title_translation_cache,
        include_summary=False,
        prefer_page=True,
    )
    skills_text, skills_changed = _get_or_extract_job_skills(
        db_path,
        job_id,
        raw_text,
        llm=llm,
        profile=runtime_profile,
        position_link=row.get("position_link", ""),
        page_context_cache=page_context_cache,
    )
    if (
        rescore
        and job_id
        and runtime_profile is not None
        and job_in_active_rescore_scope(row)
        and (skills_changed or first_materialize)
    ):
        rescore_job_by_id(db_path, runtime_profile, job_id)
    return skills_text, raw_text, skills_changed


def _notify_progress(
    on_progress: Optional[ProgressCallback],
    checked: int,
    total: int,
    updated: int,
    eta_s: Optional[float],
) -> None:
    if on_progress is None:
        return
    try:
        on_progress(checked, total, updated, eta_s)  # type: ignore[call-arg, misc]
    except TypeError:
        on_progress(checked, total, updated)  # type: ignore[call-arg]


def materialize_jobs_skills(
    db_path: str,
    rows: list[dict],
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    rescore: bool = False,
    skip_cached: bool = False,
    progress_label: str = "",
    on_progress: Optional[ProgressCallback] = None,
    on_status_message: Optional[Callable[[str], None]] = None,
    eta_store_path: Optional[str] = None,
    rematerialize: Optional["SkillsRematerializeCoordinator"] = None,
) -> int:
    """Materialize skills for multiple jobs. Returns count of jobs that received skills.

    A classification pass drops missing ids and ``skip_cached`` hits before the
    timed loop. ``checked`` / ``total`` and the ETA sidecar count only those
    slow rows.     Immediately before each expensive materialize, reloads live
    scope flags via ``get_job_scope_flags`` and skips when
    ``job_in_active_rescore_scope`` is false or the job is missing (still
    advances ``checked`` / progress; does not append an ETA sample or
    increment ``updated``). Rows appended from ``rematerialize`` pending
    (post clean-skills) always materialize regardless of live scope. When
    progress, status, or ``eta_store_path`` is
    set, each completed slow row appends its duration to the versioned
    slow-sample store and refreshes the stage line (base sentence, plus
    minutes left when a rate exists).

    When ``rematerialize`` is set, registers as an active batch (so
    ``ensure`` appends to the pending tail), holds ``MATERIALIZE_LOCK``, and
    drains pending job ids onto a growable worklist mid-batch. Dedup is only
    against not-yet-processed work so a cleaned job can be re-appended after
    its earlier pass.
    """
    from spejder.db import get_job_skills_for_jobs

    job_ids = [int(row.get("id", 0) or 0) for row in rows]
    job_ids = [job_id for job_id in job_ids if job_id > 0]
    skills_by_job = get_job_skills_for_jobs(db_path, job_ids) if job_ids else {}

    slow_rows: list[tuple[dict, bool, bool]] = []
    for row in rows:
        job_id = int(row.get("id", 0) or 0)
        if job_id <= 0:
            continue
        skills = skills_by_job.get(job_id) or []
        if skip_cached and skills:
            continue
        slow_rows.append((row, not skills, False))

    if not slow_rows and rematerialize is None:
        return 0

    worklist: list[tuple[dict, bool, bool]] = list(slow_rows)
    # Dedup pending drains against not-yet-processed work only so a cleaned
    # job can be re-appended after its earlier pass completes.
    idx = 0

    def _remaining_work_ids() -> set[int]:
        return {
            int(row.get("id", 0) or 0)
            for row, *_ in worklist[idx:]
            if int(row.get("id", 0) or 0) > 0
        }

    def _append_pending() -> None:
        if rematerialize is None:
            return
        remaining = _remaining_work_ids()
        for pending_id in rematerialize.drain_pending():
            if pending_id in remaining:
                rematerialize.complete(pending_id)
                continue
            row = get_job_by_id(db_path, pending_id)
            if row is None:
                rematerialize.complete(pending_id)
                continue
            remaining.add(pending_id)
            # Skills were cleared; treat as first materialize (like applied raw-text).
            worklist.append((row, True, True))

    owns_batch = rematerialize is not None
    if owns_batch:
        rematerialize.begin_active_batch()

    updated = 0
    try:
        with MATERIALIZE_LOCK:
            _append_pending()
            total = len(worklist)
            if total == 0:
                return 0

            if progress_label:
                print(f"{progress_label}: starting (jobs={total})")

            track_eta = (
                on_progress is not None
                or on_status_message is not None
                or eta_store_path is not None
            )
            store_path = eta_store_path or (
                skills_eta_store_path(db_path) if track_eta else ""
            )
            loaded = load_slow_samples(store_path) if track_eta else []
            historical = historical_from_slow_samples(loaded)
            run_samples: list[float] = []

            page_context_cache: dict[str, str] = {}
            title_translation_cache: dict[str, str] = {}
            if on_status_message is not None and total > 0:
                eta_s = None
                if track_eta:
                    eta_s = estimate_remaining_seconds(
                        remaining=total,
                        samples=run_samples,
                        historical=historical,
                    )
                on_status_message(
                    format_skills_stage_message(eta_s=eta_s, base=SKILLS_STAGE_MESSAGE)
                )

            while True:
                _append_pending()
                total = len(worklist)
                if idx >= total:
                    break
                row, first_materialize, force_materialize = worklist[idx]
                idx += 1
                job_id = int(row.get("id", 0) or 0)
                live_flags = get_job_scope_flags(db_path, job_id) if job_id else None
                if (
                    not force_materialize
                    and (
                        live_flags is None
                        or not job_in_active_rescore_scope(live_flags)
                    )
                ):
                    eta_s = None
                    if track_eta:
                        eta_s = estimate_remaining_seconds(
                            remaining=max(0, total - idx),
                            samples=run_samples,
                            historical=historical,
                        )
                    if on_progress is not None or on_status_message is not None:
                        _notify_progress(on_progress, idx, total, updated, eta_s)
                        if on_status_message is not None:
                            on_status_message(
                                format_skills_stage_message(
                                    eta_s=eta_s, base=SKILLS_STAGE_MESSAGE
                                )
                            )
                    if rematerialize is not None:
                        rematerialize.complete(job_id)
                    continue

                t0 = time.monotonic() if track_eta else 0.0
                try:
                    skills_text, _, skills_changed = materialize_job_skills(
                        db_path,
                        row,
                        llm=llm,
                        runtime_profile=runtime_profile,
                        page_context_cache=page_context_cache,
                        title_translation_cache=title_translation_cache,
                        rescore=rescore,
                        first_materialize=first_materialize,
                    )
                    if skills_text or skills_changed:
                        updated += 1
                    # Stdout only for slow rows (cache skips never enter this loop).
                    if progress_label and (idx % 25 == 0 or idx == total):
                        print(
                            f"{progress_label}: checked={idx}/{total}, updated={updated}"
                        )
                finally:
                    if rematerialize is not None:
                        rematerialize.complete(job_id)
                    if track_eta:
                        run_samples.append(time.monotonic() - t0)
                        if store_path:
                            try:
                                save_slow_samples(store_path, loaded + run_samples)
                            except OSError:
                                pass
                    eta_s = None
                    if track_eta:
                        eta_s = estimate_remaining_seconds(
                            remaining=max(0, total - idx),
                            samples=run_samples,
                            historical=historical,
                        )
                    if on_progress is not None or on_status_message is not None:
                        _notify_progress(on_progress, idx, total, updated, eta_s)
                        if on_status_message is not None:
                            on_status_message(
                                format_skills_stage_message(
                                    eta_s=eta_s, base=SKILLS_STAGE_MESSAGE
                                )
                            )
            return updated
    finally:
        if owns_batch:
            rematerialize.end_active_batch()


def _collect_relevant_and_applied_rows(db_path: str) -> list[dict]:
    return get_jobs_for_active_rescore(db_path)


def materialize_relevant_and_applied_skills(
    db_path: str,
    *,
    llm: LocalLLM = None,
    runtime_profile: Optional[AppConfig] = None,
    rescore: bool = True,
    skip_cached: bool = True,
    progress_label: str = "Skill materialization",
    on_progress: Optional[ProgressCallback] = None,
    on_status_message: Optional[Callable[[str], None]] = None,
    eta_store_path: Optional[str] = None,
    rematerialize: Optional["SkillsRematerializeCoordinator"] = None,
) -> int:
    """Phase-2 batch: enrich, extract, persist, and optionally rescore scoped jobs."""
    rows = _collect_relevant_and_applied_rows(db_path)
    updated = materialize_jobs_skills(
        db_path,
        rows,
        llm=llm,
        runtime_profile=runtime_profile,
        rescore=rescore,
        skip_cached=skip_cached,
        progress_label=progress_label,
        on_progress=on_progress,
        on_status_message=on_status_message,
        eta_store_path=eta_store_path,
        rematerialize=rematerialize,
    )
    if progress_label:
        print(f"{progress_label}: done (updated={updated})")
    return updated
