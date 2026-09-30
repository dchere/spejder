import time
from typing import Callable, Optional, Union

from spejder.config import AppConfig
from spejder.db import get_jobs_for_active_rescore
from spejder.extractors.skill_extractor import _get_or_extract_job_skills
from spejder.jobs.scoring import job_in_active_rescore_scope, rescore_job_by_id
from spejder.llm import LocalLLM
from spejder.workflows.job_text_enrichment import _enrich_raw_text_with_position_page
from spejder.workflows.progress_eta import (
    SKILLS_STAGE_MESSAGE,
    estimate_remaining_seconds,
    format_skills_stage_message,
    load_rolling_average,
    save_rolling_average,
    skills_eta_store_path,
)

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
) -> int:
    """Materialize skills for multiple jobs. Returns count of jobs that received skills.

    When ``on_progress`` / ``on_status_message`` is set (or ``eta_store_path`` is
    passed), tracks per-position wall time into a rolling average sidecar and
    surfaces ETA / percentage on progress ticks.
    """
    if not rows:
        return 0

    from spejder.db import get_job_skills

    track_eta = (
        on_progress is not None
        or on_status_message is not None
        or eta_store_path is not None
    )
    store_path = eta_store_path or (skills_eta_store_path(db_path) if track_eta else "")
    historical = load_rolling_average(store_path) if track_eta else None
    run_total_seconds = 0.0
    run_count = 0

    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    updated = 0
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        t0 = time.monotonic()
        try:
            job_id = int(row.get("id", 0) or 0)
            if not job_id:
                continue
            if skip_cached and get_job_skills(db_path, job_id):
                continue

            had_cache = bool(get_job_skills(db_path, job_id))
            skills_text, _, skills_changed = materialize_job_skills(
                db_path,
                row,
                llm=llm,
                runtime_profile=runtime_profile,
                page_context_cache=page_context_cache,
                title_translation_cache=title_translation_cache,
                rescore=rescore,
                first_materialize=not had_cache,
            )
            if skills_text or skills_changed:
                updated += 1
            # Stdout only when this row actually ran materialize (skip paths stay silent).
            if progress_label and (idx % 25 == 0 or idx == total):
                print(f"{progress_label}: checked={idx}/{total}, updated={updated}")
        finally:
            dt = time.monotonic() - t0
            if track_eta and historical is not None:
                historical.record(dt)
                run_total_seconds += dt
                run_count += 1
            # Always tick on_progress on cadence / last idx so skips still reach 100%.
            if (on_progress is not None or on_status_message is not None) and (
                idx % 25 == 0 or idx == total
            ):
                eta_s = None
                if track_eta and historical is not None:
                    eta_s = estimate_remaining_seconds(
                        remaining=max(0, total - idx),
                        run_total_seconds=run_total_seconds,
                        run_count=run_count,
                        historical=historical,
                    )
                    if store_path:
                        try:
                            save_rolling_average(store_path, historical)
                        except OSError:
                            pass
                _notify_progress(on_progress, idx, total, updated, eta_s)
                if on_status_message is not None:
                    on_status_message(
                        format_skills_stage_message(
                            checked=idx,
                            total=total,
                            eta_s=eta_s,
                            base=SKILLS_STAGE_MESSAGE,
                        )
                    )
    if track_eta and historical is not None and store_path:
        try:
            save_rolling_average(store_path, historical)
        except OSError:
            pass
    return updated


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
) -> int:
    """Phase-2 batch: enrich, extract, persist, and optionally rescore scoped jobs."""
    rows = _collect_relevant_and_applied_rows(db_path)
    if progress_label:
        print(f"{progress_label}: starting (jobs={len(rows)})")
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
    )
    if progress_label:
        print(f"{progress_label}: done (updated={updated})")
    return updated
