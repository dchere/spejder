import time
from collections.abc import Callable
from typing import Optional, Union

from spejder.config import AppConfig
from spejder.db import upsert_job
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import load_artifacts
from spejder.jobs.parsing.artifact_synth import try_synthesize_artifact
from spejder.jobs.parsing.core import extract_job_entries
from spejder.jobs.parsing.extract_quality import partition_entries, weak_reason_summary
from spejder.llm import LocalLLM

_INGEST_ETA_KIND = "position"

# Legacy: (processed, inserted_new, skipped_existing). Extended: same + optional eta_s.
ProgressCallback = Union[
    Callable[[int, int, int], None],
    Callable[[int, int, int, Optional[float]], None],
]


def _extract_for_doc(
    doc: dict,
    *,
    runtime_profile: Optional[AppConfig],
    artifacts: Optional[list[CareerAlertArtifact]] = None,
    meta_out: Optional[dict] = None,
) -> list[dict]:
    if runtime_profile is None:
        return extract_job_entries(doc, meta_out=meta_out)
    if artifacts is not None:
        return extract_job_entries(doc, artifacts=artifacts, meta_out=meta_out)
    return extract_job_entries(
        doc,
        artifacts_dir=runtime_profile.career_alert_artifacts_dir,
        artifacts_disabled=runtime_profile.career_alert_artifacts_disabled,
        meta_out=meta_out,
    )


def _load_run_artifacts(runtime_profile: AppConfig) -> list[CareerAlertArtifact]:
    return load_artifacts(
        overlay_dir=runtime_profile.career_alert_artifacts_dir,
        disabled_ids=runtime_profile.career_alert_artifacts_disabled,
    )


def _file_parse_status(
    *,
    strong_count: int,
    weak_count: int,
    synth_reason: str,
) -> str:
    if strong_count > 0:
        if synth_reason == "ok":
            return "synth_ok"
        if weak_count > 0:
            return "partial_weak"
        return "ok"
    if synth_reason and synth_reason != "ok":
        return "synth_failed"
    if weak_count > 0:
        return "weak"
    if synth_reason == "ok":
        # Synth wrote an overlay but re-extract still yielded nothing strong.
        return "synth_empty"
    return "empty"


def _upsert_one_entry(
    db_path: str,
    entry: dict,
    *,
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
) -> bool:
    """Transform (optional) and upsert one job entry; return whether it was new.

    Shared by :func:`ingest_entries_to_db` and :func:`ingest_docs_to_db` pass-2 so
    link-bearing upsert + ``on_new_record`` stay behavior-identical.
    """
    if entry_transform is not None:
        entry = entry_transform(dict(entry))
    is_new_record = upsert_job(db_path, entry)
    if is_new_record and on_new_record is not None:
        on_new_record()
    return bool(is_new_record)


def ingest_entries_to_db(
    db_path: str,
    entries: list[dict],
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0

    for entry in entries:
        if not entry.get("position_link"):
            continue
        is_new_record = _upsert_one_entry(
            db_path,
            entry,
            entry_transform=entry_transform,
            on_new_record=on_new_record,
        )
        if is_new_record:
            inserted_new += 1
        else:
            skipped_existing += 1
        processed += 1
        if on_progress:
            on_progress(processed, inserted_new, skipped_existing)

    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
    }


def _notify_ingest_progress(
    on_progress: Optional[ProgressCallback],
    processed: int,
    inserted_new: int,
    skipped_existing: int,
    eta_s: Optional[float],
) -> None:
    if on_progress is None:
        return
    try:
        on_progress(processed, inserted_new, skipped_existing, eta_s)  # type: ignore[call-arg, misc]
    except TypeError:
        on_progress(processed, inserted_new, skipped_existing)  # type: ignore[call-arg]


def ingest_docs_to_db(
    db_path: str,
    docs: list[dict],
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[ProgressCallback] = None,
    *,
    llm: Optional[LocalLLM] = None,
    runtime_profile: Optional[AppConfig] = None,
    on_status_message: Optional[Callable[[str], None]] = None,
    eta_store_path: Optional[str] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    positions_by_file: list[dict[str, object]] = []
    # (file_index, entry) — strong rows with position_link, after extract/synth.
    worklist: list[tuple[int, dict]] = []
    synth_llm = llm
    # Load once per ingest run; reload after a successful synth overlay write.
    artifact_cache: Optional[list[CareerAlertArtifact]] = (
        _load_run_artifacts(runtime_profile) if runtime_profile is not None else None
    )

    file_count = len(docs)
    # ETA is opt-in via status/store (job-count on_progress exists independently).
    # Lazy import: jobs ↔ workflows cycle via workflows.__init__ → inbox → jobs.
    track_eta = (
        on_status_message is not None or eta_store_path is not None
    ) and file_count > 0
    # Bound only when track_eta; referenced only inside matching branches below.
    store_path = ""
    loaded: list[float] = []
    historical = None
    run_samples: list[float] = []
    last_eta_s: Optional[float] = None
    if track_eta:
        from spejder.workflows.progress_eta import (
            estimate_remaining_seconds,
            format_ingest_stage_message,
            historical_from_slow_samples,
            ingest_eta_store_path,
            load_slow_samples,
            save_slow_samples,
        )

        store_path = eta_store_path or ingest_eta_store_path(db_path)
        loaded = load_slow_samples(store_path, kind=_INGEST_ETA_KIND)
        historical = historical_from_slow_samples(loaded)
        # Position total unknown until extract/synth finishes — no ETA yet.
        if on_status_message is not None:
            on_status_message(
                format_ingest_stage_message(file_count=file_count, eta_s=None)
            )

    # Pass 1: extract / optional synth (untimed). Build upsert worklist.
    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        extract_meta: dict = {}
        entries = _extract_for_doc(
            doc,
            runtime_profile=runtime_profile,
            artifacts=artifact_cache,
            meta_out=extract_meta,
        )
        strong, weak = partition_entries(entries)
        synth_reason = ""
        if (
            not strong
            and runtime_profile is not None
            and runtime_profile.career_alert_synth_enabled
        ):
            model_path = str(runtime_profile.default_model or "").strip()
            if model_path and synth_llm is None:
                synth_llm = LocalLLM(
                    model_path=model_path,
                    n_ctx=int(runtime_profile.n_ctx or 8192),
                    verbose=False,
                )
            html_text = str(doc.get("html") or "")
            doc_links = [str(item) for item in (doc.get("links") or []) if item]
            artifact, reason = try_synthesize_artifact(
                html_text,
                synth_llm,
                runtime_profile,
                overlay_dir=runtime_profile.career_alert_artifacts_dir,
                title_hint=str(doc.get("title") or ""),
                text=str(doc.get("text") or ""),
                from_hint=str(doc.get("from") or ""),
                links=doc_links,
                existing_artifacts=artifact_cache,
            )
            synth_reason = str(reason or "")
            if artifact is not None:
                artifact_cache = _load_run_artifacts(runtime_profile)
                extract_meta = {}
                entries = _extract_for_doc(
                    doc,
                    runtime_profile=runtime_profile,
                    artifacts=artifact_cache,
                    meta_out=extract_meta,
                )
                strong, weak = partition_entries(entries)
            else:
                print(
                    f"[spejder] career-alert synth skipped for {file_path or '(unknown)'}: "
                    f"{synth_reason}"
                )

        quality = weak_reason_summary(weak)
        status = _file_parse_status(
            strong_count=len(strong),
            weak_count=len(weak),
            synth_reason=synth_reason,
        )
        artifact_ids = [
            str(item)
            for item in (extract_meta.get("artifact_ids") or [])
            if str(item).strip()
        ]
        file_index = len(positions_by_file)
        upsertable = [entry for entry in strong if entry.get("position_link")]
        for entry in upsertable:
            worklist.append((file_index, entry))
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(len(upsertable)),
                "inserted_new": 0,
                "skipped_existing": 0,
                "weak_dropped": int(len(weak)),
                "quality": quality,
                "synth_reason": synth_reason,
                "status": status,
                "artifact_ids": artifact_ids,
            }
        )

    position_total = len(worklist)
    if track_eta:
        last_eta_s = estimate_remaining_seconds(
            remaining=position_total,
            samples=run_samples,
            historical=historical,
        )
        if on_status_message is not None:
            on_status_message(
                format_ingest_stage_message(file_count=file_count, eta_s=last_eta_s)
            )

    # Pass 2: upsert each strong position; wall time of upsert is the slow unit.
    for positions_done, (file_index, entry) in enumerate(worklist, start=1):
        t0 = time.monotonic() if track_eta else 0.0
        is_new_record = _upsert_one_entry(
            db_path,
            entry,
            entry_transform=entry_transform,
            on_new_record=on_new_record,
        )
        if track_eta:
            run_samples.append(time.monotonic() - t0)
            if store_path:
                try:
                    save_slow_samples(
                        store_path,
                        loaded + run_samples,
                        kind=_INGEST_ETA_KIND,
                    )
                except OSError:
                    pass
            last_eta_s = estimate_remaining_seconds(
                remaining=max(0, position_total - positions_done),
                samples=run_samples,
                historical=historical,
            )
            if on_status_message is not None:
                on_status_message(
                    format_ingest_stage_message(
                        file_count=file_count, eta_s=last_eta_s
                    )
                )
        file_row = positions_by_file[file_index]
        if is_new_record:
            inserted_new += 1
            file_row["inserted_new"] = int(file_row["inserted_new"]) + 1
        else:
            skipped_existing += 1
            file_row["skipped_existing"] = int(file_row["skipped_existing"]) + 1
        processed += 1
        _notify_ingest_progress(
            on_progress,
            processed,
            inserted_new,
            skipped_existing,
            last_eta_s,
        )

    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }
