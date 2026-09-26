from collections.abc import Callable
from typing import Optional

from spejder.config import AppConfig
from spejder.db import upsert_job
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import load_artifacts
from spejder.jobs.parsing.artifact_synth import try_synthesize_artifact
from spejder.jobs.parsing.core import extract_job_entries
from spejder.jobs.parsing.extract_quality import partition_entries, weak_reason_summary
from spejder.llm import LocalLLM


def _extract_for_doc(
    doc: dict,
    *,
    runtime_profile: Optional[AppConfig],
    artifacts: Optional[list[CareerAlertArtifact]] = None,
) -> list[dict]:
    if runtime_profile is None:
        return extract_job_entries(doc)
    if artifacts is not None:
        return extract_job_entries(doc, artifacts=artifacts)
    return extract_job_entries(
        doc,
        artifacts_dir=runtime_profile.career_alert_artifacts_dir,
        artifacts_disabled=runtime_profile.career_alert_artifacts_disabled,
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
        if entry_transform is not None:
            entry = entry_transform(dict(entry))
        is_new_record = upsert_job(db_path, entry)
        if is_new_record and on_new_record:
            on_new_record()
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


def ingest_docs_to_db(
    db_path: str,
    docs: list[dict],
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
    *,
    llm: Optional[LocalLLM] = None,
    runtime_profile: Optional[AppConfig] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    positions_by_file: list[dict[str, object]] = []
    synth_llm = llm
    # Load once per ingest run; reload after a successful synth overlay write.
    artifact_cache: Optional[list[CareerAlertArtifact]] = (
        _load_run_artifacts(runtime_profile) if runtime_profile is not None else None
    )

    def _cumulative_progress(
        file_processed: int, file_inserted: int, file_skipped: int
    ) -> None:
        if on_progress:
            on_progress(
                processed + file_processed,
                inserted_new + file_inserted,
                skipped_existing + file_skipped,
            )

    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        entries = _extract_for_doc(
            doc, runtime_profile=runtime_profile, artifacts=artifact_cache
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
                entries = _extract_for_doc(
                    doc, runtime_profile=runtime_profile, artifacts=artifact_cache
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
        file_stats = ingest_entries_to_db(
            db_path,
            strong,
            entry_transform=entry_transform,
            on_new_record=on_new_record,
            on_progress=_cumulative_progress if on_progress else None,
        )
        file_found = int(file_stats.get("processed", 0))
        file_inserted = int(file_stats.get("inserted_new", 0))
        file_skipped = int(file_stats.get("skipped_existing", 0))
        processed += file_found
        inserted_new += file_inserted
        skipped_existing += file_skipped
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(file_found),
                "inserted_new": int(file_inserted),
                "skipped_existing": int(file_skipped),
                "weak_dropped": int(len(weak)),
                "quality": quality,
                "synth_reason": synth_reason,
                "status": status,
            }
        )
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }
