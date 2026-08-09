from collections.abc import Callable
from typing import Optional

from spejder.config import AppConfig
from spejder.db import upsert_job
from spejder.jobs.parsing.artifact_schema import CareerAlertArtifact
from spejder.jobs.parsing.artifact_store import load_artifacts
from spejder.jobs.parsing.artifact_synth import try_synthesize_artifact
from spejder.jobs.parsing.core import extract_job_entries
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
    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        entries = _extract_for_doc(
            doc, runtime_profile=runtime_profile, artifacts=artifact_cache
        )
        if (
            not entries
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
            artifact, reason = try_synthesize_artifact(
                html_text,
                synth_llm,
                runtime_profile,
                overlay_dir=runtime_profile.career_alert_artifacts_dir,
            )
            if artifact is not None:
                artifact_cache = _load_run_artifacts(runtime_profile)
                entries = _extract_for_doc(
                    doc, runtime_profile=runtime_profile, artifacts=artifact_cache
                )
            else:
                print(
                    f"[spejder] career-alert synth skipped for {file_path or '(unknown)'}: {reason}"
                )
        file_found = 0
        file_inserted = 0
        file_skipped = 0
        for entry in entries:
            if not entry.get("position_link"):
                continue
            if entry_transform is not None:
                entry = entry_transform(dict(entry))
            file_found += 1
            is_new_record = upsert_job(db_path, entry)
            if is_new_record and on_new_record:
                on_new_record()
            if is_new_record:
                inserted_new += 1
                file_inserted += 1
            else:
                skipped_existing += 1
                file_skipped += 1
            processed += 1
            if on_progress:
                on_progress(processed, inserted_new, skipped_existing)
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(file_found),
                "inserted_new": int(file_inserted),
                "skipped_existing": int(file_skipped),
            }
        )
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }
