"""CLI/workflow helpers for career-alert artifact management."""

from __future__ import annotations

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile, save_profile
from spejder.jobs.parsing.artifact_store import list_loaded_artifacts


def list_career_alert_artifacts(profile: str = None) -> None:
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime = load_runtime_profile(profile_path)
    disabled = set(runtime.career_alert_artifacts_disabled or [])
    items = list_loaded_artifacts(
        overlay_dir=runtime.career_alert_artifacts_dir,
        disabled_ids=list(disabled),
        include_disabled=True,
    )
    if not items:
        print("No career-alert artifacts found.")
        return
    print(
        f"{'id':<32} {'prio':>4} {'ver':>3} {'origin':<8} {'src':<10} disabled  path"
    )
    for item in items:
        art = item.artifact
        is_disabled = (not art.enabled) or (art.id in disabled)
        print(
            f"{art.id:<32} {art.priority:>4} {art.version:>3} {item.origin:<8} "
            f"{art.source:<10} {str(is_disabled):<8} {item.path}"
        )


def disable_career_alert_artifact(artifact_id: str, profile: str = None) -> None:
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime = load_runtime_profile(profile_path)
    target = (artifact_id or "").strip()
    if not target:
        raise SystemExit("artifact id is required")
    disabled = list(runtime.career_alert_artifacts_disabled or [])
    if target not in disabled:
        disabled.append(target)
        runtime.career_alert_artifacts_disabled = disabled
        save_profile(runtime, profile_path)
        print(f"Disabled career-alert artifact: {target}")
    else:
        print(f"Already disabled: {target}")


def enable_career_alert_artifact(artifact_id: str, profile: str = None) -> None:
    profile_path = profile or DEFAULT_PROFILE_PATH
    runtime = load_runtime_profile(profile_path)
    target = (artifact_id or "").strip()
    if not target:
        raise SystemExit("artifact id is required")
    disabled = [x for x in (runtime.career_alert_artifacts_disabled or []) if x != target]
    runtime.career_alert_artifacts_disabled = disabled
    save_profile(runtime, profile_path)
    print(f"Enabled career-alert artifact: {target}")
