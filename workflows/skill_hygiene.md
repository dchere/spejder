# spejder.workflows.skill_hygiene

**Purpose:**
Shared post-learning skill hygiene for GUI background sync and CLI `process-inbox`, so both pipelines run the same blocked cleanup → stale cleanup → bad-cloud seed/recalibrate contract.

**API:**
- `run_stale_skill_cleanup(db_path, profile)` — delete-only stale low-share retention (no `blocked_skills` / bad-cloud side effects)
- `run_skill_hygiene_stages(db_path, profile, *, on_stage=None) -> SkillHygieneResult` — full shared stages
- `SkillHygieneResult` — blocked/stale/cloud stats, rescore counts, threshold change flags; `profile_dirty` / `*_needs_rebuild()` helpers for callers

**Stage order (fixed):**
1. `blocked_skills` — `cleanup_blocked_skills_from_db` + `rescore_jobs_if_active`
2. `stale_skills` — `run_stale_skill_cleanup` + `rescore_jobs_if_active`
3. `bad_cloud` — `ensure_db` + `ensure_bad_cloud_initialized` + `recalibrate_and_store_threshold`

**Caller responsibilities:**
- Emit stages via optional `on_stage(stage_id, message)` (GUI `_emit_stage` / CLI `sync_log.stage_start`)
- Log summary counts
- Persist profile when `result.profile_dirty` (GUI also `reload_runtime_profile`)
- Queue dashboard rebuilds from `blocked_needs_rebuild` / `stale_needs_rebuild` / `cloud_needs_rebuild` (GUI only)

**Constraints:**
- Do not diverge stage order between `gui_sync` and `inbox_workflow`; both must call `run_skill_hygiene_stages`.
- Stale cleanup must not append `blocked_skills` or touch the bad cloud.
