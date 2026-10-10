# spejder.workflows.profile_learning

**Purpose:**
Thin shared helper so GUI background sync and CLI `process-inbox` run the same post-hygiene profile keyword learning stage (learned include/exclude keywords + missing-skills suggestions).

**API:**
- `run_profile_keyword_learning(db_path, profile_path, *, on_stage=None) -> ProfileLearningResult`
- `ProfileLearningResult` — `learning_info` (counts from `update_profile_from_db_signals`); `keywords_changed` (include/exclude lists differed); `suggestions_changed` (`missing_skills_suggestions` differed); `profile_changed` property (`keywords_changed or suggestions_changed`)

**Behavior:**
1. Optional `on_stage("profile_learning", "Learning profile keywords")`
2. Snapshot `learned_include_keywords`, `learned_exclude_keywords`, `missing_skills_suggestions` from disk
3. Call `spejder.jobs.update_profile_from_db_signals` (no duplicated learning logic)
4. Re-snapshot; set `keywords_changed` / `suggestions_changed` from the matching list diffs

**Caller responsibilities:**
- GUI: always `reload_runtime_profile` after the write; `rescore_active_jobs` only when `keywords_changed` (suggestions do not affect keyword scores); queue dashboard rebuild when `profile_changed` (keywords or suggestions)
- CLI: print `Profile learning: labeled=…` summary; no dashboard queue or active-scope rescore after learning
- Do not call on early skipped empty sync

**Constraints:**
- Stays after pattern learning + `run_skill_hygiene_stages` (order: patterns → hygiene → profile learning)
- Not part of skill hygiene
