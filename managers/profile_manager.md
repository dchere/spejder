# spejder.profile_manager

**Purpose:**
Handles all profile-related mutations and querying for the application.

**API:**
- `_load_runtime_profile`
- `_save_profile`
- `_toggle_profile_skill`
- `_remove_skill_from_profile`
- `_block_skill_in_profile` — profile-only: adds to `blocked_skills`, removes from user/missing/pattern lists; does **not** touch SQLite or the bad cloud (callers run `delete_skill_from_db` / `cleanup_blocked_skills_from_db` and `bad_cloud.on_skills_blocked` for cloud ingest + prune)
- `_protected_skill_keys`

**Context:**
Extracted from `workflows.py` to isolate configuration logic from workflow orchestration. Dashboard Profile panel GET/save lives in `profile_editor.md` (partial merge into live runtime; Skills/Sync persist still share the same `profile.json`).