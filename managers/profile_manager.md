# spejder.profile_manager

**Purpose:**
Handles all profile-related mutations and querying for the application.

**API:**
- `_load_runtime_profile`
- `_save_profile`
- `_toggle_profile_skill`
- `_toggle_exclusive_profile_skill` — add/remove one list (`field`) and, when enabling, drop the skill from `drop_from` exclusive others. Returns `(changed, dropped_from_others)`.
- `_remove_skill_from_profile` — also strips `unwanted_skills`
- `_block_skill_in_profile` — profile-only: adds to `blocked_skills`, removes from user/unwanted/missing/pattern lists; does **not** touch SQLite or the bad cloud (callers run `delete_skill_from_db` / `cleanup_blocked_skills_from_db` and `bad_cloud.on_skills_blocked` for cloud ingest + prune)

**Skills-tab exclusivity (server is source of truth):**
- **Not for me** (`unwanted_skills`) is mutually exclusive with **I have** (`user_skills`) and **Want to learn** (`missing_skills_suggestions`); those two stay independently combinable.
- Enable Not for me → add to `unwanted_skills`; drop from have and learn.
- Enable I have → add to `user_skills`; drop from `unwanted_skills` (learn unchanged).
- Enable Want to learn → add to `missing_skills_suggestions`; drop from `unwanted_skills`.
- Dirty JSON / Profile textareas: `AppConfig` sanitizer, unwanted wins (drop from have/learn).

**Context:**
Extracted from `workflows.py` to isolate configuration logic from workflow orchestration. Dashboard Profile panel GET/save lives in `profile_editor.md` (partial merge into live runtime; Skills/Sync persist still share the same `profile.json`).