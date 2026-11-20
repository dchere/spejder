# spejder.profile_manager

**Purpose:**
Handles all profile-related mutations and querying for the application.

**API:**
- `_load_runtime_profile`
- `_save_profile`
- `_toggle_profile_skill` — membership/dedup via `db.utils._normalize_skill_name_key`; stored display collapses whitespace and preserves case (`" ".join(str(skill_name).strip().split())`). Enabling `"foo  bar"` when `"foo bar"` is present is a no-op; disabling `"foo  bar"` removes `"foo bar"`.
- `_toggle_exclusive_profile_skill` — add/remove one list (`field`) and, when enabling, drop the skill from `drop_from` exclusive others. Returns `(changed, dropped_from_others)`.
- `_remove_skill_from_profile` — also strips `unwanted_skills`; matches list/pattern names via `_normalize_skill_name_key`
- `_block_skill_in_profile` — profile-only: adds to `blocked_skills`, removes from user/unwanted/missing/pattern lists; does **not** touch SQLite or the bad cloud (callers run `delete_skill_from_db` / `cleanup_blocked_skills_from_db` and `bad_cloud.on_skills_blocked` for cloud ingest + prune). Block membership uses `_normalize_skill_name_key`; stored blocked display collapses whitespace and preserves case.

**Skills-tab exclusivity (server is source of truth):**
- **Pass** (`unwanted_skills`) is mutually exclusive with **Have** (`user_skills`) and **Want** (`missing_skills_suggestions`); those two stay independently combinable.
- Enable Pass → add to `unwanted_skills`; drop from Have and Want.
- Enable Have → add to `user_skills`; drop from `unwanted_skills` (Want unchanged).
- Enable Want → add to `missing_skills_suggestions`; drop from `unwanted_skills`.
- Dirty JSON / Profile textareas: `AppConfig` sanitizer, unwanted wins (drop from Have/Want).

**Context:**
Extracted from `workflows.py` to isolate configuration logic from workflow orchestration. Dashboard Profile panel GET/save lives in `profile_editor.md` (partial merge into live runtime; Skills/Sync persist still share the same `profile.json`).