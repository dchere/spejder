# spejder.profile_manager

**Purpose:**
Handles all profile-related mutations and querying for the application.

**API:**
- `_load_runtime_profile`
- `_save_profile`
- `_toggle_profile_skill`
- `_remove_skill_from_profile`
- `_block_skill_in_profile` — profile-only: adds to `blocked_skills`, removes from user/missing/pattern lists; does **not** touch SQLite (callers run `delete_skill_from_db` or `cleanup_blocked_skills_from_db`)
- `_protected_skill_keys`

**Context:**
Extracted from `workflows.py` to isolate configuration logic from workflow orchestration.