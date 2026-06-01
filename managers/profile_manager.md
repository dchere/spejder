# spejder.profile_manager

**Purpose:**
Handles all profile-related mutations and querying for the application.

**API:**
- `_load_runtime_profile`
- `_save_profile`
- `_toggle_profile_skill`
- `_remove_skill_from_profile`
- `_block_skill_in_profile`
- `_protected_skill_keys`

**Context:**
Extracted from `workflows.py` to isolate configuration logic from workflow orchestration.