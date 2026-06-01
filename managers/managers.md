# Managers

**Purpose:**
The `managers` package contains higher-level orchestration logic that coordinates between models, APIs, data stores, and application config.

**Modules:**
- `profile_manager.py` / `profile_manager.md`: Handles profile configuration defaults and overrides.
- `dashboard_manager.py` / `dashboard_manager.md`: Handles HTML dashboard generation or reporting structures.
- `language_manager.py`: (refactored) Exposes language parsing and translation utilities. It's composed of smaller feature-specific modules:
  - `language_detector.py`: FastText detection module.
  - `language_translator.py`: MarianMT translation module.

**Architectural Rule (`No Monoliths`):**
No file should exceed 300 lines. If a manager grows too large, refer to this md and split it logically into domain-specific modules.