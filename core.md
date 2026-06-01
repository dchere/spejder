# spejder.core

**Purpose:**
Central, lightweight utilities for generalized execution behaviors, logging, and legacy config wrappers.

**API:**
- `load_profile(profile_path)` (Delegates to `config.AppConfig.load()`)
- `save_profile(profile, profile_path)` (Delegates to `config.AppConfig.save()`)
- `print_step(message)`
- `fail_init(message)`

**Context:**
- Used by all modules that need standard logging steps or need to load config dynamically.
- `load_profile` now natively returns the Pydantic `AppConfig` class objects standardizing configurations project-wide.

**Dependencies:**
- `spejder.config`
