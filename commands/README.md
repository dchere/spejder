# spejder.commands

**Purpose:**
Holds individual CLI command implementations (e.g., process-inbox, sync-skills, serve-gui, etc.).

**API:**
- Each command is a function or class, e.g. `process_inbox(args)`, `sync_user_skills(args)`

**Context:**
- Used by the CLI entry point to dispatch user commands.
- Each command should import only the modules it needs (core, language, skills, reporting, server, etc.)

**Dependencies:**
- Standard library, and any relevant spejder modules

**Example usage:**
```python
from spejder.commands.process_inbox import process_inbox
process_inbox(args)
```
