# Extractors Module

## Responsibilities
This module is responsible for parsing and extracting specialized entities (such as skills) from unstructured text, profiles, or job descriptions.

## Components
- `skill_extractor/`: Package for extracting and normalizing professional/technical skills from text.
  - `normalization.py` — canonical `_normalize_skill_name`
  - `constants.py` — shared regexes and cleanup heuristic sets
  - `utils.py` — text parsing, JSON helpers, regex generation
  - `filtering.py` — blocked/protected keys, phrase quality, cleanup reasons
  - `patterns.py` — skill pattern registry and profile-to-DB migration
  - `extraction.py` — LLM + regex job skill extraction and caching
  - `learning.py` — batch pattern learning from applied/relevant jobs
  - `antipattern_sync.py` — distill junk `blocked_skills` into `skill_extraction_antipatterns` for the extraction prompt; prune validated entries
  - `user_sync.py` / `cleanup.py` — CLI commands for CV sync and DB cleanup
  - `ui.py` — skills tab data for the dashboard

## Blocked skills vs antipatterns

- `blocked_skills`: runtime deny list; filtered post-extraction and excluded from known patterns.
- `skill_extraction_antipatterns`: LLM-synthesized rules/examples injected into the job extraction prompt to reduce junk before post-filters run.
- Rare sync (`sync-antipatterns` CLI or end of GUI background sync): junk-like blocked entries are clustered by LLM, **per-rule probe-filtered**, merged only if batch validation filters ≥1 blocked skill on sample jobs; otherwise the merge is rolled back (`batch_rejected`). Validated entries are removed from `blocked_skills` and SQLite. LLM synthesis uses at most **40** candidate phrases per call (`SYNTHESIS_INPUT_MAX`) with a higher token budget so JSON is not truncated. Prompt injection uses the **newest** antipatterns (tail of the list, capped at 40). On commit, the profile is saved first, then SQLite deletes run per validated skill; there is no cross-store transaction, so a DB delete failure after a successful profile save leaves the profile updated and the skill row still in SQLite. Skipped runs log `skip_reason` (e.g. `synthesis_empty`, `gate_failed`); GUI background sync does not queue a dashboard rebuild when skipped.

## Architectural Constraints
- **Pylint adherence**: Do not disable pylint rules (e.g. `# pylint: disable=...`) in this module. Refactor the code instead.
- **Pure Functions**: Keep parsing utilities and heuristics as pure functions where possible.
- **Normalization**: All skills should pass through a central `_normalize_skill_name` block before being added to any collection.
- **LLM Use**: Prefer fallback to local DB/regex patterns if the LLM is not provided or fails.
