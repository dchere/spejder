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
  - `extraction_prompt.py` — LLM prompt and antipattern injection
  - `extraction_fallback.py` — regex/phrase fallback and blocked-skill filter (no per-job cap; quality filters only)
  - `extraction_llm.py` — LLM JSON parse path for job skills
  - `extraction.py` — orchestration facade (LLM + fallback + DB cache); no per-job skill count cap
  - `learning.py` — batch pattern learning from applied/relevant jobs
  - `antipattern_synthesis.py` — candidate selection, LLM JSON synthesis, profile merge helpers
  - `antipattern_validation.py` — synthetic job generation and multi-run extraction validation
  - `antipattern_sync.py` — orchestration, gates, CLI entry; re-exports synthesis/validation helpers
  - `user_sync.py` / `cleanup.py` — CLI commands for CV sync and DB cleanup
  - `ui.py` — skills tab data for the dashboard (`position_pct` = share of jobs with extracted skills; `occurrences` = pattern-learning counter shown as **Learned**)

## Blocked skills vs antipatterns

- `blocked_skills`: runtime deny list; filtered post-extraction and excluded from known patterns.
- `skill_extraction_antipatterns`: LLM-synthesized rules/examples injected into the job extraction prompt to reduce junk before post-filters run.
- Antipattern sync (`sync-antipatterns` CLI or end of GUI background sync): when `blocked_skills` count ≥ 15, synthesize **3** generic rules from blocked entries (spread **sample of 40** for the synthesis prompt when the list is larger; full capped list used for synthetic job validation), generate a **synthetic job posting** embedding blocked phrases + DB skills with `occurrences >= 1`, then validate each candidate by comparing stable multi-run extractions (3 runs, set intersection). Validation calls `_extract_job_skills_llm_path` with **`skip_blocked_filter=True`** so baseline and candidate comparisons measure prompt antipatterns only, not the post-extraction deny list; blocked-skill subsets are compared explicitly after extraction. Synthesis and synthetic-job prompts share the same input cap (`ANTIPATTERN_PROMPT_INPUT_MAX`, default 150). Accept a candidate only if it strictly reduces blocked-skill extraction without dropping seen skills; prune proven blocked entries and delete from SQLite. Logs synthesized rules, synthesis LLM output on parse failure, and per-candidate accept/skip reasons. Prompt injection uses the **newest** antipatterns (tail of the list, capped at 40). GUI background sync reloads profile and queues dashboard rebuild **only** when sync commits (`committed=True`).

## Architectural Constraints
- **Pylint adherence**: Do not disable pylint rules (e.g. `# pylint: disable=...`) in this module. Refactor the code instead.
- **Pure Functions**: Keep parsing utilities and heuristics as pure functions where possible.
- **Normalization**: All skills should pass through a central `_normalize_skill_name` block before being added to any collection.
- **LLM Use**: Prefer fallback to local DB/regex patterns if the LLM is not provided or fails.
