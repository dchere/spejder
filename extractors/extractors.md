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
  - `antipattern_validation.py` — synthetic job generation, LLM blocked-phrase matching, and multi-run extraction validation
  - `antipattern_sync.py` — orchestration, gates, CLI entry; re-exports synthesis/validation helpers
  - `user_sync.py` / `cleanup.py` — CLI commands for CV sync and DB cleanup
  - `ui.py` — skills tab data for the dashboard (`position_pct` = share of jobs with extracted skills; `occurrences` = pattern-learning counter shown as **Learned**)

## Blocked skills vs antipatterns

- `blocked_skills`: runtime deny list; filtered post-extraction and excluded from known patterns.
- `skill_extraction_antipatterns`: LLM-synthesized rules/examples injected into the job extraction prompt to reduce junk before post-filters run.
- Antipattern sync (`sync-antipatterns` CLI or end of GUI background sync): when `blocked_skills` count ≥ 15, synthesize **3** generic rules from blocked entries (random **sample of 40** for the synthesis prompt when the list is larger — sampling is intentionally non-deterministic; **full** blocked list used for per-candidate LLM matching), then for each candidate rule: LLM-match relevant blocked phrases from the full list (chunked at `ANTIPATTERN_PROMPT_INPUT_MAX` — equals `ANTIPATTERN_PROMPT_LIST_MAX` from `spejder.config`, default 150; off-list LLM matches ignored), cap matched blocked to that same limit before synthetic job generation and validation, generate a **per-candidate synthetic job posting** embedding matched blocked phrases + top DB skills by job link count (`skill_antipattern_good_skills_count`, default 20, clamped 1–150), and validate by comparing stable multi-run extractions (`skill_antipattern_validation_runs`, default 3; set intersection). Validation calls `_extract_job_skills_llm_path` with **`skip_blocked_filter=True`** so baseline and candidate comparisons measure prompt antipatterns only, not the post-extraction deny list; matched blocked-skill subsets are compared explicitly after extraction. Baseline (existing antipatterns only) must stably extract **all** matched blocked keys or the candidate is rejected (`baseline_missing_blocked`); with the candidate antipattern, matched blocked extraction must strictly decrease without dropping good/top-position skills (tolerance 1; skip reason `good_skills_lost`). Synthetic-job prompts cap blocked/good-skill inputs at `ANTIPATTERN_PROMPT_INPUT_MAX` (derived from `ANTIPATTERN_PROMPT_LIST_MAX` in `antipattern_synthesis.py`). Sync skips early when no ranked DB skills exist (`no_top_skills`). Top-position skill exclusion uses DB `name_key` normalization (`_normalize_skill_name_key`), not extractor `_normalize_skill_name`. Validation `good_skill_keys` use extractor `_normalize_skill_name` (lowercased), not DB `name_key`, so they align with `_extracted_skill_keys` / `_stable_extracted_keys`. Accept a candidate only if it reduces matched blocked-skill extraction; prune proven blocked entries and delete from SQLite. Logs synthesized rules, synthesis/match LLM output on parse failure, matched-blocked truncation, and per-candidate accept/skip reasons. Prompt injection uses the **newest** antipatterns (tail of the list, capped at 40). GUI background sync reloads profile and queues dashboard rebuild **only** when sync commits (`committed=True`).
- `ANTIPATTERN_PROMPT_LIST_MAX` / `ANTIPATTERN_PROMPT_INPUT_MAX` share the profile good-skills ceiling via `SKILL_ANTIPATTERN_GOOD_SKILLS_COUNT_MAX` in `spejder.config` (currently 150); changing that constant raises both caps together.

**LLM cost (no soft cap):** one synthesis call per sync, then per candidate with matched blocked phrases: `ceil(|blocked| / 150)` match calls + one synthetic job generation + `2 × skill_antipattern_validation_runs` extraction calls (baseline + with candidate). Rough **upper bound** on LLM calls per sync: `1 + Σ_candidates (match_chunks + 1 + 2 × validation_runs)` — about **3×** the extraction work of the old single shared test job when defaults apply (3 candidates, 3 runs each). Actual call counts are often lower because sync- and per-candidate skip reasons (`gate_failed`, `no_matched_blocked`, validation failures, etc.) short-circuit before later LLM steps. There is no budget guard; large blocked lists only increase match chunks.

**Skip reasons:**
- Sync-level (`stats.skip_reason`): `gate_failed` (blocked < 15 or no LLM), `no_blocked_input`, `no_top_skills`, `synthesis_error`, `synthesis_empty`, `no_candidates_accepted`.
- Per-candidate orchestration (`candidate_results[].skip_reason` before validation): `match_error`, `no_matched_blocked`, `synthetic_job_error`, `synthetic_job_empty`.
- Validation (`candidate_results[].skip_reason` from `_validate_antipattern_candidate`): `empty_rule_or_job`, `no_matched_blocked`, `baseline_empty`, `baseline_missing_blocked`, `with_pattern_empty`, `no_blocked_reduction`, `good_skills_lost`, `no_prunable_blocked`.

## Architectural Constraints
- **Pylint adherence**: Do not disable pylint rules (e.g. `# pylint: disable=...`) in this module. Refactor the code instead.
- **Pure Functions**: Keep parsing utilities and heuristics as pure functions where possible.
- **Normalization**: All skills should pass through a central `_normalize_skill_name` block before being added to any collection.
- **LLM Use**: Prefer fallback to local DB/regex patterns if the LLM is not provided or fails.
