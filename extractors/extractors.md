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
  - `extraction_prompt.py` — LLM prompt construction
  - `extraction_fallback.py` — regex/phrase fallback
  - `extraction_llm.py` — LLM JSON parse path for job skills
  - `extraction.py` — orchestration facade (LLM + fallback + DB cache); no per-job skill count cap
  - `learning.py` — batch pattern learning from applied/relevant jobs (`get_jobs_by_category(..., exclude_hidden=False)` so parked relevant Hidden jobs still contribute)
  - `bad_cloud.py` — bigram/unigram toxicity scoring, cloud ingest, threshold calibration, blocked-list pruning
  - `user_sync.py` / `cleanup.py` — CLI commands for CV sync and DB cleanup
  - `ui.py` — skills tab data for the dashboard (`position_pct` = share of jobs with extracted skills; `occurrences` = pattern-learning counter shown as **Learned**; `added_at` copied from DB `created_at`, empty for profile-only rows; `SKILLS_EMPTY_ADDED_AT_SORT` (`"0000"`) sentinel for profile-only sort keys — no real ISO date starts with `0000`; default server-side order uses stable double-sort: name A→Z first, then `added_at` DESC with profile-only rows last; sentinel forwarded to `dashboard.html` via `dashboard_manager` Jinja context for client-side sort)

## Blocked skills and bad cloud filtering

- `blocked_skills`: runtime exact-match deny list; filtered post-extraction and excluded from known patterns. Manual blocks also feed the bad cloud (see below).
- **Bad cloud** (`bad_ngram_weights` in SQLite): persistent accumulator of bigram/unigram weights from manually blocked skills. Weights **increment only** on block; removing a skill from `blocked_skills` does **not** decrement cloud weights (historical ghost effect).
- **Extraction filter order** (`filtering._filter_extracted_skills`): whitelist (`_whitelist_skill_keys`: DB patterns except auto-`detected` with zero occurrences, plus profile `user_skills` / `known_skill_patterns`) → exact `blocked_skills` → toxicity score for unseen candidates. Score = average bad-cloud weight per ngram; reject when `score >= skill_bigram_toxicity_threshold`. Automatic score-based rejects do **not** add to `blocked_skills` or update the cloud. Cached `job_skills` reads use the same filter.
- **Manual block hook** (`bad_cloud.on_skills_blocked`): ingest ngrams (+1 each) and prune `blocked_skills` entries whose own score already meets the current stored threshold. Does **not** recalibrate the threshold.
- **One-time seed** (`bad_cloud.ensure_bad_cloud_initialized`): GUI background sync after blocked DB cleanup; seeds cloud from existing `blocked_skills` when `bad_cloud_seeded` is false, prunes only entries **not** in the seeded batch. Threshold calibration is separate (see below).
- **Threshold recalibration** (`bad_cloud.recalibrate_and_store_threshold`): single write path — runs on every GUI background sync after cloud init. Uses p95 of **mature** non-blocked enabled `skill_patterns` (`created_at` older than 1 day; falls back to all eligible non-blocked patterns if none are mature) vs p50 of `blocked_skills`, with `skill_bigram_threshold_margin` and floor `0.1`. Persists result to `skill_bigram_toxicity_threshold` (cache for extraction between syncs). Empty cloud → `inf` / clears stored threshold.
- **Tokenization** (`bad_cloud._tokenize_for_cloud`): `_normalize_skill_name`, then alphanumeric tokens; stop words are **not** stripped for scoring (prompt rules already discourage stopword-only skills). Single-word skills use unigram keys; multi-word skills use overlapping bigrams.
- **Threshold calibration** (`bad_cloud.calibrate_threshold`): formula `max(0.1, p95_good + margin * max(0, p50_bad - p95_good))` with mature good set as above. Empty cloud → threshold `inf` (no auto-rejects).

## Architectural Constraints
- **Pylint adherence**: Do not disable pylint rules (e.g. `# pylint: disable=...`) in this module. Refactor the code instead.
- **Pure Functions**: Keep parsing utilities and heuristics as pure functions where possible.
- **Normalization**: All skills should pass through a central `_normalize_skill_name` block before being added to any collection.
- **LLM Use**: Prefer fallback to local DB/regex patterns if the LLM is not provided or fails.
