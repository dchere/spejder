# spejder.db

**Purpose:**
Implements the Repository Pattern, acting as the strict single source of truth for all SQLite database operations.

**API:**
- `init_db(db_path)`
- `get_relevant_jobs(db_path, ...)`
- `upsert_job(db_path, entry)` — re-ingesting an existing `position_link` merges empty `place`/`company` and longer `raw_text`; a new URL with matching normalized company+title updates the **oldest** matching row instead of inserting
- `set_job_place(db_path, job_id, place)`
- `batch_update_and_delete_jobs(db_path, updates, deletes)`
- `replace_job_skills(db_path, job_id, skill_names) -> bool` — delete/replace job skill links; returns `False` when normalized key set unchanged; `set_job_skills` delegates here
- `get_job_skills(db_path, job_id)` — cached skill names for one job
- `get_job_skills_for_jobs(db_path, job_ids)` — batch variant; returns `{job_id: [names]}` in one query
- `delete_skill_from_db(db_path, skill_name)` — removes `skill_patterns` rows and all `job_skills` links for one normalized skill key; returns `{skill_rows_deleted, job_skill_links_deleted, affected_job_ids}`
- `count_jobs_with_skill_links(db_path)` — distinct jobs with at least one `job_skills` row (denominator for Skills tab job share %)
- `get_top_skills_by_job_links(db_path, limit, exclude_keys=None)` — skill names ranked by `job_skills` link count; excluding normalized keys. **Dual filter for eligible skills:** `INNER JOIN job_skills` (only skills with at least one job link) **and** `COALESCE(sp.occurrences, 0) >= 1` (pattern-learning counter must be positive — excludes orphan `skill_patterns` rows that never graduated from the learning pipeline). **Tie-break:** equal link counts sort alphabetically by `sp.name` (`ORDER BY link_count DESC, sp.name ASC`).
- `count_bad_ngrams(db_path)` — row count in `bad_ngram_weights`
- `get_bad_ngram_weights(db_path, ngrams)` — batch lookup `{(ngram, gram_size): weight}`
- `upsert_bad_ngrams(db_path, ngrams, increment=1)` — increment weights for bigram/unigram keys (accumulator; never decrements)
- `upsert_bad_ngram_counts(db_path, counts)` — batch variant `{ (ngram, gram_size): increment }` in one transaction
- `cleanup_blocked_skills_from_db(db_path, blocked_skills)` — dedupes blocked skill names via `_normalize_skill_name_key`, delegates each to `delete_skill_from_db`, returns aggregated `{skills_processed, skill_rows_deleted, job_skill_links_deleted, affected_job_ids}`
- `get_skill_patterns(db_path, enabled_only=True)` — returns skill pattern rows including `created_at` (ISO timestamp when first stored in `skill_patterns`); default SQL order remains `weight DESC, occurrences DESC, name ASC` for callers that rely on it
- (And many more database query functions)

**Context:**
Extracted from `jobs.py`. The rest of the application (including business logic in `jobs.py` and `workflows.py`) only interacts with abstract Python data structures (lists, dicts, tuples) and never executes SQL directly. This ensures complete isolation of the persistence layer.

**Query modules (`queries.py` facade):**
- `queries_listings.py` — category/company/applied/hidden listings and viewed counts
  - `get_applied_jobs()` — `applied=1` and not on interview/stopped (Applied tab); excludes `hidden=1`
  - `get_all_applied_jobs()` — all `applied=1` rows (skill learning, enrichment, raw-text)
  - `get_interview_jobs()` — `applied=1 AND on_interview=1`; excludes `hidden=1`
  - `get_stopped_interview_jobs()` — `applied=1 AND interview_stopped=1`; excludes `hidden=1`
  - `get_hidden_jobs()` / `get_hidden_jobs_count()` — `hidden=1` for Hidden tab
  - `get_viewed_today_jobs(db_path, since_iso, limit=0)` — `viewed=1 AND applied=0 AND COALESCE(hidden,0)=0 AND updated_at IS NOT NULL AND updated_at >= since_iso`, order `updated_at DESC` (Edited today tab; uncapped when `limit=0`)
  - `local_day_start_utc_iso()` — local timezone midnight → UTC ISO (same string style as mutation timestamps); callers pass this as `since_iso`
  - Applied-stage listings sort by `(applied_at IS NULL), applied_at DESC, updated_at DESC` (dated rows first; null `applied_at` last)
- `queries_refresh.py` — description refresh, scoring candidate rows, active rescore scope
  - `get_jobs_for_active_rescore()` — jobs where `applied=1 OR on_interview=1 OR interview_stopped=1 OR viewed=0`
- `queries_signals.py` — dedupe, merge, and suggestion queries
- `queries_rows.py` — shared SQL row → dict mappers

`db/__init__.py` re-exports the public query API from `queries.py`; callers should not import submodules unless testing internals.

**Dependencies:**
- `sqlite3`, `spejder.config`

**Position deduplication (`deduplication_utils.py`):**
- `_position_dedupe_key(company, title, place="")` — normalized `company|title` key for all sources
- `_canonicalize_company_for_dedupe(company)` — when the label contains `part of …` at a phrase boundary (`^` or after `,`/`;`), keys use the parent segment after that phrase (e.g. subsidiary names → parent group); does not match inside words like `Counterpart of`
- `_canonicalize_title_for_dedupe(title, place="")` — strips EU gender markers like `(m/f/d)`, expands common abbreviations (`SW`→`Software`, `Sr.`→`Senior`), and removes a trailing `, City` when `City` matches `place` (exact or `place` prefix with word boundary after the city key) or when `City` is in `DANISH_CITY_ALLOWLIST_KEYS` and `place` is empty/`unknown` or matches `place`; `(Senior)` role qualifiers are kept
- `DANISH_CITY_ALLOWLIST_KEYS` — maintain entries as ASCII-normalized keys (`ø`→`o`, `æ`→`ae`, `å`→`a` before keying); runtime folding applies the same transliteration for lookup
- **Tradeoff:** same title at the same company with different allowlisted trailing cities may still merge when `place` is empty/`unknown` (e.g. `Engineer, Copenhagen` vs `Engineer, Odense`); tightening requires a known `place` that does not match the trailing city
- `_merge_duplicate_into_keeper`, `_merge_raw_text` — shared merge rules used by `upsert_job` and `jobs/deduplication.merge_duplicate_positions`; merge ORs `hidden` onto the keeper unless viewed/applied wins (then `hidden=0`)
- `get_all_jobs_for_dedupe` / `_row_to_dedupe_item` include `hidden`; `batch_update_and_delete_jobs` update tuples are `(company, title, place, work_type, raw_text, viewed, applied, hidden, updated_at, id)`

**Link normalization (`utils.py`):**
- `_decode_mandrill_track_link`: unwraps Mandrill `track/click` URLs (base64 JSON payload → destination URL). Requires `base64` and `html.unescape`.
- `_normalize_position_link`: canonical position URLs per provider. Career-alert additions:
  - **The Hub:** `https://thehub.io/jobs/{hex}` (12+ hex chars); Mandrill links decoded first.
  - **Danfoss:** `jobs.danfoss.com/job/...` — strips query/fragment, keeps `scheme://netloc/path`.
  - **Vestas:** `careers.vestas.com/job/.../{numeric_id}` — strips query/fragment, keeps `scheme://netloc/path`.
  - **Oracle CX:** `*.fa.[a-z0-9]+.oraclecloud.com` with `/CandidateExperience/` and `/job/{id}` — strips `:443`/`:80`, canonical `https://{netloc}{path}`.
  - **Emerson Career Site:** same Oracle FA path shape on host `hdjq.fa.us2.oraclecloud.com` (`EMERSON_ORACLE_FA_HOST` in `utils.py`).
  - **Google Careers:** `www.google.com/about/careers/applications/jobs/results/{id}-…` (or same path on `careers.google.com`) — passthrough: keeps original scheme, host, path, and full query string; HTML-entity unescapes the link (`&amp;` → `&`); strips fragment and trailing slash on path only (does not rewrite to `careers.google.com`).
- `_provider_from_link`: returns display source label (`The Hub`, `Vestas`, `Oracle CX`, `Emerson Career Site`, `Djinni`, `Google Careers`, etc.). Emerson host is checked before the generic Oracle FA rule.
- `_is_djinni_position_link`: shared Djinni job URL check (`djinni.co/jobs/{numeric_id}`); used by `_is_job_link`, `_normalize_position_link`, and `_provider_from_link`.
- `get_job_link` (`connection.py`): returns `position_link` row for a job id; lives next to `_connect` to avoid circular imports with `utils.py`.
- `ensure_db` (`connection.py`): after source backfill from `_provider_from_link`, migrates existing Emerson Oracle FA rows — `source` → `Emerson Career Site` (when blank or `Oracle CX`), `company` → `Emerson` (when blank or `Emerson Career Site`).

**Interview stage columns (`jobs` table):**
- `on_interview INTEGER DEFAULT 0` — mutually exclusive with `interview_stopped`; only settable when `applied=1`
- `interview_stopped INTEGER DEFAULT 0` — clears `on_interview` when set; only settable when `applied=1`
- `company_feedback TEXT` — free-text notes on stopped cards; only writable when `interview_stopped=1`
- `set_job_applied(False)`, `set_job_viewed(False)`, `set_job_feedback("not relevant")`, `set_job_hidden(True)`, and `batch_update_and_delete_jobs` update tuples with `applied=0` all clear interview fields via shared `_INTERVIEW_FIELDS_CLEAR` (`on_interview=0`, `interview_stopped=0`, `company_feedback=NULL`, `cover_letter=NULL`, `cover_letter_requested=0`, `applied_at=NULL`) alongside `applied=0`
- `set_job_applied(True)` sets `applied_at=COALESCE(applied_at, ?)` so re-saving an already-applied job does not shift the date; first apply and re-apply after unapply get a fresh timestamp; also clears `hidden=0`
- `set_job_viewed(True)` also clears `hidden=0`
- `ensure_db` backfills `applied_at` from `updated_at` for existing `applied=1` rows when the column is missing or null
- `set_job_interview_stopped(False)` (unstop) clears only `interview_stopped`; preserves `company_feedback`
- Mutations: `set_job_on_interview`, `set_job_interview_stopped`, `set_job_company_feedback`
- Legacy `description_raw` → `jobs_new` migration copies `on_interview`, `interview_stopped`, and `company_feedback` when source columns exist; both legacy `jobs_new` CREATE TABLE blocks include `hidden INTEGER DEFAULT 0` (value still backfilled via ALTER when missing on live `jobs`)
- Duplicate link-recognition patterns also live in `jobs/parsing/links.py` (`_is_job_link`) per project convention.

**Hidden column (`jobs.hidden`):**
- `hidden INTEGER DEFAULT 0` — parks a position on the Hidden tab without changing `category`, scores, description, or `job_skills`
- `set_job_hidden(True)`: `hidden=1`, `viewed=0`, `applied=0` + `_INTERVIEW_FIELDS_CLEAR`; does not set `viewed=1`
- `set_job_hidden(False)`: `hidden=0` only (caller UI restores Relevant / Not relevant from existing `category`)
- Mutual exclusion: `hidden=1` cannot coexist with `applied=1` or `viewed=1` (apply/viewed-true clear hidden; hide clears applied/viewed pipeline; `upsert_job` merge UPDATE uses `_HIDDEN_CLEAR_IF_VIEWED_OR_APPLIED`; `batch_update_and_delete_jobs` writes explicit `hidden` from the merge tuple and forces `0` when `viewed=1` or `applied=1`; `_merge_duplicate_into_keeper` ORs `hidden` from keeper/duplicate in-memory, then clears to `0` when either flag is 1)
- `get_hidden_jobs` / `get_hidden_jobs_count` — `COALESCE(hidden,0)=1`, order `relevance_score DESC, updated_at DESC`
- `get_viewed_today_jobs` — viewed, non-applied, non-hidden rows with `updated_at >= since_iso` (local day start as UTC ISO via `local_day_start_utc_iso`); order `updated_at DESC`; no separate count helper (UI uses `len(items)`). Tradeoff: other writes that bump `updated_at` can pull already-viewed jobs into the tab for the local day.
- Category helpers (`get_jobs_by_category`, count, paged) default `exclude_hidden=True` → `AND COALESCE(hidden,0)=0`; skill learning passes `exclude_hidden=False`
- Applied / interview / stopped getters exclude hidden rows; `get_relevant_jobs` (inbox summary) excludes hidden
- Included in `_JOB_SELECT_COLS` and row mappers; `get_jobs_by_company` returns `hidden` for company dashboard partitioning
- No 90-day retention exemption for Hidden (same age-out as other non-interview rows)

**Cover letter columns (`jobs` table):**
- `cover_letter TEXT` — saved cover letter text for an applied job; one-time write via `set_job_cover_letter`
- `cover_letter_requested INTEGER DEFAULT 0` — user checked “Cover letter” on the applied card; toggled via `set_job_cover_letter_requested` until text is saved
- Cleared with `_INTERVIEW_FIELDS_CLEAR` when `applied=0`

**Applied date (`jobs.applied_at`):**
- `applied_at TEXT` — ISO timestamp set on first apply; preserved on re-apply while still applied; cleared when `applied=0`
- Listed in `_JOB_SELECT_COLS` (shared by all full-row listing queries in `queries_listings.py`) and mapped in `_map_full_job_row` / `_map_applied_job_row`
- **Two-layer sort for applied-stage dashboard tabs:** DB queries above establish primary order by apply date; `dashboard_sorting._sort_applied_positions` re-sorts in Python for completion/viewed/score rules and uses `applied_at` as a tiebreaker (newer first; missing dates last)

**90-day retention (`ensure_db`):**
- Auto-prunes rows where `created_at` is older than `JOB_RETENTION_DAYS` (90)
- Exempt: `applied=1 AND (on_interview=1 OR interview_stopped=1)` — interview/stopped pipeline jobs are kept
- Plain `applied=1` rows (not on interview/stopped) still age out by `created_at`
- Demotion edge case: unchecking **On interview** or **Stopped** on an old retained job removes the exemption; the next `ensure_db` prunes it like any other plain applied row
