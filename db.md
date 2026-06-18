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
- `delete_skill_from_db(db_path, skill_name)` — removes `skill_patterns` rows and all `job_skills` links for one normalized skill key; returns `{skill_rows_deleted, job_skill_links_deleted, affected_job_ids}`
- `count_jobs_with_skill_links(db_path)` — distinct jobs with at least one `job_skills` row (denominator for Skills tab job share %)
- `get_top_skills_by_job_links(db_path, limit, exclude_keys=None)` — skill names ranked by `job_skills` link count (`occurrences >= 1`, **requires at least one** `job_skills` link via inner join); excluding normalized keys; used by antipattern sync for good-skill selection
- `cleanup_blocked_skills_from_db(db_path, blocked_skills)` — dedupes blocked skill names via `_normalize_skill_name_key`, delegates each to `delete_skill_from_db`, returns aggregated `{skills_processed, skill_rows_deleted, job_skill_links_deleted, affected_job_ids}`
(And many more database query functions)

**Context:**
Extracted from `jobs.py`. The rest of the application (including business logic in `jobs.py` and `workflows.py`) only interacts with abstract Python data structures (lists, dicts, tuples) and never executes SQL directly. This ensures complete isolation of the persistence layer.

**Query modules (`queries.py` facade):**
- `queries_listings.py` — category/company/applied listings and viewed counts
  - `get_applied_jobs()` — `applied=1` and not on interview/stopped (Applied tab)
  - `get_all_applied_jobs()` — all `applied=1` rows (skill learning, enrichment, raw-text)
  - `get_interview_jobs()` — `applied=1 AND on_interview=1`
  - `get_stopped_interview_jobs()` — `applied=1 AND interview_stopped=1`
- `queries_refresh.py` — description refresh, scoring candidate rows, active rescore scope
  - `get_jobs_for_active_rescore()` — jobs where `applied=1 OR on_interview=1 OR interview_stopped=1 OR viewed=0`
- `queries_signals.py` — dedupe, merge, and suggestion queries
- `queries_rows.py` — shared SQL row → dict mappers

`db/__init__.py` re-exports the public query API from `queries.py`; callers should not import submodules unless testing internals.

**Dependencies:**
- `sqlite3`, `spejder.config`

**Position deduplication (`deduplication_utils.py`):**
- `_position_dedupe_key(company, title)` — normalized `company|title` key for all sources
- `_canonicalize_title_for_dedupe(title)` — strips EU gender markers like `(m/f/d)` and expands common abbreviations (`SW`→`Software`, `Sr.`→`Senior`) before keying; `(Senior)` role qualifiers are kept
- `_merge_duplicate_into_keeper`, `_merge_raw_text` — shared merge rules used by `upsert_job` and `jobs/deduplication.merge_duplicate_positions`

**Link normalization (`utils.py`):**
- `_decode_mandrill_track_link`: unwraps Mandrill `track/click` URLs (base64 JSON payload → destination URL). Requires `base64` and `html.unescape`.
- `_normalize_position_link`: canonical position URLs per provider. Career-alert additions:
  - **The Hub:** `https://thehub.io/jobs/{hex}` (12+ hex chars); Mandrill links decoded first.
  - **Danfoss:** `jobs.danfoss.com/job/...` — strips query/fragment, keeps `scheme://netloc/path`.
  - **Vestas:** `careers.vestas.com/job/.../{numeric_id}` — strips query/fragment, keeps `scheme://netloc/path`.
  - **Oracle CX:** `*.fa.[a-z0-9]+.oraclecloud.com` with `/CandidateExperience/` and `/job/{id}` — strips `:443`/`:80`, canonical `https://{netloc}{path}`.
  - **Emerson Career Site:** same Oracle FA path shape on host `hdjq.fa.us2.oraclecloud.com` (`EMERSON_ORACLE_FA_HOST` in `utils.py`).
- `_provider_from_link`: returns display source label (`The Hub`, `Vestas`, `Oracle CX`, `Emerson Career Site`, `Djinni`, etc.). Emerson host is checked before the generic Oracle FA rule.
- `_is_djinni_position_link`: shared Djinni job URL check (`djinni.co/jobs/{numeric_id}`); used by `_is_job_link`, `_normalize_position_link`, and `_provider_from_link`.
- `get_job_link` (`connection.py`): returns `position_link` row for a job id; lives next to `_connect` to avoid circular imports with `utils.py`.
- `ensure_db` (`connection.py`): after source backfill from `_provider_from_link`, migrates existing Emerson Oracle FA rows — `source` → `Emerson Career Site` (when blank or `Oracle CX`), `company` → `Emerson` (when blank or `Emerson Career Site`).

**Interview stage columns (`jobs` table):**
- `on_interview INTEGER DEFAULT 0` — mutually exclusive with `interview_stopped`; only settable when `applied=1`
- `interview_stopped INTEGER DEFAULT 0` — clears `on_interview` when set; only settable when `applied=1`
- `company_feedback TEXT` — free-text notes on stopped cards; only writable when `interview_stopped=1`
- `set_job_applied(False)`, `set_job_viewed(False)`, `set_job_feedback("not relevant")`, and `batch_update_and_delete_jobs` update tuples with `applied=0` all clear interview fields via shared `_INTERVIEW_FIELDS_CLEAR` (`on_interview=0`, `interview_stopped=0`, `company_feedback=NULL`, `cover_letter=NULL`, `cover_letter_requested=0`) alongside `applied=0`
- `set_job_interview_stopped(False)` (unstop) clears only `interview_stopped`; preserves `company_feedback`
- Mutations: `set_job_on_interview`, `set_job_interview_stopped`, `set_job_company_feedback`
- Legacy `description_raw` → `jobs_new` migration copies `on_interview`, `interview_stopped`, and `company_feedback` when source columns exist
- Duplicate link-recognition patterns also live in `jobs/parsing/links.py` (`_is_job_link`) per project convention.

**Cover letter columns (`jobs` table):**
- `cover_letter TEXT` — saved cover letter text for an applied job; one-time write via `set_job_cover_letter`
- `cover_letter_requested INTEGER DEFAULT 0` — user checked “Cover letter” on the applied card; toggled via `set_job_cover_letter_requested` until text is saved
- Cleared with `_INTERVIEW_FIELDS_CLEAR` when `applied=0`
