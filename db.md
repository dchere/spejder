# spejder.db

**Purpose:**
Implements the Repository Pattern, acting as the strict single source of truth for all SQLite database operations.

**API:**
- `init_db(db_path)`
- `get_relevant_jobs(db_path, ...)`
- `upsert_job(db_path, entry)`
- `batch_update_and_delete_jobs(db_path, updates, deletes)`
(And many more database query functions)

**Context:**
Extracted from `jobs.py`. The rest of the application (including business logic in `jobs.py` and `workflows.py`) only interacts with abstract Python data structures (lists, dicts, tuples) and never executes SQL directly. This ensures complete isolation of the persistence layer.

**Query modules (`queries.py` facade):**
- `queries_listings.py` — category/company/applied listings and viewed counts
- `queries_refresh.py` — description refresh and scoring candidate rows
- `queries_signals.py` — dedupe, merge, and suggestion queries
- `queries_rows.py` — shared SQL row → dict mappers

`db/__init__.py` re-exports the public query API from `queries.py`; callers should not import submodules unless testing internals.

**Dependencies:**
- `sqlite3`, `spejder.config`

**Link normalization (`utils.py`):**
- `_decode_mandrill_track_link`: unwraps Mandrill `track/click` URLs (base64 JSON payload → destination URL). Requires `base64` and `html.unescape`.
- `_normalize_position_link`: canonical position URLs per provider. Career-alert additions:
  - **The Hub:** `https://thehub.io/jobs/{hex}` (12+ hex chars); Mandrill links decoded first.
  - **Vestas:** `careers.vestas.com/job/.../{numeric_id}` — strips query/fragment, keeps `scheme://netloc/path`.
  - **Oracle CX:** `*.fa.[a-z0-9]+.oraclecloud.com` with `/CandidateExperience/` and `/job/{id}` — strips `:443`/`:80`, canonical `https://{netloc}{path}`.
  - **Emerson Career Site:** same Oracle FA path shape on host `hdjq.fa.us2.oraclecloud.com` (`EMERSON_ORACLE_FA_HOST` in `utils.py`).
- `_provider_from_link`: returns display source label (`The Hub`, `Vestas`, `Oracle CX`, `Emerson Career Site`, etc.). Emerson host is checked before the generic Oracle FA rule.
- `ensure_db` (`connection.py`): after source backfill from `_provider_from_link`, migrates existing Emerson Oracle FA rows — `source` → `Emerson Career Site` (when blank or `Oracle CX`), `company` → `Emerson` (when blank or `Emerson Career Site`).
- Duplicate link-recognition patterns also live in `jobs/parsing/links.py` (`_is_job_link`) per project convention.
