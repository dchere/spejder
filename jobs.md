# spejder.jobs

**Purpose:**
Contains the core business domain logic for processing, scoring, classifying, and deduplicating job records.

**API:**
- `score_relevance(...)`
- `apply_relevance(...)` 
- `job_in_active_rescore_scope(row) -> bool` — `applied OR on_interview OR interview_stopped OR viewed==0`
- `rescore_jobs_if_active(db_path, profile, job_ids) -> int` — rescore scoped jobs; skips `manual_feedback` rows
- `rescore_active_jobs(db_path, profile) -> int` — rescore all jobs in active scope
- `merge_duplicate_positions(...)` — company+title dedup across all sources; oldest row kept; also invoked from GUI background sync via `workflows.deduplication.run_cross_source_dedupe`
- `merge_cross_source_duplicates(...)` — deprecated alias for `merge_duplicate_positions`
- `rescore_job_by_id(...)`
- `ingest_docs_to_db(...)`

**Context:**
Originally a monolith mixing SQL execution and logic, `jobs.py` now adheres to the Single Responsibility Principle. All raw `sqlite3` executions have been moved entirely into `db.py`, and runtime dictionary parsing `profile.get(...)` has been replaced by typed attribute resolution via `config.py` (`AppConfig`).
`jobs.py` now acts safely purely as a business rules processor responding to typed objects.

**Dependencies:**
- `spejder.config`, `spejder.db`

**Position deduplication (`jobs/deduplication.py`):**
- `merge_duplicate_positions(db_path)` — batch pass groups by key, keeps oldest `created_at` (then lowest `id`), merges fields, deletes duplicate rows
- `merge_cross_source_duplicates(...)` — deprecated alias
- Key/merge helpers live in [`db/deduplication_utils.py`](../db/deduplication_utils.py) to avoid circular imports with `upsert_job`

**Refactoring Update (Parsing Module):**
The previously monolithic `parsing.py` has been transitioned into a `spejder.jobs.parsing` subpackage to preserve file length constraints and separate concerns:
- `constants.py`: Regex patterns and tokens.
- `utils.py`: Reusable functional tools.
- `html_parser.py`: Functions manipulating `bs4.BeautifulSoup`.
- `text_parser.py`: Pure text transformations.
- `linkedin.py`: Rules specific to LinkedIn formatting inside jobs.
- `companies.py`: Entity and title inferences.
- `links.py`, `platforms.py`: Source routing via external links.
- `jobs2web.py`: Oracle Jobs2Web anchor parsing and Vestas/Danfoss/Novo Nordisk extractors.
- `djinni_alerts.py`, `thehub_alerts.py`, `oracle_cx_alerts.py`: Other career-alert email extractors.
- `platforms_career_alerts.py`: Re-export barrel for career-alert extractors (stable import path).
- `core.py`: The aggregator coordinating all extractors (`extract_job_entries`).

**Career-alert sources (email job alerts):**
| Source | Link pattern | Extractor | Provider label |
|--------|--------------|-----------|----------------|
| The Hub | `thehub.io/jobs/{hex}` (often via Mandrill track links) | `_extract_thehub_entries_by_link` | The Hub |
| Vestas | `careers.vestas.com/job/.../{id}` | `_extract_vestas_entries_by_link` | Vestas (`company=Vestas`) |
| Danfoss | `jobs.danfoss.com/job/...` | `_extract_danfoss_entries_by_link` | Danfoss (`company=Danfoss`) |
| Novo Nordisk | `careers.novonordisk.com/job/...` | `_extract_novonordisk_entries_by_link` | Novo Nordisk (`company=Novo Nordisk`) |
| Oracle CX | `*.fa.{region}.oraclecloud.com/.../CandidateExperience/.../job/{id}` (not Emerson host) | `_extract_oracle_cx_entries_by_link` | Oracle CX |
| Emerson Career Site | `hdjq.fa.us2.oraclecloud.com/.../CandidateExperience/.../job/{id}` | `_extract_oracle_cx_entries_by_link` | Emerson Career Site (`company=Emerson`) |
| Djinni | `djinni.co/jobs/{id}-{slug}` (often via Mandrill track links) | `_extract_djinni_entries_by_link` | Djinni |

Djinni subscription digests use `div.card` blocks inside `table.table-cards`. Titles and descriptions may be English, Ukrainian, or mixed; Ukrainian text is translated during ingest via `language_manager` (see `spejder/managers/language_manager.md`). When both remote-only markers and subscription employment type appear, `work_type` prefers explicit part-time/full-time over remote.

Vestas, Danfoss, and Novo Nordisk career-alert emails share the Oracle Jobs2Web template (`agentjoblink` anchors); extractors key off the job-board host (`careers.vestas.com`, `jobs.danfoss.com`, `careers.novonordisk.com`), not CSS class alone. LinkedIn digests that link to Novo Nordisk career pages may include boilerplate such as `according to your selected` in the company slot; `_extract_novonordisk_entries_by_link` and `sanitize_company_name` normalize that to `Novo Nordisk`. `_parse_jobs2web_anchor_text` prefers middot (`·`) segments over dash splits; single-middot anchors keep the parenthetical-stripped text as title rather than splitting on interior hyphens.

**Merge order in `core.extract_job_entries`:** platform-specific fields from Google → The Hub → Djinni → Danfoss → Vestas → Novo Nordisk → Oracle CX → Demant → Jobindex → generic HTML (only fills fields not already set by a platform extractor), then `_provider_from_link` as fallback source. The links loop uses the same priority when building entries from `doc["links"]`.

**Jobindex title place:** Titles ending with `i City (District)` (e.g. `… i Aarhus (Egå)`) yield `place` via `split_title_trailing_i_place` in `parsing/utils.py`. Display/report resolution (`_resolve_title_and_place`) uses spaced ` - ` only — not bare hyphens in compounds like `Social- og`.

**Import policy:** `jobs/` and `jobs/parsing/` use explicit imports only (no `from spejder.db import *`). Each parsing submodule imports only the DB helpers it needs.
