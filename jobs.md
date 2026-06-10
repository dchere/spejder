# spejder.jobs

**Purpose:**
Contains the core business domain logic for processing, scoring, classifying, and deduplicating job records.

**API:**
- `score_relevance(...)`
- `apply_relevance(...)` 
- `merge_cross_source_duplicates(...)` — also invoked from GUI background sync via `workflows.deduplication.run_cross_source_dedupe`
- `rescore_job_by_id(...)`
- `ingest_docs_to_db(...)`

**Context:**
Originally a monolith mixing SQL execution and logic, `jobs.py` now adheres to the Single Responsibility Principle. All raw `sqlite3` executions have been moved entirely into `db.py`, and runtime dictionary parsing `profile.get(...)` has been replaced by typed attribute resolution via `config.py` (`AppConfig`).
`jobs.py` now acts safely purely as a business rules processor responding to typed objects.

**Dependencies:**
- `spejder.config`, `spejder.db`

**Refactoring Update (Parsing Module):**
The previously monolithic `parsing.py` has been transitioned into a `spejder.jobs.parsing` subpackage to preserve file length constraints and separate concerns:
- `constants.py`: Regex patterns and tokens.
- `utils.py`: Reusable functional tools.
- `html_parser.py`: Functions manipulating `bs4.BeautifulSoup`.
- `text_parser.py`: Pure text transformations.
- `linkedin.py`: Rules specific to LinkedIn formatting inside jobs.
- `companies.py`: Entity and title inferences.
- `links.py`, `platforms.py`: Source routing via external links.
- `platforms_career_alerts.py`: Career-alert email extractors (The Hub, Vestas, Danfoss, Oracle CX, Djinni).
- `core.py`: The aggregator coordinating all extractors (`extract_job_entries`).

**Career-alert sources (email job alerts):**
| Source | Link pattern | Extractor | Provider label |
|--------|--------------|-----------|----------------|
| The Hub | `thehub.io/jobs/{hex}` (often via Mandrill track links) | `_extract_thehub_entries_by_link` | The Hub |
| Vestas | `careers.vestas.com/job/.../{id}` | `_extract_vestas_entries_by_link` | Vestas (`company=Vestas`) |
| Danfoss | `jobs.danfoss.com/job/...` | `_extract_danfoss_entries_by_link` | Danfoss (`company=Danfoss`) |
| Oracle CX | `*.fa.{region}.oraclecloud.com/.../CandidateExperience/.../job/{id}` (not Emerson host) | `_extract_oracle_cx_entries_by_link` | Oracle CX |
| Emerson Career Site | `hdjq.fa.us2.oraclecloud.com/.../CandidateExperience/.../job/{id}` | `_extract_oracle_cx_entries_by_link` | Emerson Career Site (`company=Emerson`) |
| Djinni | `djinni.co/jobs/{id}-{slug}` (often via Mandrill track links) | `_extract_djinni_entries_by_link` | Djinni |

Djinni subscription digests use `div.card` blocks inside `table.table-cards`. Titles and descriptions may be English, Ukrainian, or mixed; Ukrainian text is translated during ingest via `language_manager` (see `spejder/managers/language_manager.md`). When both remote-only markers and subscription employment type appear, `work_type` prefers explicit part-time/full-time over remote.

Vestas and Danfoss career-alert emails share the Oracle Jobs2Web template (`agentjoblink` anchors); extractors key off the job-board host (`careers.vestas.com` vs `jobs.danfoss.com`), not CSS class alone.

**Merge order in `core.extract_job_entries`:** platform-specific fields from Google → The Hub → Djinni → Danfoss → Vestas → Oracle CX → Demant → Jobindex → generic HTML (only fills fields not already set by a platform extractor), then `_provider_from_link` as fallback source. The links loop uses the same priority when building entries from `doc["links"]`.

**Import policy:** `jobs/` and `jobs/parsing/` use explicit imports only (no `from spejder.db import *`). Each parsing submodule imports only the DB helpers it needs.
