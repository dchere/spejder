# spejder.jobs

**Purpose:**
Contains the core business domain logic for processing, scoring, classifying, and deduplicating job records.

**API:**
- `score_relevance(...)`
- `apply_relevance(...)` 
- `job_in_active_rescore_scope(row) -> bool` — `applied OR on_interview OR interview_stopped OR viewed==0` (Hidden jobs stay `viewed=0`, so they remain in active rescore scope)
- `rescore_jobs_if_active(db_path, profile, job_ids) -> int` — rescore scoped jobs; skips `manual_feedback` rows
- `rescore_active_jobs(db_path, profile) -> int` — rescore all jobs in active scope
- `merge_duplicate_positions(...)` — company+title dedup across all sources; oldest row kept; also invoked from GUI background sync via `workflows.deduplication.run_cross_source_dedupe`. Title trailing-city stripping and allowlist semantics are defined in `db/deduplication_utils.py` (see `db.md`).
- `merge_cross_source_duplicates(...)` — deprecated alias for `merge_duplicate_positions`
- `rescore_job_by_id(...)`
- `ingest_docs_to_db(...)` — `on_progress` reports running totals across files (`processed` + current file, and the same for inserted/skipped)
- `ingest_entries_to_db(...)` — upsert pre-built job entry dicts (portal sync, tests)

**Context:**
Originally a monolith mixing SQL execution and logic, `jobs.py` now adheres to the Single Responsibility Principle. All raw `sqlite3` executions have been moved entirely into `db.py`, and runtime dictionary parsing `profile.get(...)` has been replaced by typed attribute resolution via `config.py` (`AppConfig`).
`jobs.py` now acts safely purely as a business rules processor responding to typed objects.

**Dependencies:**
- `spejder.config`, `spejder.db`

**Position deduplication (`jobs/deduplication.py`):**
- `merge_duplicate_positions(db_path)` — batch pass groups by key, keeps oldest `created_at` (then lowest `id`), merges fields, deletes duplicate rows
- Dedupe keys canonicalize company names (`part of …` → parent) and titles first (gender markers stripped, common abbreviations expanded, trailing `, City` removed when city is allowlisted or matches row `place`) — see `db/deduplication_utils.py`
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
- `jobs2web.py`: Oracle Jobs2Web anchor parsing and Vestas/Danfoss/Novo Nordisk extractors (Python fallback; mirrored by shipped artifacts).
- `djinni_alerts.py`, `thehub_alerts.py`, `oracle_cx_alerts.py`: Other career-alert email extractors.
- `platforms_career_alerts.py`: Re-export barrel for career-alert extractors (stable import path).
- `artifact_schema.py` / `artifact_store.py` / `artifact_interpreter.py` / `artifact_heuristic.py` / `html_shrink.py` / `artifact_synth.py`: Declarative career-alert artifacts (see [`jobs/parsing/artifacts.md`](jobs/parsing/artifacts.md)).
- `artifacts/*.json`: Shipped Jobs2Web recipes (Vestas, Danfoss, Novo Nordisk).
- `core.py`: The aggregator coordinating all extractors (`extract_job_entries`); optional `artifacts=` / overlay dir / disable list. Without `artifacts_dir`, loads **shipped artifacts only** (no default `./career_alert_artifacts` merge).

**Career-alert artifacts:**
- Shipped JSON under `jobs/parsing/artifacts/`; user overlay dir from `career_alert_artifacts_dir` (default `./career_alert_artifacts`); overlay wins on same `id`. Overlay is loaded only when callers pass that dir (ingest/CLI); bare `extract_job_entries(doc)` is shipped-only.
- Match lists must be non-empty and non-blank at schema load; interpreter also fails closed on blank-only lists.
- Interpreter opcodes are a closed set (no `exec`); unknown ops fail validation at load.
- `extract_job_entries` runs enabled artifacts by priority, then built-ins; **artifact fields fill only when the built-in field is empty**. Links present only in the artifact map still become entries (needed for new-host synth).
- Opt-in synth (`career_alert_synth_enabled`): on ingest `found=0`, try a deterministic CTA heuristic first (iCIMS-style Apply-here + ancestor title), then optionally shrink HTML → local GGUF (`default_model`) → validate link/title ratios → write overlay only; failed synth leaves the `.eml`. Rejects empty/blank match lists and overly broad recovery. Hook lives in `jobs/ingestion.py`, not inside `extract_job_entries`. Ingest loads artifacts once per run (reload after successful synth).
- Jobindex / LinkedIn / generic HTML stay in Python; Jobs2Web Python modules remain dual-run fallbacks.

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
| Google Careers | `www.google.com` or `careers.google.com` … `/about/careers/applications/jobs/results/{id}-…` (query params preserved) | `_extract_google_entries_by_link` | Google Careers (`company=Google`) |

Djinni subscription digests use `div.card` blocks inside `table.table-cards`. Titles and descriptions may be English, Ukrainian, or mixed; Ukrainian text is translated during ingest via `language_manager` (see `spejder/managers/language_manager.md`). When both remote-only markers and subscription employment type appear, `work_type` prefers explicit part-time/full-time over remote.

Vestas, Danfoss, and Novo Nordisk career-alert emails share the Oracle Jobs2Web template (`agentjoblink` anchors); extractors key off the job-board host (`careers.vestas.com`, `jobs.danfoss.com`, `careers.novonordisk.com`), not CSS class alone. LinkedIn digests that link to Novo Nordisk career pages may include boilerplate such as `according to your selected` in the company slot; `_extract_novonordisk_entries_by_link` and `sanitize_company_name` normalize that to `Novo Nordisk`. `_parse_jobs2web_anchor_text` prefers middot (`·`) segments over dash splits; single-middot anchors keep the parenthetical-stripped text as title rather than splitting on interior hyphens.

**Merge order in `core.extract_job_entries`:** enabled artifacts (by priority) produce a by-link map first; then platform-specific fields from Google → The Hub → Djinni → Danfoss → Vestas → Novo Nordisk → Oracle CX → Demant → Jobindex → generic HTML (only fills fields not already set by a platform extractor); then artifact fields fill remaining empties; then `_provider_from_link` as fallback source. Artifact-only links (no built-in/`_is_job_link` hit) are appended after the links loop. The links loop uses the same platform priority when building entries from `doc["links"]`.

**Jobindex title/place heuristics:** `pick_jobindex_title` in `parsing/utils.py` avoids selecting company-like `/jobannonce/` anchors (exact company match or suffixes like `ApS`, `A/S`, `GmbH`, `Group`) and place-like listing tokens (e.g. `Tranbjerg J`, or the segment before `\d+ min`) before taking the longest remaining candidate. Company is taken from non-jobindex links first, then company-like jobannonce anchors, then non-title fragments — never from a role line (`…` / `søger` / `seeks` / `Butikselev - Lystrup` / `Audit Trainee i EY` / ALL-CAPS volunteer titles like `BLIV FRIVILLIG …`), punctuation (`(`), teasers (`Vil du …?` / `Hvad …` / `Som …`), or places. Retail titles with spaced `` - City`` and role stems (`elev`, `assistent`, `medarbejder`, `trainee`, `frivillig`) are treated as titles. When a listing place hint equals a short brand that also appears in another title as ``i Brand`` (e.g. `EY` from `Audit Trainee i EY 30 min`), `is_jobindex_company_echo_place` clears it so the brand stays company and place is not set to the company name. Brand recovery before the title allows up to four title-cased tokens (e.g. `Museum Ovartaci`). Listing place hints prefer a single city or ``City X`` district token (not duplicated ``Lystrup Lystrup`` from title+listing). When company is still empty, `infer_jobindex_company_from_title` recovers a leading `YouSee søger …` token. Listing place hints are case-sensitive so title fragments like `til patientklager` are not swallowed; `strip_title_overlap_from_place` also drops title/company prefixes from place. `merge_jobindex_place` combines trailing `i City` title suffixes with listing districts via space-join (e.g. `i Aarhus` + `8260 Tranbjerg J` → `Aarhus Tranbjerg J`); when the title already carries `City (District)` the parsed suffix is kept unchanged (e.g. `i Aarhus (Egå)` → `Aarhus (Egå)`). Listings with multi-location text (` or `, `/`) or a city prefix are kept verbatim. `peel_jobindex_trailing_place` splits bare multi-city suffixes (e.g. `Patent Paralegal Aarhus or Copenhagen`) when the place regex fails in single-anchor digests. Single-anchor digests can also recover title+place from compact text between company and `\d+ min`, including embedded ``… i City District`` without a postcode. Stored titles are not truncated when deriving place from `i City` suffixes. Display/report resolution (`_resolve_title_and_place`) still uses spaced ` - ` only — not bare hyphens in compounds like `Social- og`.

**Import policy:** `jobs/` and `jobs/parsing/` use explicit imports only (no `from spejder.db import *`). Each parsing submodule imports only the DB helpers it needs.
