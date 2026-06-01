# spejder.jobs

**Purpose:**
Contains the core business domain logic for processing, scoring, classifying, and deduplicating job records.

**API:**
- `score_relevance(...)`
- `apply_relevance(...)` 
- `merge_cross_source_duplicates(...)`
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
- `core.py`: The aggregator coordinating all extractors (`extract_job_entries`).
