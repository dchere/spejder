# spejder.parsers.email_parser

**Purpose:**
Loads and parses job-alert email files (`.eml` only). Extracts plain text, HTML body parts, links, subject, and From from MIME multipart messages.

**API:**
- `parse_email_file` — parse a single `.eml` file
- `load_files` — walk a folder for `.eml` files (default extension list)

**Output shape:**
Each parsed document is a dict with `id`, `path`, `text`, `html`, `links`, `title`, `from`. The `html` field contains joined `text/html` MIME parts (used by career-alert extractors downstream). `from` is the decoded From header (used by the CTA heuristic company guess).

**Context:**
Originally `spejder.parser`, moved into `parsers/` to consolidate parsing logic. Standalone `.html`/`.htm` inbox files are no longer supported — save emails as `.eml` instead.
