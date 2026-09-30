# spejder.parsers.web_parser

**Purpose:**
Handles web scraping and HTML text extraction from external position pages URLs.

**API:**
- `_extract_position_page_text`
- `_get_position_page_context`
- `_extract_place_from_page_text` — Danfoss `Job Location (Short): …` hints from fetched page text
- `_append_page_context_to_raw_text` — merge listing text + `[POSITION_PAGE_CONTEXT {url}]` block; reserves up to `page_max_chars` (default 3000) inside `max_chars` (default 9000) by truncating the non-page prefix first so the page tail is not end-cut

**Context:**
Extracted during the breakdown of the monolithic `inbox_parser` to separate concerns.
