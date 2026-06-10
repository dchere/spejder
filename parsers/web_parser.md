# spejder.parsers.web_parser

**Purpose:**
Handles web scraping and HTML text extraction from external position pages URLs.

**API:**
- `_extract_position_page_text`
- `_get_position_page_context`
- `_extract_place_from_page_text` — Danfoss `Job Location (Short): …` hints from fetched page text

**Context:**
Extracted during the breakdown of the monolithic `inbox_parser` to separate concerns.
