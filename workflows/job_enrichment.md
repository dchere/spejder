# spejder.workflows.job_enrichment

**Purpose:**
Performs text enrichment, summary generation, text fallback formatting, and title extraction logic for positions traversing the inbox process.

**API:**
- `make_translate_job_entry_for_storage(runtime_profile, text_translation_cache, title_translation_cache) -> Callable[[dict], dict]` shared ingest transform factory for translating `raw_text` and deriving `title_english` with fallback behavior unchanged.

**Translation cache ownership:**
- `text_translation_cache` and `title_translation_cache` are caller-owned, invocation-scoped dictionaries.
- The factory reuses those dictionaries across records within one ingest run; callers decide lifecycle and must not treat them as module-level globals.

**Context:**
Originally part of `inbox_parser`, split out to reduce file sizes and enforce single responsibility.
