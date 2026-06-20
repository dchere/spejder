# spejder.workflows.user_portrait

**Purpose:**
Build, regenerate, and persist a user professional portrait from CV text, profile skills, applied jobs, cover letters, and stopped-interview feedback. Used for cover-letter context (future) and manual review in the dashboard Portrait panel (toolbar icon).

**API:**
- `portrait_file_path(profile)` / `cv_file_path(profile)` — resolved paths via `resolve_user_path`
- `load_portrait(path)` / `save_portrait(path, text)` — read committed text; atomic write
- `embed_portrait_for_textarea(text)` — neutralize `</textarea` breakout for static HTML embed; preserves `&`, `<`, etc.
- `collect_portrait_context(db_path, profile, cv_path=None)` — plain-text input block for LLM (budget: `max_input_chars`); job skills loaded in one batch via `get_job_skills_for_jobs`; at most 50 most recently updated applied jobs
- `portrait_has_context(db_path, profile, cv_path=None)` — true when CV, skills, or applied jobs exist
- `build_portrait_prompt(current_portrait, context)` — minimal-change regeneration prompt
- `generate_portrait_draft(llm, db_path, profile, current_portrait="", cv_path=None)` — LLM call (`portrait_max_tokens`)
- `render_portrait_diff_html(old, new)` — line-level git-style diff HTML

**Persistence:**
- Default file: `./portrait.txt` (`AppConfig.default_portrait_path`) at workspace root
- CV input: `./CV` (`AppConfig.default_cv_path`) via `load_cv_text`

**GUI:**
- Portrait panel in `templates/dashboard.html` (opened via toolbar icon `#btn-portrait`, not the mode tab row)
- Unsaved draft or manual edits trigger a confirm when leaving the Portrait panel and a `beforeunload` prompt on page close/reload
- `GET /api/portrait` — committed text
- `POST /api/portrait/generate` — sync LLM; returns `{ draft, committed, diff_html }` (does not write file)
- `POST /api/portrait/save` — writes committed portrait (no dashboard rebuild)

**Context:**
Regeneration instructs the LLM to preserve accurate prior wording. Draft is client-side until Save. Dashboard embeds committed text on rebuild (textarea-safe embed); panel refresh uses GET when the server is running. `POST /api/portrait/generate` is synchronous and blocks the request worker until the LLM returns; concurrent requests receive 409.

**Dependencies:**
- `spejder.config`, `spejder.core`, `spejder.db`, `spejder.llm`, `spejder.parsers.cv_parser`
