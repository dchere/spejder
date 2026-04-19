# spejder

`spejder` is a local CLI tool for parsing job emails (`.html`, `.htm`, `.eml`), storing extracted positions in SQLite, scoring relevance with a profile, extracting skills, and generating a browser dashboard for triage.

## Current functionality

- Parse HTML and EML job emails, extract links, and normalize job posting URLs.
- Ingest jobs into SQLite with de-duplication on normalized `position_link`.
- Classify jobs as `relevant` or `not relevant` using profile keywords and skill-aware scoring.
- Generate `summary` and `description` text for jobs, optionally with a local GGUF model through `llama-cpp-python`.
- Extract up to 10 skills per position and display them as tags in the dashboard.
- Persist known skill patterns in DB (`skill_patterns` table) and update them from applied/relevant positions.
- Learn missing skills from applied jobs and write suggestions to `profile.json`.
- Clean obviously invalid extracted skills from the SQLite skill catalog and block them in `profile.json`.
- Render `outbox/report.html` with three views: unviewed relevant jobs, unviewed not relevant jobs, and applied jobs.
- Detect LinkedIn `Easy Apply` from existing text, highlight those cards in the dashboard, and apply a relevance bonus.
- Serve the dashboard with feedback endpoints for `Relevant`, `Viewed`, and `Applied` actions.
- Learn additional profile keywords from labeled jobs and write them back to `profile.json`.
- Translate non-English job titles to English using the local LLM for consistent display in the dashboard.
- Allow pasting a full position description for applied jobs via the dashboard to trigger re-summarization and re-scoring with the LLM.
- Block or delete skills from the dashboard; blocked skills are persisted in `profile.json` under `blocked_skills`.
- Provide a per-company filtered view at `/company.html?name=<company>` linked from dashboard cards.
- Delete processed inbox files automatically after successful ingestion.

## Requirements

- Python 3.10+
- Linux/macOS shell commands below (adapt activation for Windows if needed)

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r spejder/requirements.txt
```

## Running the CLI

There are two supported ways to invoke the CLI:

- From the workspace root: `python3 -m spejder.cli ...`
- From inside the `spejder` directory: `python3 -m cli ...`

Examples below use the workspace-root form because `profile.json`, `inbox`, and `outbox` typically live next to the `spejder` folder.

## Quick start

1. Create a profile file.

```bash
python3 -m spejder.cli init-profile --path ./profile.json
```

2. Process files from `./inbox` into `./jobs.db` and build `./outbox/report.html`.

```bash
python3 -m spejder.cli process-inbox --profile ./profile.json
```

3. Serve the dashboard and feedback API.

```bash
python3 -m spejder.cli serve-gui --profile ./profile.json
```

Open `http://127.0.0.1:8765/report.html`.

## Dashboard behavior

- `Relevant` and `Not relevant` tabs show only unviewed jobs.
- Marking a job as `Viewed` removes it from those tabs.
- Marking a job as `Applied` moves it into the `Applied` tab and also marks it as relevant and viewed.
- Feedback writes are applied to DB immediately; `report.html` regeneration is queued and runs in background.
- When `serve-gui` starts, it also performs a background inbox sync, relevance scoring, and missing-description generation.
- If the requested port is busy, the server automatically tries the next ports up to 20 times.
- Clicking a company name opens a `/company.html` view filtered to that company's jobs.
- Applied jobs have a "Paste full description" form that feeds the full text to the LLM, regenerating the summary, description, and skill tags.

## CLI commands

### `report-links`

Show the most frequent links found in parsed files.

```bash
python3 -m spejder.cli report-links ./inbox
```

### `summarize-file`

Summarize a single file with a local model.

```bash
python3 -m spejder.cli summarize-file --path ./inbox/example.eml --model ./models/model.gguf
```

Add `--verbose-model` only if you want llama.cpp initialization/debug logs printed to the terminal.

Options: `--max-tokens`.

### `summarize-folder`

Summarize files in a folder, optionally writing JSONL output.

```bash
python3 -m spejder.cli summarize-folder \
  --folder ./inbox \
  --model ./models/model.gguf \
  --out ./outbox/summaries.jsonl
```

Add `--verbose-model` only if you want llama.cpp initialization/debug logs printed to the terminal.

Options: `--max-tokens`, `--limit`, `--out`.

### `process-inbox`

Parse inbox files, ingest to DB, score relevance, generate missing descriptions, learn skill patterns, update profile learning signals, and write dashboard output.

```bash
python3 -m spejder.cli process-inbox \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --max-input-chars 24000
```

Options: `--inbox`, `--db`, `--profile`, `--model`, `--report-dir`, `--limit`, `--max-tokens`, `--max-input-chars`, `--prune-irrelevant`, `--verbose`.

Notes:

- Relevant jobs get a `summary` during ingest.
- Missing descriptions are generated only for jobs that are still unviewed.
- Position skills are extracted and shown in report cards.
- Skill patterns are loaded from DB and may be auto-extended from applied/relevant jobs.
- `profile.json` gets updated with learned include/exclude keywords and `missing_skills_suggestions`.

### `serve-gui`

Serve `report.html` and the feedback API.

```bash
python3 -m spejder.cli serve-gui --profile ./profile.json
```

Options: `--report-dir`, `--db`, `--profile`, `--host`, `--port`, `--no-open`, `--verbose`.

API endpoints:

- `POST /api/feedback` with `job_id` and `signal` (`relevant` or `not relevant`)
- `POST /api/viewed` with `job_id` and `viewed` (`true` or `false`)
- `POST /api/applied` with `job_id` and `applied` (`true` or `false`)

### `refresh-descriptions`

Refresh descriptions for selected jobs without re-ingesting inbox files.

```bash
python3 -m spejder.cli refresh-descriptions \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --category relevant \
  --limit 20 \
  --report-dir ./outbox
```

Options: `--profile`, `--db`, `--model`, `--source`, `--category`, `--link` (repeatable), `--job-id` (repeatable), `--limit`, `--overwrite`, `--allow-empty`, `--quiet-model`, `--report-dir`.

Notes:

- Without `--overwrite`, only jobs with empty descriptions are selected.
- If `--report-dir` is provided, the dashboard is regenerated after the refresh.
- The job `summary` is prepended to the raw source text before generating the description.

### `sync-user-skills`

Extract user skills from a CV file/folder and write them into `profile.json` as `user_skills`.

```bash
python3 -m spejder.cli sync-user-skills \
  --profile ./profile.json \
  --cv ./CV \
  --model ./models/model.gguf
```

Options: `--profile`, `--db`, `--model`, `--cv`, `--limit`, `--max-chars`, `--replace`, `--quiet-model`.

Notes:

- If `--replace` is omitted, extracted skills are merged into existing `user_skills`.
- Works with either a single CV text file or a folder of CV-related text files.

### `cleanup-skills`

Block and delete skill entries that look like sentence fragments, role titles, or generic noise rather than real skills.

```bash
python3 -m spejder.cli cleanup-skills \
  --profile ./profile.json \
  --db ./jobs.db
```

Options: `--profile`, `--db`, `--limit`, `--dry-run`.

Notes:

- The command protects profile seed skills and explicit user skills.
- Removed skills are added to `blocked_skills` so they stay hidden and are not reintroduced into the dashboard.

### `dedupe-jobs`

Run cross-source job deduplication explicitly instead of doing it during startup.

```bash
python3 -m spejder.cli dedupe-jobs \
  --profile ./profile.json \
  --db ./jobs.db
```

Options: `--profile`, `--db`.

Notes:

- This merges matching LinkedIn and Jobindex entries into a single record.
- `serve-gui` and other startup paths no longer run this full-table dedupe automatically.

### `init-profile`

Write the default profile JSON file.

```bash
python3 -m spejder.cli init-profile --path ./profile.json
```

Options: `--force`.

### `render-html`

Render a simple HTML page from a JSONL input.

```bash
python3 -m spejder.cli render-html \
  --input ./outbox/relevant_positions.jsonl \
  --out ./outbox/relevant_positions.html
```

Options: `--title`.

## Data stored in `jobs.db`

Main fields in the `jobs` table include:

- `source`
- `company`
- `title`
- `place`
- `work_type`
- `position_link` (unique)
- `raw_text`
- `description`
- `relevance_score`
- `relevant`
- `category`
- `relevance_reason`
- `summary`
- `viewed`
- `applied`
- `created_at`
- `updated_at`

Additional table:

- `skill_patterns`: known skill names + regex patterns, source, popularity stats (`occurrences`, `weight`), and enable flag.

## Profile fields related to skills

Default profile values are stored in `spejder/default_profile.json`. Runtime loads this file, then merges `profile.json` over it, and applies schema-style normalization in code (type coercion and fallback defaults).

In `profile.json`:

- `user_skills`: your editable skill list used for scoring.
- `blocked_skills`: skills hidden from the Skills tab and filtered out from extracted skill results.
- `missing_skills_suggestions`: generated from applied jobs.
- `skill_match_weight`: bonus per matched required skill.
- `skill_missing_penalty`: penalty per missing required skill.
- `easy_apply_bonus`: extra score added for LinkedIn jobs when `Easy Apply` is detected in existing text.
- `missing_skills_max_items`: max missing-skill suggestions written to profile.
- `report_max_relevant_positions`: max number of positions shown in `Relevant`, default `7`.
- `report_max_not_relevant_positions`: max number of positions shown in `Not relevant`, default `42`.
- `skill_learning_max_positions`, `skill_learning_min_occurrences`, `skill_learning_max_new_patterns`: controls for learning new DB skill patterns.
- `max_input_chars`: maximum characters of job text passed to the LLM as input. Default `24000`. Raise this when pasting full position descriptions to get better summaries.
- `n_ctx`: LLM context window size passed to `llama-cpp-python` at load time. Default `8192`. Should be at least as large as `max_input_chars / 4 + max_tokens` to avoid the "not optimal" warning from llama.cpp.

## Notes

- Local model features require `llama-cpp-python` and a local GGUF model path passed via `--model`.
- `serve-gui` and the in-browser dashboard expect the API server to be running; if you open `report.html` directly as a file, feedback actions will try `http://127.0.0.1:8765`.

