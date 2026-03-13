# spejder

`spejder` is a local CLI tool for parsing job emails (`.html`, `.htm`, `.eml`), storing extracted positions in SQLite, scoring relevance with a profile, and generating a browser dashboard for triage.

## Current functionality

- Parse HTML and EML job emails, extract links, and normalize job posting URLs.
- Ingest jobs into SQLite with de-duplication on normalized `position_link`.
- Classify jobs as `relevant` or `not relevant` using a keyword profile.
- Generate `summary` and `description` text for jobs, optionally with a local GGUF model through `llama-cpp-python`.
- Render `outbox/report.html` with three views: unviewed relevant jobs, unviewed not relevant jobs, and applied jobs.
- Serve the dashboard with feedback endpoints for `Relevant`, `Viewed`, and `Applied` actions.
- Learn additional profile keywords from labeled jobs and write them back to `profile.json`.

## Requirements

- Python 3.10+
- Linux/macOS shell commands below (adapt activation for Windows if needed)

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r spejder/requirements.txt
```

## Running the CLI

There are two supported ways to invoke the CLI:

- From the workspace root: `python -m spejder.cli ...`
- From inside the `spejder` directory: `python -m cli ...`

Examples below use the workspace-root form because `profile.json`, `inbox`, and `outbox` typically live next to the `spejder` folder.

## Quick start

1. Create a profile file.

```bash
python -m spejder.cli init-profile --path ./profile.json
```

2. Process files from `./inbox` into `./jobs.db` and build `./outbox/report.html`.

```bash
python -m spejder.cli process-inbox --profile ./profile.json
```

3. Serve the dashboard and feedback API.

```bash
python -m spejder.cli serve-gui --profile ./profile.json
```

Open `http://127.0.0.1:8765/report.html`.

## Dashboard behavior

- `Relevant` and `Not relevant` tabs show only unviewed jobs.
- Marking a job as `Viewed` removes it from those tabs.
- Marking a job as `Applied` moves it into the `Applied` tab and also marks it as relevant and viewed.
- The server rebuilds `report.html` after feedback changes.
- When `serve-gui` starts, it also performs a background inbox sync, relevance scoring, and missing-description generation.
- If the requested port is busy, the server automatically tries the next ports up to 20 times.

## CLI commands

### `report-links`

Show the most frequent links found in parsed files.

```bash
python -m spejder.cli report-links ./inbox
```

### `summarize-file`

Summarize a single file with a local model.

```bash
python -m spejder.cli summarize-file --path ./inbox/example.eml --model ./models/model.gguf
```

Options: `--max-tokens`.

### `summarize-folder`

Summarize files in a folder, optionally writing JSONL output.

```bash
python -m spejder.cli summarize-folder \
  --folder ./inbox \
  --model ./models/model.gguf \
  --out ./outbox/summaries.jsonl
```

Options: `--max-tokens`, `--limit`, `--out`.

### `process-inbox`

Parse inbox files, ingest to DB, score relevance, generate missing descriptions, and write dashboard output.

```bash
python -m spejder.cli process-inbox \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --max-input-chars 4500
```

Options: `--inbox`, `--db`, `--profile`, `--model`, `--report-dir`, `--limit`, `--max-tokens`, `--max-input-chars`, `--prune-irrelevant`, `--verbose`.

Notes:

- Relevant jobs get a `summary` during ingest.
- Missing descriptions are generated only for jobs that are still unviewed.
- If no model is supplied, fallback text is used for summaries.

### `serve-gui`

Serve `report.html` and the feedback API.

```bash
python -m spejder.cli serve-gui --profile ./profile.json
```

Options: `--report-dir`, `--db`, `--profile`, `--host`, `--port`, `--no-open`, `--verbose`.

API endpoints:

- `POST /api/feedback` with `job_id` and `signal` (`relevant` or `not relevant`)
- `POST /api/viewed` with `job_id` and `viewed` (`true` or `false`)
- `POST /api/applied` with `job_id` and `applied` (`true` or `false`)

### `refresh-descriptions`

Refresh descriptions for selected jobs without re-ingesting inbox files.

```bash
python -m spejder.cli refresh-descriptions \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --category relevant \
  --limit 20 \
  --report-dir ./outbox
```

Options: `--db`, `--source`, `--category`, `--link` (repeatable), `--job-id` (repeatable), `--limit`, `--overwrite`, `--allow-empty`, `--quiet-model`, `--report-dir`.

Notes:

- Without `--overwrite`, only jobs with empty descriptions are selected.
- If `--report-dir` is provided, the dashboard is regenerated after the refresh.
- The job `summary` is prepended to the raw source text before generating the description.

### `init-profile`

Write the default profile JSON file.

```bash
python -m spejder.cli init-profile --path ./profile.json
```

Options: `--force`.

### `render-html`

Render a simple HTML page from a JSONL input.

```bash
python -m spejder.cli render-html \
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
- `description_raw`
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

## Notes

- Local model features require `llama-cpp-python` and a local GGUF model path passed via `--model`.
- `serve-gui` and the in-browser dashboard expect the API server to be running; if you open `report.html` directly as a file, feedback actions will try `http://127.0.0.1:8765`.

