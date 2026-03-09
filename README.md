# spejder

`spejder` is a local CLI tool for parsing job emails (`.html`, `.htm`, `.eml`), storing extracted positions in SQLite, scoring relevance with a profile, and generating a browser-based dashboard.

## Current functionality

- Parse email files and extract text + links.
- Ingest jobs into `jobs.db` with de-duplication by normalized position link.
- Classify jobs as `relevant` or `not relevant` using keyword scoring.
- Generate/update short job descriptions (optionally with a local GGUF model via `llama-cpp-python`).
- Render `outbox/report.html` and run a local server with interactive feedback (`Relevant`, `Viewed`, `Applied`).
- Learn additional profile keywords from labeled jobs and write them to `profile.json`.

## Requirements

- Python 3.10+
- Linux/macOS shell commands below (adapt activation command for Windows if needed)

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quick start

1. Create a profile file:

```bash
python -m cli init-profile --path ./profile.json
```

2. Process files from `./inbox` into `./jobs.db` and build a report in `./outbox`:

```bash
python -m cli process-inbox --profile ./profile.json
```

3. Serve the dashboard and feedback API:

```bash
python -m cli serve-gui --profile ./profile.json
```

Open: `http://127.0.0.1:8765/report.html`

## CLI commands

### `report-links`

Show the most frequent links found in parsed files.

```bash
python -m cli report-links /path/to/emails
```

### `summarize-file`

Summarize a single file with a local model.

```bash
python -m cli summarize-file --path /path/to/email.html --model ./models/model.gguf
```

### `summarize-folder`

Summarize files in a folder (optional JSONL output).

```bash
python -m cli summarize-folder --folder /path/to/emails --model ./models/model.gguf --out ./summaries.jsonl
```

### `process-inbox`

Parse inbox files, ingest to DB, score relevance, and write dashboard output.

```bash
python -m cli process-inbox \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --max-input-chars 4500
```

Useful options: `--inbox`, `--db`, `--report-dir`, `--limit`, `--prune-irrelevant`, `--verbose`.

### `serve-gui`

Serve `report.html` and API endpoints for feedback updates.

```bash
python -m cli serve-gui --profile ./profile.json
```

API endpoints:

- `POST /api/feedback` (`relevant` / `not relevant`)
- `POST /api/viewed` (`true` / `false`)
- `POST /api/applied` (`true` / `false`)

### `refresh-descriptions`

Refresh description summaries for selected jobs without re-ingesting inbox files.

```bash
python -m cli refresh-descriptions \
  --profile ./profile.json \
  --model ./models/model.gguf \
  --category relevant \
  --limit 20 \
  --report-dir ./outbox
```

Useful filters: `--source`, `--link` (repeatable), `--job-id` (repeatable), `--overwrite`, `--allow-empty`.

### `render-html`

Render a simple HTML page from a JSONL input.

```bash
python -m cli render-html --input ./outbox/relevant_positions.jsonl --out ./outbox/relevant_positions.html
```

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
- If no model is provided for description refresh, descriptions can remain empty (unless your pipeline provides fallback content).

