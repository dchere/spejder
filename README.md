# spejder

`spejder` is a local CLI tool for parsing job emails (`.eml`), storing extracted positions in SQLite, scoring relevance with a profile, extracting skills, and generating a browser dashboard for triage.

## Table of contents

- [Features](#features)
- [Project layout](#project-layout)
- [Install](#install)
- [Running the CLI](#running-the-cli)
- [Quick start](#quick-start)
- [Dashboard behavior](#dashboard-behavior)
- [CLI commands](#cli-commands)
- [Data stored in `jobs.db`](#data-stored-in-jobsdb)
- [Profile fields related to skills](#profile-fields-related-to-skills)
- [Notes](#notes)

## Features

- Parse job emails (`.eml`) and ingest positions into SQLite with URL-based deduplication.
- Score jobs as relevant or not relevant from your profile (keywords and extracted skills).
- Generate summaries and descriptions with an optional local GGUF model (`llama-cpp-python`).
- Extract skills per job and show them as tags on dashboard cards.
- Interactive dashboard for triage: relevant / not relevant / applied / interview / stopped panels, plus a Skills tab.
- Learn profile keywords and missing-skill suggestions from jobs you label or apply to.
- Sync skills from a CV; block or delete noisy auto-extracted skills.
- Per-company job view from dashboard cards.
- Background inbox sync while `serve-gui` is running (ingest, dedupe, scoring, descriptions), including an on-demand **Sync inbox** button on the report dashboard.
- LinkedIn Easy Apply detection with a relevance bonus when detected in job text.

## Project layout

Commands resolve relative paths (`--profile`, `--db`, `--inbox`, `--report-dir`, and similar) against a **project directory** — the folder you run the CLI from (or `SPEJDER_WORKSPACE` if set). That directory should contain your runtime data:

```text
my-project/
  profile.json
  jobs.db
  inbox/          # drop .eml job emails here
  outbox/         # report.html and other generated output
  spejder/        # this repository (see layouts below)
```

**Layout A — project folder with a `spejder` subdirectory** (common when the Git repo lives inside a larger workspace):

- Run `python3 -m spejder.cli …` from `my-project/`.
- Install with `pip install -r spejder/requirements.txt`.

**Layout B — clone used as the project folder** (repository root is `my-project/`):

- Create `inbox/` and `outbox/` next to `profile.json` in the repo root.
- Run `python3 -m cli …` from the repo root.
- Install with `pip install -r requirements.txt`.

## Install

- Python 3.10+
- Linux/macOS examples below (adapt activation for Windows if needed)
- See [LICENSE](LICENSE) (MIT)

```bash
cd my-project
python3 -m venv .venv
source .venv/bin/activate
pip install -r spejder/requirements.txt    # layout A
# pip install -r requirements.txt          # layout B
```

Local model features also need `llama-cpp-python` and a GGUF model path passed via `--model` on relevant commands.

## Running the CLI

From your **project directory** (layout A or B above):

```bash
python3 -m spejder.cli ...    # layout A
# python3 -m cli ...          # layout B
```

Relative paths resolve against the project directory (current working directory by default). To use a different directory:

```bash
export SPEJDER_WORKSPACE=/path/to/my-project
```

Examples below use `python3 -m spejder.cli` (layout A). Substitute `python3 -m cli` when using layout B.

## Quick start

1. Create runtime folders (if they do not exist yet).

```bash
mkdir -p inbox outbox
```

2. Create a profile file.

```bash
python3 -m spejder.cli init-profile --path ./profile.json
```

3. Drop job emails into `./inbox`, then process them into `./jobs.db` and build `./outbox/report.html`.

```bash
python3 -m spejder.cli process-inbox --profile ./profile.json
```

4. Serve the dashboard and feedback API.

```bash
python3 -m spejder.cli serve-gui --profile ./profile.json
```

Open `http://127.0.0.1:8765/report.html`.

## Dashboard behavior

- `Relevant` and `Not relevant` tabs show only unviewed jobs.
- Marking a job as `Viewed` removes it from those tabs.
- Marking a job as `Applied` moves it into the `Applied` tab and also marks it as relevant and viewed.
- Applied jobs can be moved to `Interview` (on interview) or `Stopped` (process ended); the two flags are mutually exclusive. Cards live in one applied-stage panel at a time.
- Stopped cards support free-text `Company feedback` saved via the dashboard.
- Unmarking `Applied`, unmarking `Viewed`, or marking `Not relevant` clears interview/stopped state and company feedback.
- Feedback writes are saved immediately; `report.html` regeneration is queued and runs in the background.
- **Regenerate report** queues a dashboard rebuild from the current DB and reloads the page when the new `report.html` is ready (requires `serve-gui`).
- **Sync inbox** (requires `serve-gui`) processes new inbox files on demand: ingest, dedupe, skills, descriptions, and related background steps. The button stays disabled while sync runs and until dashboard rebuild is idle; the status line shows stage progress. Reload the page manually when sync completes to see new positions — the page does not auto-reload.
- When `serve-gui` starts, it rebuilds the dashboard snapshot from the current DB (blocking), starts the HTTP server, then triggers the same inbox sync pipeline as **Sync inbox** (ingest, dedupe, skills, descriptions, and related background steps). Reload the page manually when sync completes to see new positions.
- If the requested port is busy, the server automatically tries the next ports up to 20 times.
- Clicking a company name opens a filtered company page for that employer's jobs.
- Applied jobs have a "Paste full description" form that feeds the full text to the LLM, regenerating the summary, description, and skill tags.
- **Skills** tab columns (sortable; default order is skill name A→Z):
  - **Job share** — share of jobs with extracted skills that list this skill. Hover a cell for exact counts.
  - **Learned** — pattern-learning score from applied/relevant jobs (not the same as job share; see [Profile fields](#profile-fields-related-to-skills)).
  - **I have** / **Want to learn** — toggles for your profile skill list and want-to-learn suggestions.

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

Main dashboard API endpoints (JSON `POST` unless noted):

- Triage: `/api/feedback`, `/api/viewed`, `/api/applied`
- Interview: `/api/interview`, `/api/interview/stopped`, `/api/interview/feedback`
- Applied enrichment: `/api/applied/raw-text`, `/api/applied/cover-letter/request`, `/api/applied/cover-letter`
- Skills tab: `/api/skill/user`, `/api/skill/learn`, `/api/skill/block`, `/api/skill/delete`
- Pages: `GET /report.html`, `GET /company.html?company=…`

The dashboard uses these endpoints while you triage; you normally do not call them manually. Request body shapes are documented in [`server.md`](server.md).

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

### `sync-antipatterns`

Distill `blocked_skills` into LLM antipattern rules using per-candidate synthetic test jobs, validate each candidate independently, and prune blocked entries the prompt now filters.

```bash
python3 -m spejder.cli sync-antipatterns \
  --profile ./profile.json \
  --db ./jobs.db \
  --model /path/to/model.gguf \
  --dry-run
```

Options: `--profile`, `--db`, `--model`, `--dry-run`, `--force` (skip gate thresholds).

Each candidate rule triggers its own LLM match pass against the **full** blocked list (chunked at 150 phrases per call), synthetic job generation, and multi-run extraction validation (~3× LLM work vs the old single shared test job).

Runs automatically at the end of GUI background sync when blocked skills grow enough (rare maintenance).

### `dedupe-jobs`

Run company+title position deduplication on demand (e.g. after manual DB edits).

```bash
python3 -m spejder.cli dedupe-jobs \
  --profile ./profile.json \
  --db ./jobs.db
```

Options: `--profile`, `--db`.

Notes:

- Merges rows with the same normalized company and title across **all sources**; keeps the oldest row (`created_at`, then lowest `id`). Title keys strip gender markers like `(m/f/d)` and expand common abbreviations (`SW`→`Software`, `Sr.`→`Senior`).
- Dissimilar duplicate `raw_text` snippets are appended under `[DEDUPE_SNIPPET]`; similar text (&gt;= 85%) is not duplicated.
- `serve-gui` background sync also runs this pass after ingest and before relevance scoring; use this command for a standalone full-table pass.

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
- `on_interview`
- `interview_stopped`
- `company_feedback`
- `created_at`
- `updated_at`

Additional table:

- `skill_patterns`: known skill names + regex patterns, source, popularity stats (`occurrences`, `weight`), and enable flag.

## Profile fields related to skills

Default profile values are stored in `spejder/default_profile.json`. Runtime loads this file, then merges `profile.json` over it, and applies schema-style normalization in code (type coercion and fallback defaults).

In `profile.json`:

- `user_skills`: your editable skill list used for scoring.
- `blocked_skills`: skills hidden from the Skills tab and filtered out from extracted skill results; blocking also deletes matching rows from SQLite `skill_patterns` and `job_skills`.
- `skill_extraction_antipatterns`: LLM-synthesized rules injected into the job skill extraction prompt.
- `skill_antipattern_synthesis_count`: antipattern rules to synthesize per sync (default `3`).
- `skill_antipattern_validation_runs`: stable extraction runs per validation step (default `3`).
- `skill_antipattern_prompt_max_items`: max antipatterns included in the extraction prompt (default `40`).
- `skill_antipattern_good_skills_count`: top DB skills by job link count used in per-candidate synthetic validation jobs (default `20`, minimum `1`).
- `missing_skills_suggestions`: generated from applied jobs.
- `skill_new_confidence_threshold`: minimum LLM confidence for accepting a novel skill candidate (default `0.9`).
- `skill_match_weight`: bonus per matched required skill.
- `skill_missing_penalty`: penalty per missing required skill.
- `easy_apply_bonus`: extra score added for LinkedIn jobs when `Easy Apply` is detected in existing text.
- `missing_skills_max_items`: max missing-skill suggestions written to profile.
- `report_max_relevant_positions`: max number of positions shown in `Relevant`, default `7`.
- `report_max_not_relevant_positions`: max number of positions shown in `Not relevant`, default `42`.
- `skill_learning_max_positions`, `skill_learning_min_occurrences`, `skill_learning_max_new_patterns`: controls for learning new DB skill patterns (Skills tab **Learned** column shows `skill_patterns.occurrences`).
- `max_input_chars`: maximum characters of job text passed to the LLM as input. Default `24000`. Raise this when pasting full position descriptions to get better summaries.
- `n_ctx`: LLM context window size passed to `llama-cpp-python` at load time. Default `8192`. Should be at least as large as `max_input_chars / 4 + max_tokens` to avoid the "not optimal" warning from llama.cpp.

## Notes

- `serve-gui` and the in-browser dashboard expect the API server to be running; if you open `report.html` directly as a file, feedback actions will try `http://127.0.0.1:8765`.
- Skill tags on a job card come from cached extraction. If tags look incomplete after an upgrade, paste a full description on an applied card or run `refresh-descriptions` with a model to re-extract skills for matching jobs.
- Re-extracting skills (manual description paste, `refresh-descriptions`, or clearing cached skills) can change `relevance_score` when more or fewer skills match your profile.
- Processed inbox files are removed automatically after successful ingestion when using background sync or `process-inbox`.
- Inbox ingestion accepts `.eml` files only. Save emails as `.eml` (e.g. drag from Mail.app, or **File → Save As** in Thunderbird) rather than "Save as HTML".

