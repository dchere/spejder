# spejder

Local CLI for parsing job emails (`.eml`), storing positions in SQLite, scoring relevance from a profile, extracting skills, and triaging in a browser dashboard.

## Table of contents

- [Features](#features)
- [Project layout](#project-layout)
- [Install](#install)
- [Running the CLI](#running-the-cli)
- [Quick start](#quick-start)
- [Dashboard](#dashboard)
- [CLI commands](#cli-commands)
- [Data and profile](#data-and-profile)
- [Notes](#notes)

## Features

- Parse `.eml` job emails into SQLite with URL-based deduplication (optional [IT-DAY portal](https://www.itday.dk/job-portal) sync).
- Score relevance from profile keywords and skills; optional local GGUF model for summaries/descriptions (`llama-cpp-python`).
- Interactive dashboard: Relevant / Not relevant / Applied / Interview / Stopped / Hidden / Edited today, plus Skills, Portrait, and Profile panels.
- Skills: extract per job, Sync from CV, Block (teach bad cloud) / Delete / Forgive, I have / Want to learn / Not for me; card tags re-filtered on read (whitelist → blocked → bad cloud).
- Learn keywords and missing-skill suggestions from jobs you label or apply to.
- Background inbox sync while `serve-gui` runs (also **Sync inbox** on the dashboard).
- LinkedIn Easy Apply detection with a relevance bonus when present in job text.

## Project layout

Relative paths (`--profile`, `--db`, `--inbox`, `--report-dir`, …) resolve against the **project directory** — the folder you run the CLI from, or `SPEJDER_WORKSPACE` if set:

```text
my-project/
  profile.json
  jobs.db
  inbox/          # drop .eml files here
  outbox/         # report.html and generated output
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
- See [LICENSE](LICENSE) (MIT)

```bash
cd my-project
python3 -m venv .venv
source .venv/bin/activate
pip install -r spejder/requirements.txt    # layout A
# pip install -r requirements.txt          # layout B
```

Local model features need `llama-cpp-python` and a GGUF path via `--model` (or `default_model` in profile).

## Running the CLI

```bash
python3 -m spejder.cli ...    # layout A
# python3 -m cli ...          # layout B
```

```bash
export SPEJDER_WORKSPACE=/path/to/my-project   # optional override
```

Examples below use layout A (`python3 -m spejder.cli`).

## Quick start

```bash
mkdir -p inbox outbox
python3 -m spejder.cli init-profile --path ./profile.json
# drop .eml files into ./inbox
python3 -m spejder.cli process-inbox --profile ./profile.json
python3 -m spejder.cli serve-gui --profile ./profile.json
```

Open `http://127.0.0.1:8765/report.html`.

## Dashboard

Requires `serve-gui`. Deep UI wiring lives in [`workflows/dashboard.md`](workflows/dashboard.md); API shapes in [`server.md`](server.md).

**Tabs:** Relevant and Not relevant show unviewed jobs only. Viewed jobs move to **Edited today** (local calendar day). **Applied** / **Interview** / **Stopped** are the apply pipeline (mutually exclusive Interview/Stopped). **Hidden** parks a card without changing category/scores. Applied and Hidden do not auto-switch tabs.

**Toolbar** (right of the tab bar): Portrait, Regenerate report, Profile (gear), Sync inbox. Sync runs the same pipeline as startup background sync (inbox ingest, optional IT-DAY portal, dedupe, skills, descriptions). Reload manually when sync finishes.

**Skills tab** (top → bottom): action bar with Block/Delete selected and right-aligned **Sync from CV**; skills table; **bad cloud** status (ngrams, threshold, remaining blocked list with **Forgive**). Columns: Skill → Action (Block / Delete) → Added → Source → Job share → Learned → I have / Want to learn / Not for me (default sort: Added newest first). **Block** hides and teaches the bad cloud; **Delete** removes without teaching; **Forgive** unblocks and decrements cloud weights; **Not for me** is a score penalty (`skill_unwanted_penalty`), not Block.

**Other:** company name → company page; paste full description on Applied cards to re-generate summary/skills; saved cover letters expand under **Show letter**; busy port tries the next ports (up to 20).

## CLI commands

### `process-inbox`

Parse inbox, ingest, score, descriptions, skill learning/hygiene, optional IT-DAY portal, write dashboard.

```bash
python3 -m spejder.cli process-inbox --profile ./profile.json --model ./models/model.gguf
```

Options: `--inbox`, `--db`, `--profile`, `--model`, `--report-dir`, `--limit`, `--max-tokens`, `--max-input-chars`, `--prune-irrelevant`, `--verbose`.

### `serve-gui`

Serve `report.html` and the feedback API (rebuilds dashboard on start, then background sync).

```bash
python3 -m spejder.cli serve-gui --profile ./profile.json
```

Options: `--report-dir`, `--db`, `--profile`, `--host`, `--port`, `--no-open`, `--verbose`.

### `sync-user-skills`

Extract skills from a CV into `user_skills` (same path as Skills tab **Sync from CV**).

```bash
python3 -m spejder.cli sync-user-skills --profile ./profile.json --cv ./CV --model ./models/model.gguf
```

Options: `--profile`, `--db`, `--model`, `--cv`, `--limit`, `--max-chars`, `--replace`, `--quiet-model`.

### `cleanup-skills`

Block structurally noisy skills (empty, malformed, pronoun fragments, >4 tokens, repeated single letters). Protects seed/user skills. Adds to `blocked_skills` / bad cloud. Separate from automatic retention (unflagged DB skills older than 90 days with Job share &lt; 0.1% are deleted without teaching).

```bash
python3 -m spejder.cli cleanup-skills --profile ./profile.json --db ./jobs.db
```

Options: `--profile`, `--db`, `--limit`, `--dry-run`.

### `dedupe-jobs`

Company+title dedupe across all sources (also runs during GUI sync after ingest).

```bash
python3 -m spejder.cli dedupe-jobs --profile ./profile.json --db ./jobs.db
```

### `refresh-descriptions`

Regenerate descriptions (and skill tags) without re-ingesting.

```bash
python3 -m spejder.cli refresh-descriptions --profile ./profile.json --model ./models/model.gguf --category relevant --limit 20
```

Options: `--profile`, `--db`, `--model`, `--source`, `--category`, `--link`, `--job-id`, `--limit`, `--overwrite`, `--allow-empty`, `--quiet-model`, `--report-dir`.

### `init-profile`

```bash
python3 -m spejder.cli init-profile --path ./profile.json
```

Options: `--force`.

### Career-alert artifacts

```bash
python3 -m spejder.cli list-career-alert-artifacts --profile ./profile.json
python3 -m spejder.cli disable-career-alert-artifact --id jobs2web_danfoss --profile ./profile.json
python3 -m spejder.cli enable-career-alert-artifact --id jobs2web_danfoss --profile ./profile.json
python3 -m spejder.cli promote-career-alert-artifact --id synth_example_host_abc123 --profile ./profile.json
```

Overlay dir defaults to `./career_alert_artifacts`. Disable is profile-based (`career_alert_artifacts_disabled`). Opt-in synthesis: `career_alert_synth_enabled` in profile. `promote-career-alert-artifact` copies an overlay recipe into the package shipped tree (maintainer/checkout write); drops the overlay file unless `--keep-overlay`.

### Other

- `report-links ./inbox` — frequent links in parsed files
- `summarize-file` / `summarize-folder` — local-model summaries (`--model`, optional `--verbose-model`)
- `render-html` — JSONL → simple HTML page

## Data and profile

| Store | Role | Detail |
|-------|------|--------|
| `jobs.db` | Jobs, `skill_patterns`, `bad_ngram_weights`, job↔skill cache | [`db.md`](db.md) |
| `profile.json` | Keywords, paths, LLM, scoring, skill lists, portals | [`config.md`](config.md) |
| Skill filter / bad cloud | Whitelist → blocked → toxicity; forgive; hygiene stages | [`extractors/extractors.md`](extractors/extractors.md) |

Defaults live in `default_profile.json`; runtime merges `profile.json` over them. Skill lists: `user_skills`, `missing_skills_suggestions`, `unwanted_skills` (**Not for me**), `blocked_skills` (hide + teach). Auto-written: `skill_bigram_toxicity_threshold`, `bad_cloud_seeded`. Jobs older than 90 days are pruned on DB open except interview/stopped applied rows.

## Notes

- Open `report.html` only via `serve-gui`; `file://` feedback falls back to `http://127.0.0.1:8765`.
- Inbox accepts `.eml` only. Processed files are removed after successful ingest (`process-inbox` / Sync). Files that yield no usable positions (empty or all-weak titles such as “Apply here”) stay out of the DB and move to `{report_dir}/parse_quarantine` with a JSON sidecar; see `sync.log` `event=parse_file` / `parse_quarantine`.
- Sync progress: `{report_dir}/sync.log` (also `/sync.log` while serving) and terminal lines starting with `sync `.
- Card skill tags use `get_job_skills_filtered` (may rewrite dropped names out of `job_skills`). Re-extract via paste-description or `refresh-descriptions` if tags look wrong after an upgrade.
- Module memory for agents/maintainers: see companion `*.md` files next to each package (not duplicated here).
