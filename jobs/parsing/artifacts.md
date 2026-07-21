# Career-alert format artifacts

**Purpose:** Declarative JSON recipes + fixed Python interpreter for career-alert email formats. Local-LLM synthesis may write **overlay** artifacts only after re-validation.

## Layout

| Path | Role |
|------|------|
| `artifact_schema.py` | Pydantic models (`CareerAlertArtifact`, match/extract/fields) |
| `artifact_store.py` | Load shipped + overlay; disable list; overlay save |
| `artifact_interpreter.py` | HTML → `dict[normalized_link, fields]` (no LLM, no exec) |
| `html_shrink.py` | Deterministic shrink for synth prompts |
| `artifact_synth.py` | Shrink → GGUF → validate thresholds → overlay persist |
| `artifacts/*.json` | Shipped recipes (Jobs2Web Vestas/Danfoss/Novo Nordisk) |

## Storage

- **Shipped:** `spejder/jobs/parsing/artifacts/*.json` (package tree; never written by synth)
- **Overlay:** profile `career_alert_artifacts_dir` (default `./career_alert_artifacts`), resolved via `resolve_user_path`
- Overlay **overrides** the same `id` as shipped
- Disable via profile `career_alert_artifacts_disabled` (ids); CLI `disable`/`enable` mutates that list
- **Load rule:** `load_artifacts(overlay_dir=None)` / `extract_job_entries` without `artifacts_dir` → **shipped only**. Overlay merges only when `artifacts_dir` / profile dir is passed (ingest, CLI list/disable/enable).

## Schema (v1)

- Identity: `id`, `version`, `priority` (higher first among artifacts), `enabled`
- Match: non-empty `host_substrings` and `path_includes` (blank strings stripped; empty/all-blank lists rejected at load); optional `require_path_regex`
- Extract: `mode` = `filtered_links` (Jobs2Web) or `css` (reserved; interpreter returns empty)
- Fields opcodes (closed set): `from_anchor` ∈ `jobs2web_middot_or_dash` | `anchor_text_compact`; literal `company` / `source`; max lengths
- Provenance: `source` ∈ `shipped` | `llm_synth` | `manual`; optional `created_at`, `model_path` (basename), `parent_eml_hash`

## Merge with built-ins

See `jobs.md`: artifacts fill fields only when the built-in value is empty; artifact-only links (new hosts) become entries after the links loop. `jobs2web.py` remains the Python fallback until migration.

## Synthesis gate

Defaults: `career_alert_synth_link_ratio` / `career_alert_synth_title_ratio` = `0.8`. Persist only when interpreter recovery meets thresholds.

Before persist, drafts are rejected when:
- schema rejects empty/all-blank `host_substrings` or `path_includes` (`schema`); synth also guards with `empty_match_rules`
- recovered link count ≫ proposed (`match_too_broad`)

Interpreter fail-closed: after stripping blanks, if no host **or** no path substrings remain, `href_matches_artifact` returns `False` (`"" in host` / `"" in path` would otherwise match every href).

Synth ids are always rewritten to `synth_<host>_<html_hash6>` (counter suffix if the overlay file exists); LLM-chosen ids are ignored. `require_path_regex` is length-capped and nested-quantifier-rejected at schema + match time (ReDoS).

`ingest_docs_to_db` loads artifacts once per run (with profile overlay dir) and reloads after a successful overlay write. When synth is enabled but no model/LLM is available, it prints `no_model`.
