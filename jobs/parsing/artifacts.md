# Career-alert format artifacts

**Purpose:** Declarative JSON recipes + fixed Python interpreter for career-alert email formats. Local-LLM synthesis may write **overlay** artifacts only after re-validation.

## Layout

| Path | Role |
|------|------|
| `artifact_schema.py` | Pydantic models (`CareerAlertArtifact`, match/extract/fields) |
| `artifact_store.py` | Load shipped + overlay; disable list; overlay save |
| `artifact_interpreter.py` | HTML → `dict[normalized_link, fields]` (no LLM, no exec) |
| `artifact_heuristic.py` | Deterministic CTA-digest drafts (iCIMS Apply-here + strong title; single-card OK; learns CTA labels from overlays; company from Subject/From/`companies.py`) |
| `html_shrink.py` | Deterministic shrink for synth prompts |
| `artifact_synth_json.py` | Truncated-JSON recovery (`_balanced_json_slice` … `_recover_truncated_synth_payload` / `_extract_json_object`) |
| `artifact_synth_validate.py` | Jaccard/title agreement, `validate_synth_thresholds`, broad-match gates |
| `artifact_synth.py` | Prompt, persist, `try_synthesize_artifact`; re-exports `_extract_json_object` and `validate_synth_thresholds` |
| `artifact_synth_budget.py` | Per-host UTC-day LLM synth attempt cap (`.synth_budget.json` under overlay dir) |
| `artifact_synth_drift.py` | Format-drift detection; disable stale overlays after a successful second-pass synth |
| `artifacts/*.json` | Shipped recipes (Jobs2Web Vestas/Danfoss/Novo Nordisk) |

## Storage

- **Shipped:** `spejder/jobs/parsing/artifacts/*.json` (package tree; never written by synth)
- **Overlay:** profile `career_alert_artifacts_dir` (default `./career_alert_artifacts`), resolved via `resolve_user_path`. Dotfiles (e.g. `.synth_budget.json`) are not loaded as recipes.
- Overlay **overrides** the same `id` as shipped
- Disable via profile `career_alert_artifacts_disabled` (ids); CLI `disable`/`enable` mutates that list
- **Load rule:** `load_artifacts(overlay_dir=None)` / `extract_job_entries` without `artifacts_dir` → **shipped only**. Overlay merges only when `artifacts_dir` / profile dir is passed (ingest, CLI list/disable/enable).

## Schema (v1)

- Identity: `id`, `version`, `priority` (higher first among artifacts), `enabled`
- Match: non-empty `host_substrings` and `path_includes` (blank strings stripped; empty/all-blank lists rejected at load); optional `require_path_regex`; optional `anchor_text_equals` (CTA digests — only anchors whose visible text matches, e.g. `["Apply here"]`)
- Extract: `mode` = `filtered_links` only (`css` removed from the schema until a real implementation exists)
- Fields opcodes (closed set): `from_anchor` ∈ `jobs2web_middot_or_dash` | `anchor_text_compact` | `ancestor_strong_or_first_line` | `prev_sibling_text`; literal `company` / `source`; max lengths
  - `ancestor_strong_or_first_line`: for CTA-only buttons (iCIMS etc.), walk up to a substantial ancestor and take non-CTA `<strong>`/`<b>`/heading text (joining adjacent fragments) as the title
  - `prev_sibling_text`: when the title sits in a previous sibling cell/paragraph beside the CTA (not in an ancestor heading), take that sibling’s non-CTA text
- Provenance: `source` ∈ `shipped` | `llm_synth` | `manual` | `heuristic`; optional `created_at`, `model_path` (basename), `parent_eml_hash`

## Merge with built-ins

See `jobs.md`: artifacts fill fields only when the built-in value is empty; artifact-only links (new hosts) become entries after the links loop. Vestas / Danfoss / Novo Nordisk are artifact-only in `extract_job_entries` (Python site extractors in `jobs2web.py` remain for parity tests). Promote a stable overlay into `jobs/parsing/artifacts/` with `promote_overlay_artifact` / CLI `promote-career-alert-artifact`.

## Synthesis gate

Defaults: `career_alert_synth_link_ratio` / `career_alert_synth_title_ratio` = `0.8`; `career_alert_synth_max_per_host_day` = `3` (LLM attempts only; `0` = unlimited). Persist only when interpreter recovery meets thresholds.

Before persist, drafts are rejected when:
- schema rejects empty/all-blank `host_substrings` or `path_includes` (`schema`); synth also guards with `empty_match_rules`
- recovered link count ≫ proposed (`match_too_broad`)

Interpreter fail-closed: after stripping blanks, if no host **or** no path substrings remain, `href_matches_artifact` returns `False` (`"" in host` / `"" in path` would otherwise match every href).

Synth ids are always rewritten to `synth_<host>_<html_hash6>` (counter suffix if the overlay file exists); LLM-chosen ids are ignored. `require_path_regex` is length-capped and nested-quantifier-rejected at schema + match time (ReDoS).

Prompt hygiene: `html_shrink` strips query/fragment from hrefs, skips empty image/track anchors, prefers CTA rows with ancestor heading context (`:: Title`, max 8), and caps anchor text so Jobs2Web / iCIMS tracking URLs do not blow the GGUF token budget. Synthesis tries a deterministic CTA heuristic first (`artifact_heuristic.draft_cta_ancestor_artifact`) before calling the GGUF — single-card digests are eligible; CTA recognition unions the built-in label set with `anchor_text_equals` from shipped/overlay artifacts; company comes from Subject / `companies.py` / From (not a hard shortlist). `unwrap_track_link` (Mandrill) runs before host/path match in the heuristic and interpreter so tracker wrappers resolve to the real ATS host. The LLM prompt asks for at most 5 positions and uses `_SYNTH_MAX_TOKENS` (1600). If the model still truncates mid-JSON, `_extract_json_object` recovers a complete `artifact` object plus any finished `positions` entries. When positions are missing but the draft artifact recovers real non-CTA titles, those recovered titles are used to prove the rules. Title validation accepts either the parsed title or the full Jobs2Web anchor `raw_text` (LLMs often echo the whole anchor). CTA digests should use `ancestor_strong_or_first_line` (or `prev_sibling_text`) + `anchor_text_equals`.

**Format drift (second pass):** when enabled overlays/shipped recipes prefilter-match the digest but recover no usable titles, the LLM prompt gets a drift note; a successful new overlay disables overlapping stale **overlay** recipes (`enabled=false`; shipped never disabled). LLM attempts are counted in `{overlay}/.synth_budget.json` per host/UTC-day (`host_budget` when capped).

`ingest_docs_to_db` loads artifacts once per run (with profile overlay dir) and reloads after a successful overlay write. Synth runs when the file has **no strong** entries (hard zero **or** all rows fail `extract_quality`); only strong rows are upserted. When synth is enabled but no model/LLM is available, it prints `no_model`.
