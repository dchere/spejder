# spejder.workflows.company_catalog

**Purpose:** Orchestrate company catalog merge / rename / delete, plus the shared late-sync empty-company prune stage. Persistence and `resolve_company` live in `spejder.db.companies`; this module does not run SQL.

**API:**
- `run_empty_company_prune(db_path, *, on_stage=None) -> list[int]` — shared late sync stage: optional `on_stage("empty_companies", "Pruning empty companies")`, then `prune_empty_companies(db_path)` (full catalog, `company_ids=None`). Used by GUI sync and CLI `process-inbox` after profile keyword learning. Not part of skill hygiene. Do not call on early skipped empty sync. Merge keeps its own scoped prune of loser ids (unchanged).
- `merge_companies(db_path, company_ids, display_name)` — one DB txn via `merge_company_rows` (`UPDATE company_aliases SET company_id=survivor WHERE company_id=loser` for each loser so **all** participant alias keys move to the survivor — including rename-history keys jobs no longer carry — then survivor display + display-alias insert + rewrite `jobs.company` to display_name + delete loser rows); then **always** `merge_duplicate_positions` on its own connections; then `prune_empty_companies` for the deleted loser ids (no-op if already gone). No checkbox / opt-out. Raises `ValueError` (API 400) when `display_name`’s alias key is owned by a company **not** in `company_ids` (“already used by … — merge that company too”); does not auto-include third parties.
- `rename_company(db_path, company_id, display_name)` — update display_name, rewrite member jobs, ensure the new display is an alias (aliases still resolve). Raises `ValueError` (API 400) when the new display’s alias key belongs to a different company (“already used by … — merge that company instead”); rewrite uses only this company’s alias keys after insert.
- `delete_company(db_path, company_id)` — CASCADE-style delete of company + aliases and membership jobs; idempotent `{ already_gone: true }` when missing.
- `delete_companies(db_path, company_ids)` — one-txn batch via `delete_company_rows`; cascade deletes company + aliases + membership jobs per id; returns `{ company_ids, deleted_ids, already_gone_ids, jobs_deleted, count }` (`count` = `len(deleted_ids)`); missing ids are soft (`already_gone_ids`), not errors.

**Locked out:** hide, per-card edit, ML auto-merge, `jobs.company_id` FK, auto-create on upsert, CLI, `company_feedback` changes, rescoring as part of merge.

**Concurrency:** sync FastAPI handlers; catalog+job rewrite in one writer txn; dedupe uses separate connections afterward.

**Ambiguity picks (locked):**
- SSR Companies tab (not live JSON); blank companies never catalogued; company dashboard has no Companies tab.
- Applied bonus keys = union of all alias keys for applied∧¬stopped companies (minus stopped companies' alias unions).
- Merge: identity+job rewrite txn, then `merge_duplicate_positions` on separate connections, then prune empty losers.
