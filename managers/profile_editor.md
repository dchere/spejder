# spejder.managers.profile_editor

**Purpose:**
Dashboard Profile panel: field metadata for every `AppConfig` field, GET payload shaping, and validated save with live runtime reload.

**Modules:**
- `profile_editor_fields.py` — `PROFILE_FIELD_META`, `GROUP_ORDER` / `GROUP_TITLES`, `READONLY_FIELDS` (`skill_bigram_toxicity_threshold`, `bad_cloud_seeded`)
- `profile_editor.py` — `build_profile_get_response`, `merge_profile_updates`, `save_profile_updates`, `validation_errors_by_field`; import-time `assert_field_meta_complete()` ensures `set(PROFILE_FIELD_META) == set(AppConfig.model_fields)`

**Save semantics:**
- Client POSTs **only dirty keys** (fields that differ from the last loaded/saved snapshot; readonly fields omitted). Server merges those keys into the live `runtime_profile` so Sync/Skills updates to other fields are not overwritten by a stale full-form snapshot. Empty skill-pattern rows (Add pattern with no name/pattern) are not dirty; pattern compare uses `{name, pattern}` only. Save stays disabled while the payload is empty.
- Reject unknown keys (400)
- Ignore client values for readonly fields; retain current runtime values
- Validate via `AppConfig.model_validate`
- Ordering: merge → write `profile_path` → `reload_runtime_profile()` so Sync/scoring see live values. If write succeeds and reload fails, disk is ahead of memory until the next successful reload. `OSError` on write/reload surfaces as HTTP 500 `{ ok: false, error: "failed to write profile" }`.
- Editable vs readonly: all other AppConfig fields are editable in the UI and accepted on save
- Score-affecting edits (`min_score`, keywords, skill weights, etc.) apply to **future** scoring immediately after reload; existing DB scores/`relevant` flags stay until Sync / Regenerate report / CLI rescore (not auto-triggered on Profile save)

**Known MVP limits:**
- No profile-file lock. Partial HTTP merge avoids full-form overwrite, but concurrent writers (Profile save, Skills APIs, Sync persist/reload) still last-write-win on the whole `profile.json`. Same-field races remain.
- `AppConfig.load` swallows read/parse errors (`OSError`, `JSONDecodeError`, …) and continues with defaults. Profile save reloads after write; a corrupt/unreadable file can silently reset runtime to defaults. Follow-up: surface load failures (do not treat as in-scope for the editor MVP).

**Manual UI checks (before commit; no Playwright):**
1. Open Profile, change `min_score`, Save — status Saved; reload Profile and confirm the value.
2. Change a field, then also toggle a skill on Skills (or run Sync) before Save — the unposted sibling should still be present after Profile save.
3. Click **Add pattern** without filling name/pattern — Save stays disabled, no Unsaved edits.
4. Fill a pattern then clear it — dirty clears; fill `{name, pattern}` — Save enables.
5. Invalid number → 400 with field error and Unsaved edits restored; Save still enabled.

**Context:**
Used by `GET /api/profile` and `POST /api/profile/save` in `server.py`. Does not own a second skills store; skills lists remain on `AppConfig`.
