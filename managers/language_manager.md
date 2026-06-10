# spejder.managers.language_manager

**Purpose:**
Provides orchestrating utilities around basic language detection and translation using FastText and MarianMT.

**API:**
- `is_danish_text`, `is_ukrainian_text`, `translation_source_language`
- `translate_title_to_english`, `translate_text_to_english_if_needed`
- `initialize_language_checker_or_exit`, `initialize_translation_or_exit`

**Context:**
Currently encapsulates the heavy logic for NLP. Could be refactored further into specific detection/translation modules.

**Detection:**
- FastText `lid.176` (`language_checker_model_path`) predicts the top language label.
- `translation_source_language` returns `"da"` or `"uk"` when confidence ≥ `language_checker_threshold`; other languages pass through untranslated.

**Translation models (profile):**
- `danish_translation_model_path` — MarianMT Danish→English (e.g. `opus-mt-da-en`)
- `ukrainian_translation_model_path` — MarianMT Ukrainian→English (e.g. `opus-mt-uk-en`)
- `get_translation_runtime(..., source_lang)` loads and caches the model for the detected source language.

**Runtime Notes:**
- `translate_text_to_english_if_needed` caches by `{source_lang}:{normalized_text}` so Danish and Ukrainian translations do not collide.
- `normalize_title_compare_key` keeps Unicode letters (Cyrillic titles compare correctly against English translations).
- Translation model runtime is cached per absolute model path (`TRANSLATION_MODELS`) to avoid repeated Marian loads during workflows like background GUI sync.
- If Marian loads with meta tensors, the engine retries with `low_cpu_mem_usage=False` before device placement, then falls back to CPU if MPS move fails.
- `initialize_translation_or_exit` self-tests Danish always; Ukrainian self-tests run when `ukrainian_translation_model_path` is configured. Danish translation self-test uses the same multi-word job-title sample as the language checker (single-word compounds like `Softwareudvikler` are often misclassified by FastText).
- At runtime, when FastText detects Danish or Ukrainian but the matching MarianMT path is missing or invalid, translation helpers log a warning and return the original text instead of aborting ingest (init still fails fast for configured models).
