# spejder.managers.language_manager

**Purpose:**
Provides orchestrating utilities around basic language detection and translation using FastText and MarianMT.

**API:**
- `is_danish_text`
- `translate_title_to_english`
- `initialize_language_checker_or_exit`
- `initialize_translation_or_exit`

**Context:**
Currently encapsulates the heavy logic for NLP. Could be refactored further into specific detection/translation modules.

**Runtime Notes:**
- Translation model runtime is cached per absolute model path (`TRANSLATION_MODELS`) to avoid repeated Marian loads during workflows like background GUI sync.
- If Marian loads with meta tensors, the engine retries with `low_cpu_mem_usage=False` before device placement, then falls back to CPU if MPS move fails.
