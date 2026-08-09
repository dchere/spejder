# spejder.llm

**Purpose:**
Thin wrapper around `llama_cpp.Llama` so workflows stay decoupled from the native GGUF runtime.

**API:**
- `LocalLLM(model_path, n_ctx=8192, verbose=False)` — lazy-loads the model on first `generate`
- `generate(prompt, max_tokens=256, stop=None) -> str`
- `summarize(text, max_tokens=200) -> str`
- `classify(text, taxonomy_prompt, max_tokens=128) -> str`

**Context:**
- CLI and workflows obtain model paths from profile (`default_model`, `n_ctx`).
- Quiet mode suppresses native llama.cpp stdout/stderr during model init.
- When `llama-cpp-python` is missing, `load()` raises `RuntimeError`.
- Career-alert format synthesis (`jobs/parsing/artifact_synth.py`) is an optional consumer: when `career_alert_synth_enabled` is true and ingest finds zero positions for a file, it calls `LocalLLM.generate` with shrunk HTML. No new public API on this module; GGUF-only via `default_model`.

**Dependencies:**
- Optional `llama_cpp` (llama-cpp-python)
