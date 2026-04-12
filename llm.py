"""Local LLM wrapper using llama-cpp-python (llama.cpp bindings)

This module provides a thin wrapper around `llama_cpp.Llama` if available.
It keeps the rest of the codebase decoupled so the agent can run offline
with a quantized GGUF model.
"""

import os
from contextlib import contextmanager
from typing import Optional

try:
    from llama_cpp import Llama
except Exception:  # pragma: no cover - graceful fallback when package missing
    Llama = None


class LocalLLM:
    def __init__(
        self,
        model_path: Optional[str] = None,
        n_ctx: int = 8192,
        verbose: bool = False,
    ):
        self.model_path = model_path
        self.n_ctx = n_ctx
        self.verbose = verbose
        self.model = None

    def load(self):
        if Llama is None:
            raise RuntimeError("llama_cpp (llama-cpp-python) is not installed")
        if not self.model_path:
            raise RuntimeError("model_path is not set")
        if self.verbose:
            self.model = Llama(model_path=self.model_path, n_ctx=self.n_ctx, verbose=True)
            return

        # Some native llama.cpp startup messages bypass Python logging flags.
        # Silence both stdout/stderr while initializing the model in quiet mode.
        with _suppress_native_stdio():
            self.model = Llama(model_path=self.model_path, n_ctx=self.n_ctx, verbose=False)

    def generate(
        self,
        prompt: str,
        max_tokens: int = 256,
        stop: Optional[list] = None,
    ) -> str:
        if self.model is None:
            self.load()
        if hasattr(self.model, "create_completion"):
            resp = self.model.create_completion(prompt=prompt, max_tokens=max_tokens, stop=stop)
        elif callable(self.model):
            resp = self.model(prompt=prompt, max_tokens=max_tokens, stop=stop)
        else:
            raise RuntimeError("Unsupported llama_cpp API: no completion method found")
        return resp.get("choices", [{}])[0].get("text", "").strip()

    def summarize(self, text: str, max_tokens: int = 200) -> str:
        prompt = (
            "Summarize the following email content in 3 sentences, focusing on action items, dates, "
            "and important links:\n\n" + text + "\n\nSummary:"
        )
        return self.generate(prompt, max_tokens=max_tokens)

    def classify(self, text: str, taxonomy_prompt: str, max_tokens: int = 128) -> str:
        prompt = taxonomy_prompt + "\n\nContent:\n" + text + "\n\nAnswer:"
        return self.generate(prompt, max_tokens=max_tokens)


@contextmanager
def _suppress_native_stdio():
    saved_stdout = None
    saved_stderr = None
    devnull = None
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        saved_stdout = os.dup(1)
        saved_stderr = os.dup(2)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        yield
    finally:
        if saved_stdout is not None:
            os.dup2(saved_stdout, 1)
            os.close(saved_stdout)
        if saved_stderr is not None:
            os.dup2(saved_stderr, 2)
            os.close(saved_stderr)
        if devnull is not None:
            os.close(devnull)
