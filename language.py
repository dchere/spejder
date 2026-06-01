"""Language models."""
# pylint: disable=too-many-locals,missing-class-docstring,missing-function-docstring,unused-import,no-name-in-module,trailing-whitespace,unused-variable,broad-exception-caught,line-too-long,unused-argument,fixme,pointless-string-statement,wrong-import-position
"""
language.py: Language detection and translation utilities for spejder
"""

import re
try:
    import fasttext
except ImportError:
    fasttext = None
try:
    import torch
    from transformers import MarianMTModel, MarianTokenizer
except ImportError:
    torch = None
    MarianMTModel = None
    MarianTokenizer = None

LANGUAGE_CHECKER_DETECTORS = {}
TRANSLATION_MODELS = {}

def _normalize_title_text(text: str) -> str:
    return " ".join((text or "").split()).strip()

def _normalize_translation_text(text: str) -> str:
    return (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()

def _split_translation_chunks(text: str, max_chars: int = 500) -> list[str]:
    normalized = _normalize_translation_text(text)
    if not normalized:
        return []
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", normalized) if part.strip()]
    chunks = []
    for paragraph in paragraphs or [normalized]:
        if len(paragraph) <= max_chars:
            chunks.append(paragraph)
            continue
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", paragraph) if s.strip()]
        chunk = ""
        for sentence in sentences:
            if len(chunk) + len(sentence) + 1 > max_chars:
                if chunk:
                    chunks.append(chunk.strip())
                chunk = sentence
            else:
                chunk += (" " if chunk else "") + sentence
        if chunk:
            chunks.append(chunk.strip())
    return chunks

def _get_language_checker_detector(runtime_profile):
    engine = runtime_profile.language_checker_engine if runtime_profile else "fasttext"
    model_path = runtime_profile.language_checker_model_path if runtime_profile else None
    if engine != "fasttext" or fasttext is None or not model_path:
        return None
    cache_key = f"{engine}:{model_path}"
    detector = LANGUAGE_CHECKER_DETECTORS.get(cache_key)
    if detector is None:
        detector = fasttext.load_model(model_path)
        LANGUAGE_CHECKER_DETECTORS[cache_key] = detector
    return detector

def is_danish(text, runtime_profile=None):
    sample = " ".join((text or "").split()).strip()
    if not sample:
        return False
    min_letters = runtime_profile.language_checker_min_letters if runtime_profile else 10
    if sum(1 for c in sample if c.isalpha()) < min_letters:
        return False
    detector = _get_language_checker_detector(runtime_profile)
    if detector is None:
        return False
    try:
        labels, probabilities = detector.predict(sample.replace("\n", " "), k=1)
    except Exception:
        return False
    if not labels or not probabilities:
        return False
    top_label = str(labels[0] or "").strip().lower()
    top_probability = float(probabilities[0] or 0.0)
    threshold = runtime_profile.language_checker_threshold if runtime_profile else 0.8
    return top_label == "__label__da" and top_probability >= threshold

def _get_translation_runtime(runtime_profile):
    model_path = runtime_profile.translation_model_path if runtime_profile else None
    if (
        not model_path
        or MarianTokenizer is None
        or MarianMTModel is None
        or torch is None
    ):
        return None
    runtime = TRANSLATION_MODELS.get(model_path)
    if runtime is not None:
        return runtime
    tokenizer = MarianTokenizer.from_pretrained(model_path)
    model = MarianMTModel.from_pretrained(model_path)
    device = "cpu"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        device = "mps"
    model.to(device)
    TRANSLATION_MODELS[model_path] = (tokenizer, model, device)
    return tokenizer, model, device

def translate_to_english(text, runtime_profile=None, cache=None):
    text_clean = _normalize_title_text(text)
    if not text_clean:
        return ""
    cache_key = text_clean.lower()
    if cache is not None and cache_key in cache:
        return str(cache.get(cache_key, text_clean) or text_clean)
    result = text_clean
    if is_danish(text_clean, runtime_profile=runtime_profile):
        runtime = _get_translation_runtime(runtime_profile)
        if runtime is None:
            raise RuntimeError("title translation model is not available")
        tokenizer, model, device = runtime
        # Actual translation logic would go here
        # For now, just return the input for placeholder
        result = text_clean  # TODO: implement translation
    if cache is not None:
        cache[cache_key] = result
    return result

def initialize_language_services(runtime_profile=None):
    # Optionally preload models or run self-tests
    pass
