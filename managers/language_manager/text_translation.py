
import logging
import re
from typing import Optional

from spejder.core import AppConfig

try:
    import sentencepiece as spm
    from ctranslate2 import Translator
    TRANSLATION_AVAILABLE = True
except ImportError:
    Translator = None
    spm = None
    TRANSLATION_AVAILABLE = False

try:
    import fasttext
    FASTTEXT_AVAILABLE = True
except ImportError:
    fasttext = None
    FASTTEXT_AVAILABLE = False

logger = logging.getLogger(__name__)

# Global singletons
_language_checker_detector = None
_translation_instance = None



from .detection import is_danish_text
from .engines import get_translation_runtime


def normalize_translation_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def split_translation_chunks(text: str, max_chars: int = 500) -> list[str]:
    normalized = normalize_translation_text(text)
    if not normalized:
        return []

    paragraphs = [part.strip() for part in re.split(r"\n\s*\n+", normalized) if part.strip()]
    chunks: list[str] = []
    for paragraph in paragraphs or [normalized]:
        if len(paragraph) <= max_chars:
            chunks.append(paragraph)
            continue

        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", paragraph) if s.strip()]
        buffer = ""
        for sentence in sentences or [paragraph]:
            if len(sentence) > max_chars:
                words = sentence.split()
                word_buffer = ""
                for word in words:
                    candidate = f"{word_buffer} {word}".strip()
                    if word_buffer and len(candidate) > max_chars:
                        chunks.append(word_buffer)
                        word_buffer = word
                    else:
                        word_buffer = candidate
                if word_buffer:
                    if buffer:
                        chunks.append(buffer)
                        buffer = ""
                    chunks.append(word_buffer)
                continue

            candidate = f"{buffer} {sentence}".strip() if buffer else sentence
            if buffer and len(candidate) > max_chars:
                chunks.append(buffer)
                buffer = sentence
            else:
                buffer = candidate
        if buffer:
            chunks.append(buffer)
    return chunks


def translate_text_chunks_to_english(
    chunks: list[str], runtime_profile: Optional[AppConfig] = None
) -> list[str]:
    runtime = get_translation_runtime(runtime_profile)
    if runtime is None:
        raise RuntimeError("translation model is not available")
    tokenizer, model, device = runtime
    translated_chunks: list[str] = []
    for chunk in chunks:
        if not chunk.strip():
            continue
        try:
            encoded = tokenizer(chunk, return_tensors="pt", truncation=True)
            encoded = {
                key: value.to(device) if hasattr(value, "to") else value
                for key, value in encoded.items()
            }
            generated = model.generate(**encoded, max_new_tokens=512, num_beams=4)
            translated = tokenizer.batch_decode(generated, skip_special_tokens=True)[0]
        except Exception as exc:
            raise RuntimeError(f"text translation execution failed: {exc}") from exc
        translated = normalize_translation_text(translated)
        if not translated:
            raise RuntimeError("text translation returned empty text")
        translated_chunks.append(translated)
    return translated_chunks


TEXT_TRANSLATION_CACHE: dict[str, str] = {}

def translate_text_to_english_if_needed(
    text: str,
    runtime_profile: Optional[AppConfig] = None,
    translation_cache: Optional[dict[str, str]] = None,
) -> str:
    source_text = normalize_translation_text(text)
    if not source_text:
        return ""

    cache_key = source_text
    cache = translation_cache if translation_cache is not None else TEXT_TRANSLATION_CACHE
    if cache_key in cache:
        return cache[cache_key]

    if not is_danish_text(source_text, runtime_profile=runtime_profile):
        cache[cache_key] = source_text
        return source_text

    chunks = split_translation_chunks(source_text)
    translated_chunks = translate_text_chunks_to_english(chunks, runtime_profile=runtime_profile)
    translated_text = "\n\n".join(translated_chunks).strip()
    if not translated_text:
        raise RuntimeError("text translation returned empty output")
    cache[cache_key] = translated_text
    return translated_text


