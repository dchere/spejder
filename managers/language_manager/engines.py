
import logging
from typing import Optional

from spejder.core import AppConfig
from spejder.managers.language_manager.utils import (
    _language_checker_model_path,
    _language_checker_cache_key,
    _translation_model_looks_valid,
    _translation_model_path_for_language,
    language_checker_engine,
)


try:
    from ctranslate2 import Translator
    import sentencepiece as spm
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

try:
    import torch
    from transformers import MarianTokenizer, MarianMTModel
except ImportError:
    torch = None
    MarianTokenizer = None
    MarianMTModel = None

logger = logging.getLogger(__name__)

LANGUAGE_CHECKER_DETECTORS = {}
TRANSLATION_MODELS = {}


def _model_has_meta_tensors(model: object) -> bool:
    try:
        parameters = getattr(model, "parameters", None)
        if parameters is None:
            return False
        return any(getattr(param, "is_meta", False) for param in parameters())
    except Exception:
        return False


def _load_translation_model(model_path: str):
    model = MarianMTModel.from_pretrained(model_path)
    if _model_has_meta_tensors(model):
        model = MarianMTModel.from_pretrained(model_path, low_cpu_mem_usage=False)
    return model


def _get_language_checker_detector(runtime_profile: Optional[AppConfig]) -> Optional[object]:
    engine = language_checker_engine(runtime_profile)
    model_path = _language_checker_model_path(runtime_profile)
    if engine != "fasttext" or fasttext is None or not model_path:
        return None

    cache_key = _language_checker_cache_key(runtime_profile)
    detector = LANGUAGE_CHECKER_DETECTORS.get(cache_key)
    if detector is None:
        detector = fasttext.load_model(model_path)
        LANGUAGE_CHECKER_DETECTORS[cache_key] = detector
    return detector




def get_translation_runtime(
    runtime_profile: Optional[AppConfig],
    source_lang: str = "da",
) -> Optional[tuple[object, object, str]]:
    model_path = _translation_model_path_for_language(runtime_profile, source_lang)
    if (
        not model_path
        or not _translation_model_looks_valid(model_path)
        or MarianTokenizer is None
        or MarianMTModel is None
        or torch is None
    ):
        return None

    runtime = TRANSLATION_MODELS.get(model_path, None)
    if runtime is not None:
        return runtime

    tokenizer = MarianTokenizer.from_pretrained(model_path)
    model = _load_translation_model(model_path)
    device = "cpu"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        try:
            model = model.to("mps")
            device = "mps"
        except Exception:
            # Retry with eager CPU materialization when the model was initialized on meta tensors.
            if _model_has_meta_tensors(model):
                model = MarianMTModel.from_pretrained(model_path, low_cpu_mem_usage=False)
            model = model.to("cpu")
    else:
        model = model.to("cpu")
    model.eval()
    runtime = (tokenizer, model, device)
    TRANSLATION_MODELS[model_path] = runtime
    return runtime


