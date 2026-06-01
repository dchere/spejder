# spejder.language

**Purpose:**
Language detection (Danish/English) and translation utilities. Encapsulates FastText and MarianMT logic.

**API:**
- `is_danish(text, runtime_profile=None)`
- `translate_to_english(text, runtime_profile=None, cache=None)`
- `initialize_language_services(runtime_profile=None)`

**Context:**
- Used by CLI, reporting, and skill extraction modules to normalize and translate job titles/descriptions.
- Handles model loading, caching, and self-tests.

**Dependencies:**
- fasttext, transformers, torch (optional, heavy)

**Example usage:**
```python
from spejder.language import is_danish, translate_to_english
if is_danish(title):
    title_en = translate_to_english(title)
```
