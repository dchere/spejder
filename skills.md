# spejder.skills

**Purpose:**
Skill extraction, normalization, and pattern matching from job descriptions and CVs.

**API:**
- `extract_skills(text, profile=None)`
- `normalize_skill(name)`
- `sync_cv_skills(cv_text, profile=None)`

**Context:**
- Used by reporting, CLI, and server modules to extract and manage user/job skills.
- Handles both regex-based and LLM-based extraction.

**Dependencies:**
- Standard library, LLM integration (optional)

**Example usage:**
```python
from spejder.skills import extract_skills
skills = extract_skills(job_text)
```
