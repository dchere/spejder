# spejder.reporting

**Purpose:**
HTML rendering, dashboard generation, and job relevance scoring.

**API:**
- `render_html(jobs, profile=None)`
- `calculate_relevance(job, profile)`

**Context:**
- Used by CLI, server, and commands to generate user-facing reports and dashboards.
- Encapsulates all HTML/CSS template logic and relevance scoring.

**Dependencies:**
- Standard library, Jinja2 (optional for templating)

**Example usage:**
```python
from spejder.reporting import render_html
html = render_html(jobs)
```
