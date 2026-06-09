
"""
cv_parser covers logic for reading CV files
"""
import os


def load_cv_text(cv_path: str, max_chars: int = 40000) -> str:
    """Load text from a single file or a directory of text/markdown files."""
    path = (cv_path or "").strip()
    if not path:
        return ""

    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                return f.read()[:max_chars]
        except Exception:
            return ""

    if os.path.isdir(path):
        chunks = []
        total = 0
        allowed_ext = {".txt", ".md", ".rst", ".html", ".htm", ".eml"}
        for root, _, files in os.walk(path):
            for name in files:
                ext = os.path.splitext(name)[1].lower()
                if ext and ext not in allowed_ext:
                    continue
                fp = os.path.join(root, name)
                try:
                    with open(fp, encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception:
                    continue
                if not content:
                    continue
                header = f"\n\n[CV_FILE {fp}]\n"
                part = header + content
                remaining = max_chars - total
                if remaining <= 0:
                    return "".join(chunks)
                chunks.append(part[:remaining])
                total += min(len(part), remaining)
        return "".join(chunks)

    return ""
