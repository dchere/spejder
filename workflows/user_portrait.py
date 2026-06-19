"""User portrait: load/save, context assembly, LLM generation, diff rendering."""

from __future__ import annotations

import difflib
import html as html_lib
import os
import re
import tempfile
from typing import Optional

from spejder.config import AppConfig
from spejder.core import resolve_user_path
from spejder.db import get_all_applied_jobs, get_job_skills_for_jobs
from spejder.llm import LocalLLM
from spejder.parsers.cv_parser import load_cv_text

_JOB_TEXT_LIMIT = 800
_CV_SHARE_OF_BUDGET = 0.35
_PORTRAIT_MAX_APPLIED_JOBS = 50


def portrait_file_path(profile: AppConfig) -> str:
    return resolve_user_path(str(profile.default_portrait_path or "./portrait.txt"))


def cv_file_path(profile: AppConfig) -> str:
    return resolve_user_path(str(profile.default_cv_path or "./CV"))


def load_portrait(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ""
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except OSError:
        return ""


def save_portrait(path: str, text: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=directory or None, prefix=".portrait-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def embed_portrait_for_textarea(text: str) -> str:
    """Neutralize textarea breakout only; preserve plain text characters like & and <."""
    return re.sub(r"</textarea", "&lt;/textarea", text or "", flags=re.IGNORECASE)


def _stripped_skill_list(items: list[str] | None) -> list[str]:
    return [str(skill).strip() for skill in (items or []) if str(skill).strip()]


def _truncate(text: str, limit: int, label: str = "") -> str:
    compact = (text or "").strip()
    if limit <= 0 or len(compact) <= limit:
        return compact
    suffix = f" [{label} truncated]" if label else " [truncated]"
    return compact[: max(0, limit - len(suffix))] + suffix


def _job_stage(row: dict) -> str:
    if int(row.get("interview_stopped", 0) or 0):
        return "stopped"
    if int(row.get("on_interview", 0) or 0):
        return "interview"
    return "applied"


def _format_job_block(row: dict, per_job_limit: int, skills: list[str]) -> str:
    parts = [
        f"- {row.get('company', '') or 'Unknown'} — {row.get('title', '') or 'Untitled'} "
        f"(stage: {_job_stage(row)})",
    ]
    if skills:
        parts.append(f"  skills: {', '.join(skills[:30])}")
    summary = _truncate(str(row.get("summary", "") or ""), per_job_limit, "summary")
    if summary:
        parts.append(f"  summary: {summary}")
    description = _truncate(str(row.get("description", "") or ""), per_job_limit, "description")
    if description:
        parts.append(f"  description: {description}")
    cover = _truncate(str(row.get("cover_letter", "") or ""), per_job_limit, "cover letter")
    if cover:
        parts.append(f"  cover letter: {cover}")
    feedback = _truncate(str(row.get("company_feedback", "") or ""), per_job_limit, "feedback")
    if feedback:
        parts.append(f"  company feedback: {feedback}")
    return "\n".join(parts)


def collect_portrait_context(db_path: str, profile: AppConfig, cv_path: Optional[str] = None) -> str:
    budget = max(1000, int(profile.max_input_chars or 24000))
    cv_budget = int(budget * _CV_SHARE_OF_BUDGET)
    cv_text = load_cv_text(cv_path or cv_file_path(profile), max_chars=cv_budget)
    cv_text = _truncate(cv_text, cv_budget, "CV")

    user_skills = _stripped_skill_list(profile.user_skills)
    learn_skills = _stripped_skill_list(profile.missing_skills_suggestions)
    applied_jobs = get_all_applied_jobs(db_path, limit=_PORTRAIT_MAX_APPLIED_JOBS)

    remaining = budget - len(cv_text)
    per_job_limit = _JOB_TEXT_LIMIT
    if applied_jobs:
        per_job_limit = min(_JOB_TEXT_LIMIT, max(200, remaining // max(1, len(applied_jobs))))

    sections: list[str] = []
    if cv_text.strip():
        sections.append(f"CV TEXT:\n{cv_text.strip()}")
    if user_skills:
        sections.append("SKILLS I HAVE:\n" + "\n".join(f"- {s}" for s in user_skills))
    if learn_skills:
        sections.append("SKILLS TO LEARN:\n" + "\n".join(f"- {s}" for s in learn_skills))
    if applied_jobs:
        job_ids = [int(row.get("id", 0) or 0) for row in applied_jobs]
        skills_by_job = get_job_skills_for_jobs(db_path, job_ids)
        job_blocks = [
            _format_job_block(
                row,
                per_job_limit,
                skills_by_job.get(int(row.get("id", 0) or 0), []),
            )
            for row in applied_jobs
        ]
        sections.append("APPLIED JOBS:\n" + "\n\n".join(job_blocks))

    context = "\n\n".join(sections)
    return _truncate(context, budget, "context")


def portrait_has_context(db_path: str, profile: AppConfig, cv_path: Optional[str] = None) -> bool:
    if _stripped_skill_list(profile.user_skills) or _stripped_skill_list(profile.missing_skills_suggestions):
        return True
    if get_all_applied_jobs(db_path, limit=1):
        return True
    cv_text = load_cv_text(cv_path or cv_file_path(profile), max_chars=500)
    return bool(cv_text.strip())


def build_portrait_prompt(current_portrait: str, context: str) -> str:
    committed = (current_portrait or "").strip() or "(none)"
    return (
        "You are writing a professional portrait for cover letter generation.\n\n"
        "CURRENT PORTRAIT (committed — preserve accurate parts, minimal edits only):\n"
        f"{committed}\n\n"
        "NEW DATA:\n"
        f"{context}\n\n"
        "Write a structured portrait with sections such as: Professional summary, Core strengths, "
        "Application themes, Learning goals, Interview feedback insights, Voice/tone notes for cover letters.\n\n"
        "Rules:\n"
        "- Output ONLY the updated portrait text (no preamble).\n"
        "- Change the minimum needed to reflect new data; do not rewrite for style alone.\n"
        "- Keep section headings if present; use plain text or markdown headings.\n"
        "- Do not invent facts not supported by the data.\n\n"
        "Updated portrait:"
    )


def generate_portrait_draft(
    llm: LocalLLM,
    db_path: str,
    profile: AppConfig,
    current_portrait: str = "",
    cv_path: Optional[str] = None,
) -> str:
    context = collect_portrait_context(db_path, profile, cv_path=cv_path)
    if not context.strip():
        raise ValueError("no portrait context available")
    prompt = build_portrait_prompt(current_portrait, context)
    max_tokens = max(256, int(profile.portrait_max_tokens or 1200))
    draft = llm.generate(prompt, max_tokens=max_tokens)
    return (draft or "").strip()


def render_portrait_diff_html(old: str, new: str) -> str:
    old_lines = (old or "").splitlines()
    new_lines = (new or "").splitlines()
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    parts = ['<pre class="portrait-diff">']
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for line in old_lines[i1:i2]:
                parts.append(f'<div class="diff-line-ctx"> {html_lib.escape(line)}</div>')
        elif tag == "delete":
            for line in old_lines[i1:i2]:
                parts.append(f'<div class="diff-line-del">-{html_lib.escape(line)}</div>')
        elif tag == "insert":
            for line in new_lines[j1:j2]:
                parts.append(f'<div class="diff-line-add">+{html_lib.escape(line)}</div>')
        elif tag == "replace":
            for line in old_lines[i1:i2]:
                parts.append(f'<div class="diff-line-del">-{html_lib.escape(line)}</div>')
            for line in new_lines[j1:j2]:
                parts.append(f'<div class="diff-line-add">+{html_lib.escape(line)}</div>')
    parts.append("</pre>")
    return "".join(parts)
