"""
Dashboard and Reporting views renderer module for creating static HTML reports.
"""

import html as html_lib
import json
import os
from email.utils import formatdate
from typing import Optional

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT

from .dashboard_cards import _build_job_cards, _render_html_from_items
from .dashboard_sorting import _sort_applied_positions, _sort_positions_unviewed_then_score
from .dashboard_templates import jinja_env

__all__ = [
    "_render_html_from_items",
    "_build_job_cards",
    "_render_company_dashboard_html",
    "_render_html_dashboard",
]

# Fixed-width placeholder (29 chars, same as HTTP-date) replaced after write.
_SPEJDER_REPORT_MTIME_PLACEHOLDER = "_____SPEJDER_REPORT_MTIME_P__"

def _render_company_dashboard_html(company_name: str, company_items: list[dict]) -> str:
    company_label = (company_name or "").strip() or "Unknown company"
    safe_company_label = html_lib.escape(company_label)
    hidden_items = [
        item for item in company_items if int(item.get("hidden", 0) or 0) == 1
    ]
    applied_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and int(item.get("applied", 0) or 0) == 1
        and int(item.get("on_interview", 0) or 0) == 0
        and int(item.get("interview_stopped", 0) or 0) == 0
    ]
    interview_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and int(item.get("applied", 0) or 0) == 1
        and int(item.get("on_interview", 0) or 0) == 1
    ]
    stopped_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and int(item.get("applied", 0) or 0) == 1
        and int(item.get("interview_stopped", 0) or 0) == 1
    ]
    relevant_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and str(item.get("category", "")).strip().lower() == "relevant"
        and int(item.get("applied", 0) or 0) != 1
    ]
    not_relevant_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and str(item.get("category", "")).strip().lower() == "not relevant"
        and int(item.get("applied", 0) or 0) != 1
    ]

    relevant_items = _sort_positions_unviewed_then_score(relevant_items)
    not_relevant_items = _sort_positions_unviewed_then_score(not_relevant_items)
    hidden_items = _sort_positions_unviewed_then_score(hidden_items)
    applied_items = _sort_applied_positions(applied_items)
    interview_items = _sort_applied_positions(interview_items)
    stopped_items = _sort_applied_positions(stopped_items)

    relevant_cards = _build_job_cards(relevant_items, company_links=False, skill_buttons=False)
    not_relevant_cards = _build_job_cards(
        not_relevant_items,
        company_links=False,
        skill_buttons=False,
    )
    applied_cards = _build_job_cards(
        applied_items, company_links=False, skill_buttons=False, card_panel="applied"
    )
    interview_cards = _build_job_cards(
        interview_items, company_links=False, skill_buttons=False, card_panel="interview"
    )
    stopped_cards = _build_job_cards(
        stopped_items, company_links=False, skill_buttons=False, card_panel="stopped"
    )
    hidden_cards = _build_job_cards(
        hidden_items, company_links=False, skill_buttons=False, card_panel="hidden"
    )

    template = jinja_env.get_template("company_dashboard.html")
    return template.render(
        company_label=company_label,
        safe_company_label=safe_company_label,
        len_company_items=len(company_items),
        len_relevant_items=len(relevant_items),
        len_not_relevant_items=len(not_relevant_items),
        len_applied_items=len(applied_items),
        len_interview_items=len(interview_items),
        len_stopped_items=len(stopped_items),
        len_hidden_items=len(hidden_items),
        relevant_cards=relevant_cards,
        not_relevant_cards=not_relevant_cards,
        applied_cards=applied_cards,
        interview_cards=interview_cards,
        stopped_cards=stopped_cards,
        hidden_cards=hidden_cards,
        skills_table_html=locals().get("skills_table_html", ""),
        len_skills_items=0
    )


def _render_html_dashboard(
    relevant_items,
    not_relevant_items,
    applied_items,
    out_html: str,
    title: str,
    viewed_total: int = 0,
    skills_items: Optional[list[dict]] = None,
    report_max_relevant_positions: int = 7,
    report_max_not_relevant_positions: int = 7,
    relevant_total_count: Optional[int] = None,
    not_relevant_total_count: Optional[int] = None,
    interview_items: Optional[list[dict]] = None,
    stopped_items: Optional[list[dict]] = None,
    hidden_items: Optional[list[dict]] = None,
    runtime_profile: Optional[AppConfig] = None,
):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)
    relevant_items = _sort_positions_unviewed_then_score(relevant_items)
    not_relevant_items = _sort_positions_unviewed_then_score(not_relevant_items)
    applied_items = _sort_applied_positions(applied_items)
    interview_items = _sort_applied_positions(interview_items or [])
    stopped_items = _sort_applied_positions(stopped_items or [])
    hidden_items = _sort_positions_unviewed_then_score(hidden_items or [])

    if relevant_total_count is None:
        relevant_total_count = len(relevant_items)
    if not_relevant_total_count is None:
        not_relevant_total_count = len(not_relevant_items)
    relevant_items = list(relevant_items)[: max(1, int(report_max_relevant_positions or 7))]
    not_relevant_items = list(not_relevant_items)[: max(1, int(report_max_not_relevant_positions or 7))]
    applied_items = list(applied_items)
    interview_items = list(interview_items)
    stopped_items = list(stopped_items)
    hidden_items = list(hidden_items)

    relevant_cards = _build_job_cards(relevant_items)
    not_relevant_cards = _build_job_cards(not_relevant_items)
    applied_cards = _build_job_cards(applied_items, card_panel="applied")
    interview_cards = _build_job_cards(interview_items, card_panel="interview")
    stopped_cards = _build_job_cards(stopped_items, card_panel="stopped")
    hidden_cards = _build_job_cards(hidden_items, card_panel="hidden")
    skills_items = skills_items or []

    skills_rows = []
    for item in skills_items:
        skill_name = html_lib.escape(str(item.get("name", "")))
        skill_key = html_lib.escape(str(item.get("key", "")), quote=True)
        skill_key_js = html_lib.escape(json.dumps(str(item.get("key", ""))), quote=True)
        source = html_lib.escape(str(item.get("source", "")))
        occurrences = int(item.get("occurrences", 0) or 0)
        position_count = int(item.get("position_count", 0) or 0)
        jobs_with_skills = int(item.get("jobs_with_skills", 0) or 0)
        position_pct = float(item.get("position_pct", 0) or 0)
        has_skill = bool(item.get("has_skill"))
        want_to_learn = bool(item.get("want_to_learn"))
        has_skill_checked = "checked" if has_skill else ""
        learn_checked = "checked" if want_to_learn else ""
        added_at_raw = str(item.get("added_at", "") or "")
        if added_at_raw:
            added_display = html_lib.escape(added_at_raw[:10])
            added_title = html_lib.escape(f"Added to skill_patterns: {added_at_raw}")
            sort_added_at = html_lib.escape(added_at_raw, quote=True)
        else:
            added_display = "—"
            added_title = html_lib.escape("Profile-only skill (not stored in skill_patterns)")
            sort_added_at = SKILLS_EMPTY_ADDED_AT_SORT
        if jobs_with_skills > 0:
            position_display = f"{position_pct:.1f}%"
            position_title = html_lib.escape(
                f"On {position_count} of {jobs_with_skills} jobs with extracted skills ({position_pct:.1f}%)"
            )
        else:
            position_display = "—"
            position_title = html_lib.escape("No jobs with extracted skills yet")
        skills_rows.append(
            f"""
            <tr data-skill-key="{skill_key}"
                data-sort-name="{html_lib.escape(str(item.get('name', '')), quote=True)}"
                data-sort-added-at="{sort_added_at}"
                data-sort-source="{html_lib.escape(str(item.get('source', '')), quote=True)}"
                data-sort-position-pct="{position_pct:.1f}"
                data-sort-occurrences="{occurrences}"
                data-sort-has-skill="{'1' if has_skill else '0'}"
                data-sort-want-learn="{'1' if want_to_learn else '0'}">
                <td><input type="checkbox" class="skill-row-select" aria-label="Select skill" onchange="updateSkillsBulkBar()" /></td>
                <td>{skill_name}</td>
                <td title="{added_title}">{added_display}</td>
                <td>{source}</td>
                <td title="{position_title}">{position_display}</td>
                <td>{occurrences}</td>
                <td><input type="checkbox" {has_skill_checked} onchange="setUserSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" {learn_checked} onchange="setLearnSkill({skill_key_js}, this.checked, this)" /></td>
                <td><button type="button" class="block-skill-btn" onclick="blockSkill({skill_key_js}, this)">Block</button><button type="button" class="delete-skill-btn" onclick="deleteSkill({skill_key_js}, this)">Delete</button></td>
            </tr>
            """.strip()
        )

    skills_table_html = (
        """
        <table class="skills-table" id="skills-table">
            <thead>
                <tr>
                    <th title="Select skills for bulk Block or Delete."><input type="checkbox" id="skills-select-all" aria-label="Select all skills" onchange="toggleSelectAllSkills(this.checked)" /></th>
                    <th class="skills-sortable" data-sort-key="name" title="Skill name (normalized). Click to sort.">Skill<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable skills-sort-active" data-sort-key="added_at" title="When the skill was first stored in skill_patterns (profile-only skills show —). Click to sort.">Added<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="source" title="Where defined: db (SQLite pattern) or profile (your lists / seed patterns). Click to sort.">Source<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="position_pct" title="Share of jobs with extracted skills that list this skill. Cell tooltip shows exact counts. Click to sort.">Job share<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="occurrences" title="Pattern-learning score from applied/relevant positions (not the same as job share). Click to sort.">Learned<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="has_skill" title="Whether the skill is in your profile user_skills list. Click to sort.">I have<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="want_learn" title="Whether the skill is in missing_skills_suggestions (want to learn). Click to sort.">Want to learn<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th title="Block hides the skill; Delete removes it from profile and DB.">Action</th>
                </tr>
            </thead>
            <tbody>
        """
        + "".join(skills_rows)
        + """
            </tbody>
        </table>
        """
        if skills_rows
        else '<p class="empty">No skills found.</p>'
    )

    from spejder.workflows.user_portrait import (
        embed_portrait_for_textarea,
        load_portrait,
        portrait_file_path,
    )

    portrait_text = ""
    if runtime_profile is not None:
        portrait_text = load_portrait(portrait_file_path(runtime_profile))
    textarea_portrait_text = embed_portrait_for_textarea(portrait_text)

    template = jinja_env.get_template("dashboard.html")
    content = template.render(
        title=html_lib.escape(title),
        relevant_total_count=relevant_total_count,
        not_relevant_total_count=not_relevant_total_count,
        viewed_total=viewed_total,
        len_relevant_items=len(relevant_items),
        len_not_relevant_items=len(not_relevant_items),
        len_applied_items=len(applied_items),
        len_interview_items=len(interview_items),
        len_stopped_items=len(stopped_items),
        len_hidden_items=len(hidden_items),
        len_skills_items=len(skills_items) if skills_items else 0,
        relevant_cards=relevant_cards,
        not_relevant_cards=not_relevant_cards,
        applied_cards=applied_cards,
        interview_cards=interview_cards,
        stopped_cards=stopped_cards,
        hidden_cards=hidden_cards,
        skills_table_html=skills_table_html,
        skills_empty_added_at_sort=SKILLS_EMPTY_ADDED_AT_SORT,
        portrait_text=textarea_portrait_text,
        has_portrait=bool(portrait_text.strip()),
        report_mtime=_SPEJDER_REPORT_MTIME_PLACEHOLDER,
    )

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    mtime = os.path.getmtime(out_html)
    mtime_str = formatdate(mtime, usegmt=True)
    with open(out_html, "r+", encoding="utf-8") as f:
        patched = f.read().replace(_SPEJDER_REPORT_MTIME_PLACEHOLDER, mtime_str, 1)
        if _SPEJDER_REPORT_MTIME_PLACEHOLDER in patched:
            raise RuntimeError("report mtime placeholder was not replaced in dashboard HTML")
        f.seek(0)
        f.write(patched)
        f.truncate()
    os.utime(out_html, (mtime, mtime))

    print(
        f"Wrote HTML dashboard: {out_html} "
        f"(relevant={len(relevant_items)}, not_relevant={len(not_relevant_items)}, "
        f"applied={len(applied_items)}, interview={len(interview_items)}, "
        f"stopped={len(stopped_items)}, hidden={len(hidden_items)}, "
        f"viewed={int(viewed_total)})"
    )
