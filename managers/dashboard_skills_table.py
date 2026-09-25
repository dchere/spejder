"""Skills tab HTML for the main dashboard."""

import html as html_lib
import json
from typing import Optional

from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT

_SKILL_ICON_SVG_ATTRS = (
    'xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" '
    'fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round" aria-hidden="true"'
)
_BLOCK_SKILL_ICON_SVG = (
    f"<svg {_SKILL_ICON_SVG_ATTRS}>"
    '<circle cx="12" cy="12" r="10"/>'
    '<path d="m4.9 4.9 14.2 14.2"/>'
    "</svg>"
)
_DELETE_SKILL_ICON_SVG = (
    f"<svg {_SKILL_ICON_SVG_ATTRS}>"
    '<path d="M3 6h18"/>'
    '<path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/>'
    '<path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/>'
    "</svg>"
)


def _format_threshold_label(threshold: Optional[float]) -> str:
    if threshold is None:
        return "not set (uses sync calibration / cold-start)"
    if threshold == float("inf"):
        return "∞ (empty cloud)"
    return f"{threshold:.3f}"


def _render_skills_cloud_status_html(cloud_status: Optional[dict]) -> str:
    """Render bad-cloud size / threshold / forgive controls for the Skills tab."""
    status = cloud_status or {}
    ngram_count = int(status.get("ngram_count", 0) or 0)
    total_weight = int(status.get("total_weight", 0) or 0)
    max_weight = int(status.get("max_weight", 0) or 0)
    weight_cap = status.get("weight_cap")
    if weight_cap is None:
        cap_label = "uncapped"
    else:
        cap_label = str(int(weight_cap))
    threshold_label = html_lib.escape(_format_threshold_label(status.get("threshold")))
    blocked = [
        str(name)
        for name in (status.get("blocked_skills") or [])
        if str(name).strip()
    ]
    blocked_count = len(blocked)

    summary = (
        f'<div class="skills-cloud-status" id="skills-cloud-status">'
        f'<p class="skills-cloud-summary" title="Bad-cloud ngrams reject similar noisy extractions. '
        f'Weights are capped per ngram; Forgive decrements weights for a blocked name.">'
        f"Bad cloud: <strong>{ngram_count}</strong> ngrams"
        f" · total weight <strong>{total_weight}</strong>"
        f" · heaviest <strong>{max_weight}</strong>"
        f" · cap <strong>{html_lib.escape(cap_label)}</strong>"
        f" · threshold <strong>{threshold_label}</strong>"
        f" · blocked list <strong>{blocked_count}</strong>"
        f"</p>"
    )
    if not blocked:
        return summary + "</div>"

    rows: list[str] = []
    for name in blocked:
        skill_key_js = html_lib.escape(json.dumps(name), quote=True)
        rows.append(
            "<li class=\"skills-blocked-item\" "
            f'data-skill-key="{html_lib.escape(name, quote=True)}">'
            f"<span>{html_lib.escape(name)}</span>"
            f'<button type="button" class="forgive-skill-btn" '
            f'onclick="forgiveSkill({skill_key_js}, this)" '
            f'title="Forgive: remove from blocked list and decrement cloud weights">'
            f"Forgive</button></li>"
        )
    return (
        summary
        + '<div class="skills-blocked-list-wrap">'
        + f'<p class="skills-blocked-heading">Still on blocked list ({blocked_count})'
        + " — Forgive softens cloud weights for shared tokens:</p>"
        + '<ul class="skills-blocked-list" id="skills-blocked-list">'
        + "".join(rows)
        + "</ul></div></div>"
    )


def _render_skills_table_html(skills_items: list[dict]) -> str:
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
        not_for_me = bool(item.get("not_for_me"))
        has_skill_checked = "checked" if has_skill else ""
        learn_checked = "checked" if want_to_learn else ""
        unwanted_checked = "checked" if not_for_me else ""
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
                data-sort-want-learn="{'1' if want_to_learn else '0'}"
                data-sort-not-for-me="{'1' if not_for_me else '0'}">
                <td><input type="checkbox" class="skill-row-select" aria-label="Select skill" onchange="updateSkillsBulkBar()" /></td>
                <td>{skill_name}</td>
                <td class="skills-action"><button type="button" class="block-skill-btn" onclick="blockSkill({skill_key_js}, this)" title="Block" aria-label="Block">{_BLOCK_SKILL_ICON_SVG}</button><button type="button" class="delete-skill-btn" onclick="deleteSkill({skill_key_js}, this)" title="Delete" aria-label="Delete">{_DELETE_SKILL_ICON_SVG}</button></td>
                <td title="{added_title}">{added_display}</td>
                <td>{source}</td>
                <td title="{position_title}">{position_display}</td>
                <td>{occurrences}</td>
                <td><input type="checkbox" class="skill-has-checkbox" {has_skill_checked} onchange="setUserSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" class="skill-learn-checkbox" {learn_checked} onchange="setLearnSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" class="skill-unwanted-checkbox" {unwanted_checked} onchange="setUnwantedSkill({skill_key_js}, this.checked, this)" /></td>
            </tr>
            """.strip()
        )

    if not skills_rows:
        return '<p class="empty">No skills found.</p>'

    return (
        """
        <table class="skills-table" id="skills-table">
            <thead>
                <tr>
                    <th title="Select skills for bulk Block or Delete."><input type="checkbox" id="skills-select-all" aria-label="Select all skills" onchange="toggleSelectAllSkills(this.checked)" /></th>
                    <th class="skills-sortable" data-sort-key="name" title="Skill name (normalized). Click to sort.">Skill<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-action" title="Block hides the skill; Delete removes it from profile and DB.">Action</th>
                    <th class="skills-sortable skills-sort-active" data-sort-key="added_at" title="When the skill was first stored in skill_patterns (profile-only skills show —). Click to sort.">Added<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="source" title="Where defined: db (SQLite pattern) or profile (your lists / seed patterns). Click to sort.">Source<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="position_pct" title="Share of jobs with extracted skills that list this skill. Cell tooltip shows exact counts. Click to sort.">Job share<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="occurrences" title="Pattern-learning score from applied/relevant positions (not the same as job share). Click to sort.">Learned<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="has_skill" title="Whether the skill is in your profile user_skills list. Click to sort.">I have<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="want_learn" title="Whether the skill is in missing_skills_suggestions (want to learn). Click to sort.">Want to learn<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="not_for_me" title="Whether the skill is in unwanted_skills (not for me). Mutually exclusive with I have and Want to learn. Click to sort.">Not for me<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                </tr>
            </thead>
            <tbody>
        """
        + "".join(skills_rows)
        + """
            </tbody>
        </table>
        """
    )
