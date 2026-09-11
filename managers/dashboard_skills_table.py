"""Skills tab HTML for the main dashboard."""

import html as html_lib
import json

from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT


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
                <td title="{added_title}">{added_display}</td>
                <td>{source}</td>
                <td title="{position_title}">{position_display}</td>
                <td>{occurrences}</td>
                <td><input type="checkbox" class="skill-has-checkbox" {has_skill_checked} onchange="setUserSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" class="skill-learn-checkbox" {learn_checked} onchange="setLearnSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" class="skill-unwanted-checkbox" {unwanted_checked} onchange="setUnwantedSkill({skill_key_js}, this.checked, this)" /></td>
                <td><button type="button" class="block-skill-btn" onclick="blockSkill({skill_key_js}, this)">Block</button><button type="button" class="delete-skill-btn" onclick="deleteSkill({skill_key_js}, this)">Delete</button></td>
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
                    <th class="skills-sortable skills-sort-active" data-sort-key="added_at" title="When the skill was first stored in skill_patterns (profile-only skills show —). Click to sort.">Added<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="source" title="Where defined: db (SQLite pattern) or profile (your lists / seed patterns). Click to sort.">Source<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="position_pct" title="Share of jobs with extracted skills that list this skill. Cell tooltip shows exact counts. Click to sort.">Job share<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="occurrences" title="Pattern-learning score from applied/relevant positions (not the same as job share). Click to sort.">Learned<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="has_skill" title="Whether the skill is in your profile user_skills list. Click to sort.">I have<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="want_learn" title="Whether the skill is in missing_skills_suggestions (want to learn). Click to sort.">Want to learn<span class="skills-sort-indicator" aria-hidden="true"></span></th>
                    <th class="skills-sortable" data-sort-key="not_for_me" title="Whether the skill is in unwanted_skills (not for me). Mutually exclusive with I have and Want to learn. Click to sort.">Not for me<span class="skills-sort-indicator" aria-hidden="true"></span></th>
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
    )
