"""
Dashboard and Reporting views renderer module for creating static HTML reports.
"""
import os
import html as html_lib
import json
from typing import Optional
from jinja2 import Environment, FileSystemLoader


from spejder.extractors.skill_extractor import _normalize_skill_name
from spejder.core import MANUAL_APPLIED_RAW_MARKER
import spejder.workflows

# pylint: disable=duplicate-code,no-member,too-many-branches,too-many-statements,too-many-locals,protected-access,cyclic-import
# pylint: disable=too-many-arguments,too-many-positional-arguments,line-too-long

jinja_env = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")))

def _render_html_from_items(items, out_html: str, title: str):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)

    cards = []
    for item in items:
        source = html_lib.escape(str(item.get("source", "Unknown")))
        company = html_lib.escape(str(item.get("company", "")))
        role = html_lib.escape(str(item.get("title", "")))
        title_english_html = spejder.workflows._render_title_english_line(item)
        place = html_lib.escape(str(item.get("place", "")))
        work_type = html_lib.escape(str(item.get("work_type", "Unknown")))
        description = html_lib.escape(str(item.get("description", "")))
        skills_text = str(item.get("skills", ""))
        skills_items = [html_lib.escape(s.strip()) for s in skills_text.split(",") if s.strip()]
        skills_html = "".join([f'<span class="skill-tag">{skill}</span>' for skill in skills_items])
        link = str(item.get("position_link", ""))
        safe_link = html_lib.escape(link, quote=True)
        is_easy_apply = spejder.workflows._is_easy_apply_item(item)
        easy_apply_badge = (
            '<span class="easy-apply-badge" title="LinkedIn Easy Apply detected">Easy Apply</span>'
            if is_easy_apply
            else ""
        )
        card_class = "card easy-apply-card" if is_easy_apply else "card"

        cards.append(
            f"""
            <article class="{card_class}">
                            <p><strong>Title:</strong> <a href="{safe_link}" target="_blank" rel="noopener noreferrer">{role}</a> {easy_apply_badge}</p>
                            {title_english_html}
                            <p><strong>Source:</strong> {source}</p>
                            <p><strong>Company:</strong> {company}</p>
                            <p><strong>Place:</strong> {place}</p>
                            <p><strong>Type:</strong> {work_type}</p>
                            <p><strong>Description:</strong> {description}</p>
                            <p><strong>Skills:</strong> <span class="skill-tags">{skills_html or '<span class="skills-empty">No skills extracted</span>'}</span></p>
            </article>
            """.strip()
        )

    template = jinja_env.get_template("report.html")
    content = template.render(title=html_lib.escape(title), items_len=len(items), cards_html="".join(cards) if cards else "<p>No records found.</p>")

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Wrote HTML report: {out_html} (items={len(items)})")


def _build_job_cards(
    items,
    company_links: bool = True,
    skill_buttons: bool = True,
):
    cards = []
    for item in items:
        job_id = int(item.get("id", 0) or 0)
        source = html_lib.escape(str(item.get("source", "Unknown")))
        raw_company = str(item.get("company", ""))
        company = html_lib.escape(raw_company)
        if raw_company.strip() and company_links:
            company_html = (
                f'<button type="button" class="pseudo-link company-link" '
                f'data-company-name="{html_lib.escape(raw_company, quote=True)}" '
                'onclick="openCompanyPage(this.dataset.companyName)">'
                f"{company}</button>"
            )
        else:
            company_html = company
        role = html_lib.escape(str(item.get("title", "")))
        title_english_html = spejder.workflows._render_title_english_line(item)
        place = html_lib.escape(str(item.get("place", "")))
        work_type = html_lib.escape(str(item.get("work_type", "Unknown")))
        description = html_lib.escape(str(item.get("description", "")))
        skills_text = str(item.get("skills", ""))
        skill_tags = []
        for raw_skill in [s.strip() for s in skills_text.split(",") if s.strip()]:
            skill_label = html_lib.escape(raw_skill)
            if skill_buttons:
                skill_key = html_lib.escape(_normalize_skill_name(raw_skill), quote=True)
                if not skill_key:
                    continue
                skill_tags.append(
                    f'<button type="button" class="skill-tag skill-tag-btn" data-skill-key="{skill_key}" onclick="openSkillsForSkill(this.dataset.skillKey)">{skill_label}</button>'
                )
            else:
                skill_tags.append(f'<span class="skill-tag">{skill_label}</span>')
        skills_html = "".join(skill_tags)
        relevance_score = float(item.get("relevance_score", 0) or 0)
        link = str(item.get("position_link", ""))
        safe_link = html_lib.escape(link, quote=True)
        is_easy_apply = spejder.workflows._is_easy_apply_item(item)
        easy_apply_badge = (
            '<span class="easy-apply-badge" title="LinkedIn Easy Apply detected">Easy Apply</span>'
            if is_easy_apply
            else ""
        )
        card_class = "card easy-apply-card" if is_easy_apply else "card"
        is_applied = int(item.get("applied", 0) or 0) == 1
        has_manual_applied_text = MANUAL_APPLIED_RAW_MARKER in str(item.get("raw_text", ""))
        manual_status = (
            '<span class="manual-status done">Manual full text: added</span>'
            if has_manual_applied_text
            else '<span class="manual-status todo">Manual full text: not added</span>'
        )
        manual_controls = (
            f"""
            <div class="applied-manual-input">
                <p><strong>Add full applied description to raw text</strong></p>
                <textarea class="raw-append-input" placeholder="Paste full job description here..."></textarea>
                <div><button type="button" class="raw-append-btn" onclick="appendAppliedRawText({job_id}, this)">Append to raw text</button></div>
            </div>
            """.strip()
            if is_applied and not has_manual_applied_text
            else ""
        )

        cards.append(
            f"""
            <article class="{card_class}" data-job-id="{job_id}">
                <span class="relevance-score" title="Relevance score">{relevance_score:.2f}</span>
                <p><strong>Title:</strong> <a href="{safe_link}" target="_blank" rel="noopener noreferrer">{role}</a> {easy_apply_badge}</p>
                {title_english_html}
                <p><strong>Source:</strong> {source}</p>
                <p><strong>Company:</strong> {company_html}</p>
                <p><strong>Place:</strong> {place}</p>
                <p><strong>Type:</strong> {work_type}</p>
                <p><strong>Description:</strong> {description}</p>
                {manual_status if is_applied else ""}
                {manual_controls}
                <p><strong>Skills:</strong> <span class="skill-tags">{skills_html or '<span class="skills-empty">No skills extracted</span>'}</span></p>
                <div class="feedback">
                    <label class="relevant-wrap"><input type="checkbox" {"checked" if str(item.get("category", "")).strip().lower() == "relevant" else ""} onchange="setRelevant({job_id}, this.checked, this)"/> Relevant</label>
                    <label class="viewed-wrap"><input type="checkbox" {"checked" if int(item.get("viewed", 0) or 0) == 1 else ""} onchange="setViewed({job_id}, this.checked, this)"/> Viewed</label>
                    <label class="applied-wrap"><input type="checkbox" {"checked" if int(item.get("applied", 0) or 0) == 1 else ""} onchange="setApplied({job_id}, this.checked, this)"/> Applied</label>
                    <span class="feedback-status"></span>
                </div>
            </article>
            """.strip()
        )
    return "".join(cards)


def _sort_positions_unviewed_then_score(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (viewed, -score)

    return sorted(list(items), key=_key)


def _sort_applied_positions(items: list[dict]) -> list[dict]:
    def _key(item: dict):
        has_manual_applied_text = MANUAL_APPLIED_RAW_MARKER in str(item.get("raw_text", ""))
        viewed = int(item.get("viewed", 0) or 0)
        score = float(item.get("relevance_score", 0) or 0.0)
        return (has_manual_applied_text, viewed, -score)

    return sorted(list(items), key=_key)


def _render_company_dashboard_html(company_name: str, company_items: list[dict]) -> str:
    company_label = (company_name or "").strip() or "Unknown company"
    safe_company_label = html_lib.escape(company_label)
    applied_items = [item for item in company_items if int(item.get("applied", 0) or 0) == 1]
    relevant_items = [
        item
        for item in company_items
        if str(item.get("category", "")).strip().lower() == "relevant"
        and int(item.get("applied", 0) or 0) != 1
    ]
    not_relevant_items = [
        item
        for item in company_items
        if str(item.get("category", "")).strip().lower() == "not relevant"
        and int(item.get("applied", 0) or 0) != 1
    ]

    relevant_items = _sort_positions_unviewed_then_score(relevant_items)
    not_relevant_items = _sort_positions_unviewed_then_score(not_relevant_items)
    applied_items = _sort_applied_positions(applied_items)

    relevant_cards = _build_job_cards(relevant_items, company_links=False, skill_buttons=False)
    not_relevant_cards = _build_job_cards(
        not_relevant_items,
        company_links=False,
        skill_buttons=False,
    )
    applied_cards = _build_job_cards(applied_items, company_links=False, skill_buttons=False)

    template = jinja_env.get_template("company_dashboard.html")
    return template.render(
        company_label=company_label,
        safe_company_label=safe_company_label,
        len_company_items=len(company_items),
        len_relevant_items=len(relevant_items),
        len_not_relevant_items=len(not_relevant_items),
        len_applied_items=len(applied_items),
        relevant_cards=relevant_cards,
        not_relevant_cards=not_relevant_cards,
        applied_cards=applied_cards,
        # Fallback empty string if not passed
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
):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)
    relevant_items = _sort_positions_unviewed_then_score(relevant_items)
    not_relevant_items = _sort_positions_unviewed_then_score(not_relevant_items)
    applied_items = _sort_applied_positions(applied_items)

    if relevant_total_count is None:
        relevant_total_count = len(relevant_items)
    if not_relevant_total_count is None:
        not_relevant_total_count = len(not_relevant_items)
    relevant_items = list(relevant_items)[: max(1, int(report_max_relevant_positions or 7))]
    not_relevant_items = list(not_relevant_items)[: max(1, int(report_max_not_relevant_positions or 7))]
    applied_items = list(applied_items)

    relevant_cards = _build_job_cards(relevant_items)
    not_relevant_cards = _build_job_cards(not_relevant_items)
    applied_cards = _build_job_cards(applied_items)
    skills_items = skills_items or []

    skills_rows = []
    for item in skills_items:
        skill_name = html_lib.escape(str(item.get("name", "")))
        skill_key = html_lib.escape(str(item.get("key", "")), quote=True)
        skill_key_js = html_lib.escape(json.dumps(str(item.get("key", ""))), quote=True)
        source = html_lib.escape(str(item.get("source", "")))
        occurrences = int(item.get("occurrences", 0) or 0)
        has_skill_checked = "checked" if bool(item.get("has_skill")) else ""
        learn_checked = "checked" if bool(item.get("want_to_learn")) else ""
        skills_rows.append(
            f"""
            <tr data-skill-key="{skill_key}">
                <td>{skill_name}</td>
                <td>{source}</td>
                <td>{occurrences}</td>
                <td><input type="checkbox" {has_skill_checked} onchange="setUserSkill({skill_key_js}, this.checked, this)" /></td>
                <td><input type="checkbox" {learn_checked} onchange="setLearnSkill({skill_key_js}, this.checked, this)" /></td>
                <td><button type="button" class="block-skill-btn" onclick="blockSkill({skill_key_js}, this)">Block</button><button type="button" class="delete-skill-btn" onclick="deleteSkill({skill_key_js}, this)">Delete</button></td>
            </tr>
            """.strip()
        )

    skills_table_html = (
        """
        <table class="skills-table">
            <thead>
                <tr>
                    <th>Skill</th>
                    <th>Source</th>
                    <th>Seen</th>
                    <th>I have</th>
                    <th>Learn</th>
                    <th>Action</th>
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

    template = jinja_env.get_template("dashboard.html")
    content = template.render(title=html_lib.escape(title), relevant_total_count=relevant_total_count, not_relevant_total_count=not_relevant_total_count, viewed_total=viewed_total, len_relevant_items=len(relevant_items), len_not_relevant_items=len(not_relevant_items), len_applied_items=len(applied_items), len_skills_items=len(skills_items) if skills_items else 0, relevant_cards=relevant_cards, not_relevant_cards=not_relevant_cards, applied_cards=applied_cards, skills_table_html=skills_table_html)

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    print(
        f"Wrote HTML dashboard: {out_html} "
        f"(relevant={len(relevant_items)}, not_relevant={len(not_relevant_items)}, applied={len(applied_items)}, viewed={int(viewed_total)})"
    )
