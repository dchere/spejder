"""Job card HTML builders for dashboards and reports."""

import html as html_lib
import os

import spejder.workflows
from spejder.core import MANUAL_APPLIED_RAW_MARKER
from spejder.extractors.skill_extractor import _normalize_skill_name

from .dashboard_templates import jinja_env


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

        place_line = f"<p><strong>Place:</strong> {place}</p>" if place else ""
        cards.append(
            f"""
            <article class="{card_class}">
                            <p><strong>Title:</strong> <a href="{safe_link}" target="_blank" rel="noopener noreferrer">{role}</a> {easy_apply_badge}</p>
                            {title_english_html}
                            <p><strong>Source:</strong> {source}</p>
                            <p><strong>Company:</strong> {company}</p>
                            {place_line}
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

        place_line = f"<p><strong>Place:</strong> {place}</p>" if place else ""
        cards.append(
            f"""
            <article class="{card_class}" data-job-id="{job_id}">
                <span class="relevance-score" title="Relevance score">{relevance_score:.2f}</span>
                <p><strong>Title:</strong> <a href="{safe_link}" target="_blank" rel="noopener noreferrer">{role}</a> {easy_apply_badge}</p>
                {title_english_html}
                <p><strong>Source:</strong> {source}</p>
                <p><strong>Company:</strong> {company_html}</p>
                {place_line}
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
