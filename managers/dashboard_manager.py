"""
Dashboard and Reporting views renderer module for creating static HTML reports.
"""

import html as html_lib
import os
from email.utils import formatdate
from typing import Optional

from spejder.config import AppConfig
from spejder.extractors.skill_extractor.ui import SKILLS_EMPTY_ADDED_AT_SORT

from .dashboard_cards import _build_job_cards, _render_html_from_items
from .dashboard_skills_table import _render_skills_table_html
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

def _render_company_dashboard_html(
    company_name: str,
    company_items: list[dict],
    viewed_today_order: Optional[dict[int, int]] = None,
) -> str:
    company_label = (company_name or "").strip() or "Unknown company"
    safe_company_label = html_lib.escape(company_label)
    order = viewed_today_order or {}
    viewed_today_ids = set(order.keys())
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
    viewed_today_items = [
        item
        for item in company_items
        if int(item.get("id", 0) or 0) in viewed_today_ids
        and int(item.get("hidden", 0) or 0) == 0
        and int(item.get("applied", 0) or 0) == 0
    ]
    viewed_today_items.sort(
        key=lambda item: order.get(int(item.get("id", 0) or 0), 10**9)
    )
    relevant_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and int(item.get("id", 0) or 0) not in viewed_today_ids
        and str(item.get("category", "")).strip().lower() == "relevant"
        and int(item.get("applied", 0) or 0) != 1
    ]
    not_relevant_items = [
        item
        for item in company_items
        if int(item.get("hidden", 0) or 0) == 0
        and int(item.get("id", 0) or 0) not in viewed_today_ids
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
    viewed_today_cards = _build_job_cards(
        viewed_today_items, company_links=False, skill_buttons=False
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
        len_viewed_today_items=len(viewed_today_items),
        len_applied_items=len(applied_items),
        len_interview_items=len(interview_items),
        len_stopped_items=len(stopped_items),
        len_hidden_items=len(hidden_items),
        relevant_cards=relevant_cards,
        not_relevant_cards=not_relevant_cards,
        viewed_today_cards=viewed_today_cards,
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
    viewed_today_items: Optional[list[dict]] = None,
    runtime_profile: Optional[AppConfig] = None,
):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)
    relevant_items = _sort_positions_unviewed_then_score(relevant_items)
    not_relevant_items = _sort_positions_unviewed_then_score(not_relevant_items)
    applied_items = _sort_applied_positions(applied_items)
    interview_items = _sort_applied_positions(interview_items or [])
    stopped_items = _sort_applied_positions(stopped_items or [])
    hidden_items = _sort_positions_unviewed_then_score(hidden_items or [])
    # Preserve DB updated_at DESC from get_viewed_today_jobs / build helper.
    viewed_today_items = list(viewed_today_items or [])

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
    viewed_today_cards = _build_job_cards(viewed_today_items)
    applied_cards = _build_job_cards(applied_items, card_panel="applied")
    interview_cards = _build_job_cards(interview_items, card_panel="interview")
    stopped_cards = _build_job_cards(stopped_items, card_panel="stopped")
    hidden_cards = _build_job_cards(hidden_items, card_panel="hidden")
    skills_items = skills_items or []
    skills_table_html = _render_skills_table_html(skills_items)

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
        len_viewed_today_items=len(viewed_today_items),
        len_applied_items=len(applied_items),
        len_interview_items=len(interview_items),
        len_stopped_items=len(stopped_items),
        len_hidden_items=len(hidden_items),
        len_skills_items=len(skills_items) if skills_items else 0,
        relevant_cards=relevant_cards,
        not_relevant_cards=not_relevant_cards,
        viewed_today_cards=viewed_today_cards,
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
        f"viewed_today={len(viewed_today_items)}, "
        f"applied={len(applied_items)}, interview={len(interview_items)}, "
        f"stopped={len(stopped_items)}, hidden={len(hidden_items)}, "
        f"viewed={int(viewed_total)})"
    )
