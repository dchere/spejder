import threading
import time
from typing import Optional

from spejder.config import AppConfig
from spejder.db import (
    get_applied_jobs,
    get_job_skills,
    get_jobs_by_category,
    get_jobs_count_by_category,
    get_viewed_jobs_count,
)
from spejder.extractors.skill_extractor import _build_skills_tab_items, _format_skills
from spejder.llm import LocalLLM
from spejder.managers.dashboard_manager import _render_html_dashboard
from spejder.workflows.job_enrichment import (
    _build_title_fields,
    _fallback_description_text,
    _summary_for_display,
    materialize_jobs_skills,
)
from spejder.workflows.report_workflow import (
    _report_max_not_relevant_positions,
    _report_max_relevant_positions,
)


def coalesce_rebuild_reasons(reasons: list[str]) -> str:
    if not reasons:
        return ""
    reason = reasons[-1]
    if len(reasons) > 1:
        reason = f"{reason} (+{len(reasons) - 1} queued)"
    return reason


def build_dashboard_record(
    db_path: str,
    runtime_profile: AppConfig,
    title_translation_cache: dict[str, str],
    row: dict,
    default_category: str,
    default_viewed: int = 0,
    default_applied: int = 0,
    translate_title: bool = True,
) -> dict:
    raw_text = row.get("raw_text") or ""
    summary = _summary_for_display(
        row.get("summary", ""),
        raw_text,
    )
    cached_skills = get_job_skills(db_path, int(row.get("id", 0) or 0))
    title_fields = (
        _build_title_fields(
            db_path,
            row,
            runtime_profile=runtime_profile,
            title_translation_cache=title_translation_cache,
        )
        if translate_title
        else {
            "title": str(row.get("title", "") or ""),
            "title_english": str(row.get("title_english", "") or ""),
        }
    )

    return {
        "id": row.get("id", 0),
        "source": row.get("source", "Unknown"),
        "company": row.get("company", ""),
        **title_fields,
        "place": row.get("place", ""),
        "work_type": row.get("work_type", "Unknown"),
        "description": _fallback_description_text(row.get("description") or "", raw_text),
        "skills": _format_skills(cached_skills, limit=10),
        "position_link": row.get("position_link", ""),
        "raw_text": raw_text,
        "relevance_score": row.get("relevance_score", 0),
        "relevance_reason": row.get("relevance_reason", ""),
        "summary": summary,
        "category": row.get("category", default_category),
        "viewed": row.get("viewed", default_viewed),
        "applied": row.get("applied", default_applied),
    }


def populate_missing_dashboard_skills(
    db_path: str,
    runtime_profile: AppConfig,
    rows: list[dict],
    *,
    llm: Optional[LocalLLM] = None,
    progress_label: str = "",
) -> int:
    if not rows:
        return 0

    updated = materialize_jobs_skills(
        db_path,
        rows,
        llm=llm,
        runtime_profile=runtime_profile,
        limit=10,
        rescore=True,
        skip_cached=True,
        progress_label=progress_label,
    )
    return updated
class DashboardRebuildQueue:
    def __init__(self, db_path: str, dashboard_path: str, runtime_profile: AppConfig) -> None:
        self.db_path = db_path
        self.dashboard_path = dashboard_path
        self.runtime_profile = runtime_profile

        self._dashboard_lock = threading.Lock()
        self._rebuild_signal = threading.Event()
        self._rebuild_pending_lock = threading.Lock()
        self._rebuild_pending_reasons: list[str] = []
        self._title_translation_cache: dict[str, str] = {}

    def queue(self, reason: str = "") -> None:
        reason_text = str(reason or "").strip() or "queued update"
        with self._rebuild_pending_lock:
            self._rebuild_pending_reasons.append(reason_text)
        self._rebuild_signal.set()

    def start_worker(self) -> None:
        threading.Thread(
            target=self._worker,
            name="spejder-dashboard-rebuild-worker",
            daemon=True,
        ).start()

    def _worker(self) -> None:
        while True:
            self._rebuild_signal.wait()
            with self._rebuild_pending_lock:
                reasons = list(self._rebuild_pending_reasons)
                self._rebuild_pending_reasons.clear()
                self._rebuild_signal.clear()

            if not reasons:
                continue

            reason = coalesce_rebuild_reasons(reasons)
            self.rebuild(reason=reason)

    def rebuild(self, reason: str = "") -> None:
        should_log_rebuild = bool(reason)

        if should_log_rebuild:
            print(f"Dashboard rebuild: started ({reason})")
        for attempt in range(3):
            try:
                with self._dashboard_lock:
                    refreshed_report_data = {}
                    relevant_limit = _report_max_relevant_positions(self.runtime_profile)
                    not_relevant_limit = _report_max_not_relevant_positions(self.runtime_profile)
                    category_totals: dict[str, int] = {}
                    for cat in ["relevant", "not relevant"]:
                        total_rows = get_jobs_count_by_category(
                            self.db_path,
                            cat,
                            unviewed_only=True,
                        )
                        category_totals[cat] = int(total_rows)
                        report_limit = relevant_limit if cat == "relevant" else not_relevant_limit
                        rows = get_jobs_by_category(
                            self.db_path,
                            cat,
                            limit=report_limit,
                            unviewed_only=True,
                        )
                        if should_log_rebuild:
                            print(
                                "Dashboard rebuild: collecting "
                                f"{cat} (showing={len(rows)}, total_unviewed={total_rows})"
                            )
                        refreshed_report_data[cat] = [
                            build_dashboard_record(
                                self.db_path,
                                self.runtime_profile,
                                self._title_translation_cache,
                                row,
                                default_category=cat,
                                default_viewed=0,
                                default_applied=0,
                            )
                            for row in rows
                        ]

                    refreshed_applied_rows = get_applied_jobs(self.db_path, limit=0)
                    if should_log_rebuild:
                        print(
                            "Dashboard rebuild: collecting applied "
                            f"({len(refreshed_applied_rows)} rows)"
                        )
                    refreshed_applied_records = [
                        build_dashboard_record(
                            self.db_path,
                            self.runtime_profile,
                            self._title_translation_cache,
                            row,
                            default_category="relevant",
                            default_viewed=1,
                            default_applied=1,
                            translate_title=False,
                        )
                        for row in refreshed_applied_rows
                    ]

                    _render_html_dashboard(
                        refreshed_report_data.get("relevant", []),
                        refreshed_report_data.get("not relevant", []),
                        refreshed_applied_records,
                        self.dashboard_path,
                        "Positions Report",
                        viewed_total=get_viewed_jobs_count(self.db_path),
                        skills_items=_build_skills_tab_items(self.db_path, self.runtime_profile),
                        report_max_relevant_positions=_report_max_relevant_positions(
                            self.runtime_profile
                        ),
                        report_max_not_relevant_positions=_report_max_not_relevant_positions(
                            self.runtime_profile
                        ),
                        relevant_total_count=category_totals.get("relevant", 0),
                        not_relevant_total_count=category_totals.get("not relevant", 0),
                    )
                if should_log_rebuild and not reason.startswith("new record"):
                    print(f"Dashboard rebuild: done ({reason})")
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"Report regeneration failed ({reason or 'unknown'}): {exc}")
                else:
                    time.sleep(0.2)
