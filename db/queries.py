"""Job query facade — re-exports split query modules."""

from .queries_listings import (
    get_all_applied_jobs,
    get_applied_jobs,
    get_hidden_jobs,
    get_hidden_jobs_count,
    get_interview_jobs,
    get_jobs_by_category,
    get_jobs_by_category_paged,
    get_jobs_by_company,
    get_jobs_count_by_category,
    get_relevant_jobs,
    get_stopped_interview_jobs,
    get_viewed_jobs_count,
    get_viewed_today_jobs,
    local_day_start_utc_iso,
)
from .queries_refresh import (
    get_job_for_rescoring,
    get_jobs_for_active_rescore,
    get_jobs_for_description_refresh,
    get_jobs_for_scoring,
)
from .queries_signals import (
    get_all_jobs_for_dedupe,
    get_jobs_for_keyword_suggestions,
    get_jobs_for_skill_suggestions,
    get_jobs_merge_candidates,
    get_titles_for_labeled_jobs,
    get_titles_for_missing_skills,
)

__all__ = [
    "get_relevant_jobs",
    "get_jobs_by_category",
    "get_jobs_count_by_category",
    "get_jobs_by_category_paged",
    "get_jobs_by_company",
    "get_hidden_jobs",
    "get_hidden_jobs_count",
    "get_viewed_today_jobs",
    "local_day_start_utc_iso",
    "get_applied_jobs",
    "get_all_applied_jobs",
    "get_interview_jobs",
    "get_stopped_interview_jobs",
    "get_viewed_jobs_count",
    "get_jobs_for_description_refresh",
    "get_jobs_for_scoring",
    "get_jobs_for_active_rescore",
    "get_job_for_rescoring",
    "get_jobs_merge_candidates",
    "get_titles_for_labeled_jobs",
    "get_titles_for_missing_skills",
    "get_all_jobs_for_dedupe",
    "get_jobs_for_keyword_suggestions",
    "get_jobs_for_skill_suggestions",
]
