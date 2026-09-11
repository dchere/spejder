"""DB mutations facade."""
from .mutations_content import (
    append_applied_job_raw_text,
    set_job_cover_letter,
    set_job_cover_letter_requested,
    set_job_description,
    set_job_place,
    set_job_summary,
    set_job_title_english,
)
from .mutations_pipeline import (
    _HIDDEN_CLEAR_IF_VIEWED_OR_APPLIED,
    _INTERVIEW_FIELDS_CLEAR,
    set_job_applied,
    set_job_company_feedback,
    set_job_feedback,
    set_job_hidden,
    set_job_interview_stopped,
    set_job_on_interview,
    set_job_viewed,
)
from .mutations_upsert import (
    batch_update_and_delete_jobs,
    delete_jobs,
    update_job_source,
    update_jobs_relevance,
    upsert_job,
)
