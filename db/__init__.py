from importlib import import_module

_EXPORTS = {
    "_connect": "connection",
    "ensure_db": "connection",
    "get_job_link": "connection",
    "get_skill_patterns": "skills",
    "upsert_skill_pattern": "skills",
    "migrate_profile_skill_patterns_to_db": "skills",
    "replace_job_skills": "skills",
    "set_job_skills": "skills",
    "get_job_skills": "skills",
    "get_job_skills_for_jobs": "skills",
    "get_job_ids_for_skill": "skills",
    "get_top_skills_by_job_links": "skills",
    "count_job_links_for_skills": "skills",
    "count_jobs_with_skill_links": "skills",
    "position_pct": "skills",
    "clear_job_skills_for_unviewed_jobs": "skills",
    "clear_job_skills_for_job": "skills",
    "delete_skill_from_db": "skills",
    "cleanup_blocked_skills_from_db": "skills",
    "cleanup_stale_low_share_skills_from_db": "skills",
    "count_bad_ngrams": "skills",
    "summarize_bad_ngrams": "skills",
    "get_bad_ngram_weights": "skills",
    "upsert_bad_ngrams": "skills",
    "upsert_bad_ngram_counts": "skills",
    "decrement_bad_ngram_counts": "skills",
    "get_relevant_jobs": "queries",
    "get_jobs_by_category": "queries",
    "get_jobs_count_by_category": "queries",
    "get_jobs_by_category_paged": "queries",
    "get_jobs_by_company": "queries",
    "get_hidden_jobs": "queries",
    "get_hidden_jobs_count": "queries",
    "get_viewed_today_jobs": "queries",
    "local_day_start_utc_iso": "queries",
    "get_applied_jobs": "queries",
    "get_all_applied_jobs": "queries",
    "get_interview_jobs": "queries",
    "get_stopped_interview_jobs": "queries",
    "get_applied_pipeline_company_keys": "queries",
    "get_viewed_jobs_count": "queries",
    "get_jobs_for_description_refresh": "queries",
    "get_jobs_for_scoring": "queries",
    "get_jobs_for_active_rescore": "queries",
    "get_job_for_rescoring": "queries",
    "get_jobs_merge_candidates": "queries",
    "get_titles_for_labeled_jobs": "queries",
    "get_titles_for_missing_skills": "queries",
    "get_all_jobs_for_dedupe": "queries",
    "get_jobs_for_keyword_suggestions": "queries",
    "get_jobs_for_skill_suggestions": "queries",
    "upsert_job": "mutations",
    "set_job_summary": "mutations",
    "set_job_description": "mutations",
    "set_job_place": "mutations",
    "set_job_title_english": "mutations",
    "append_applied_job_raw_text": "mutations",
    "set_job_cover_letter": "mutations",
    "set_job_cover_letter_requested": "mutations",
    "set_job_feedback": "mutations",
    "set_job_viewed": "mutations",
    "set_job_applied": "mutations",
    "set_job_hidden": "mutations",
    "set_job_on_interview": "mutations",
    "set_job_interview_stopped": "mutations",
    "set_job_company_feedback": "mutations",
    "update_jobs_relevance": "mutations",
    "delete_jobs": "mutations",
    "update_job_source": "mutations",
    "batch_update_and_delete_jobs": "mutations",
    "sanitize_job_title": "utils",
    "_decode_mandrill_track_link": "utils",
    "_normalize_position_link": "utils",
    "_extract_jobindex_id": "utils",
    "_provider_from_link": "utils",
    "_is_djinni_position_link": "utils",
    "_is_teamtailor_position_link": "utils",
}

__all__ = list(_EXPORTS)


def __getattr__(name: str):
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(f".{module_name}", __name__), name)
    globals()[name] = value
    return value


def __dir__():
    return sorted(set(globals()) | set(_EXPORTS))
