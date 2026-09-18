"""Skills DB facade."""
from .skills_bad_ngrams import (
    count_bad_ngrams,
    get_bad_ngram_weights,
    upsert_bad_ngram_counts,
    upsert_bad_ngrams,
)
from .skills_delete import (
    cleanup_blocked_skills_from_db,
    delete_skill_from_db,
)
from .skills_links import (
    clear_job_skills_for_job,
    clear_job_skills_for_unviewed_jobs,
    get_job_ids_for_skill,
    get_job_skills,
    get_job_skills_for_jobs,
    replace_job_skills,
    set_job_skills,
)
from .skills_patterns import (
    _skill_to_regex_simple,
    get_skill_patterns,
    migrate_profile_skill_patterns_to_db,
    upsert_skill_pattern,
)
from .skills_rank import (
    count_job_links_for_skills,
    count_jobs_with_skill_links,
    get_top_skills_by_job_links,
)
