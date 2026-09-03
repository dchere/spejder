"""Field metadata for the dashboard profile editor."""

from __future__ import annotations

from typing import Any, Optional

READONLY_FIELDS = frozenset(
    {
        "skill_bigram_toxicity_threshold",
        "bad_cloud_seeded",
    }
)

GROUP_ORDER = (
    "keywords_scoring",
    "paths",
    "llm_ingest",
    "server",
    "language",
    "skills",
    "career_alert",
    "auto_written",
)

GROUP_TITLES = {
    "keywords_scoring": "Keywords / scoring",
    "paths": "Paths",
    "llm_ingest": "LLM / ingest",
    "server": "Server",
    "language": "Language",
    "skills": "Skills learning / extraction",
    "career_alert": "Career-alert artifacts",
    "auto_written": "Auto-written (read-only)",
}


def _field(
    group: str,
    label: str,
    widget: str,
    *,
    readonly: bool = False,
    help: Optional[str] = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "group": group,
        "label": label,
        "widget": widget,
        "readonly": readonly,
    }
    if help:
        entry["help"] = help
    return entry


# widget: checkbox | number | text | list_str | skill_patterns
PROFILE_FIELD_META: dict[str, dict[str, Any]] = {
    "include_keywords": _field(
        "keywords_scoring", "Include keywords", "list_str",
        help="One keyword per line. Boosts relevance when present in the job text.",
    ),
    "exclude_keywords": _field(
        "keywords_scoring", "Exclude keywords", "list_str",
        help="One keyword per line. Penalizes relevance when present.",
    ),
    "min_score": _field(
        "keywords_scoring", "Minimum score", "number",
        help="Jobs at or above this score are treated as relevant.",
    ),
    "learned_include_keywords": _field(
        "keywords_scoring", "Learned include keywords", "list_str",
    ),
    "learned_exclude_keywords": _field(
        "keywords_scoring", "Learned exclude keywords", "list_str",
    ),
    "report_max_relevant_positions": _field(
        "keywords_scoring", "Max relevant positions (report)", "number",
    ),
    "report_max_not_relevant_positions": _field(
        "keywords_scoring", "Max not-relevant positions (report)", "number",
    ),
    "skill_match_weight": _field("keywords_scoring", "Skill match weight", "number"),
    "skill_missing_penalty": _field("keywords_scoring", "Skill missing penalty", "number"),
    "easy_apply_bonus": _field(
        "keywords_scoring", "Easy Apply bonus", "number",
        help="Extra relevance for LinkedIn Easy Apply (0 disables).",
    ),
    "applied_company_bonus": _field(
        "keywords_scoring", "Applied company bonus", "number",
        help="Extra relevance for companies with an active applied/interview pipeline (0 disables).",
    ),
    "missing_skills_max_items": _field(
        "keywords_scoring", "Missing skills max items", "number",
    ),
    "default_inbox": _field(
        "paths", "Inbox path", "text",
        help="Startup-bound: changing this takes effect after restarting serve-gui.",
    ),
    "default_db": _field(
        "paths", "Database path", "text",
        help="Startup-bound: changing this takes effect after restarting serve-gui.",
    ),
    "default_report_dir": _field(
        "paths", "Report directory", "text",
        help="Startup-bound: changing this takes effect after restarting serve-gui.",
    ),
    "default_model": _field(
        "paths", "Default LLM model path", "text",
        help="Path to local GGUF model. Some LLM uses may need a restart to pick up a new path.",
    ),
    "default_cv_path": _field("paths", "CV path", "text"),
    "default_portrait_path": _field("paths", "Portrait file path", "text"),
    "max_input_chars": _field("llm_ingest", "Max input characters", "number"),
    "n_ctx": _field(
        "llm_ingest", "Context window (n_ctx)", "number",
        help="LLM context size. New LocalLLM instances use the live value; already-open models may need a restart.",
    ),
    "portrait_max_tokens": _field("llm_ingest", "Portrait max tokens", "number"),
    "server_host": _field(
        "server", "Server host", "text",
        help="Persisted immediately but requires restarting serve-gui to rebind.",
    ),
    "server_port": _field(
        "server", "Server port", "number",
        help="Persisted immediately but requires restarting serve-gui to rebind.",
    ),
    "language_checker_engine": _field("language", "Language checker engine", "text"),
    "language_checker_model_path": _field("language", "Language checker model path", "text"),
    "language_checker_threshold": _field("language", "Language checker threshold", "number"),
    "language_checker_min_letters": _field("language", "Language checker min letters", "number"),
    "language_translation_model_1": _field("language", "Translation model 1 path", "text"),
    "language_translation_source_1": _field(
        "language", "Translation source language 1", "text",
        help="ISO 639-1 code (e.g. da).",
    ),
    "language_translation_model_2": _field("language", "Translation model 2 path", "text"),
    "language_translation_source_2": _field("language", "Translation source language 2", "text"),
    "language_translation_model_3": _field("language", "Translation model 3 path", "text"),
    "language_translation_source_3": _field("language", "Translation source language 3", "text"),
    "skill_learning_max_positions": _field("skills", "Skill learning max positions", "number"),
    "skill_learning_min_occurrences": _field("skills", "Skill learning min occurrences", "number"),
    "skill_learning_max_new_patterns": _field("skills", "Skill learning max new patterns", "number"),
    "skill_new_confidence_threshold": _field("skills", "New skill confidence threshold", "number"),
    "skill_bigram_threshold_margin": _field(
        "skills", "Bigram toxicity threshold margin", "number",
        help="Operator-tunable margin for sync threshold calibration.",
    ),
    "user_skills": _field(
        "skills", "User skills", "list_str", help="One skill name per line.",
    ),
    "blocked_skills": _field(
        "skills", "Blocked skills", "list_str", help="One skill name per line.",
    ),
    "missing_skills_suggestions": _field("skills", "Missing skills suggestions", "list_str"),
    "known_skill_patterns": _field(
        "skills", "Known skill patterns", "skill_patterns",
        help="Name plus regex pattern rows used for skill matching.",
    ),
    "career_alert_artifacts_dir": _field("career_alert", "Artifacts directory", "text"),
    "career_alert_artifacts_disabled": _field(
        "career_alert", "Disabled artifact ids", "list_str",
        help="One artifact id per line.",
    ),
    "career_alert_synth_enabled": _field(
        "career_alert", "Auto-synthesize artifacts", "checkbox",
    ),
    "career_alert_synth_link_ratio": _field("career_alert", "Synth link ratio", "number"),
    "career_alert_synth_title_ratio": _field("career_alert", "Synth title ratio", "number"),
    "skill_bigram_toxicity_threshold": _field(
        "auto_written", "Skill bigram toxicity threshold", "number",
        readonly=True,
        help="Auto-written by sync calibration; not editable here.",
    ),
    "bad_cloud_seeded": _field(
        "auto_written", "Bad cloud seeded", "checkbox",
        readonly=True,
        help="One-time migration flag; not editable here.",
    ),
}
