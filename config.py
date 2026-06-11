"""Configuration loader."""
import os
import json
from pydantic import BaseModel, Field

DEFAULT_PROFILE_FILE = "default_profile.json"

def _default_profile_file_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), DEFAULT_PROFILE_FILE)

class AppConfig(BaseModel):
    include_keywords: list[str] = Field(
        default_factory=lambda: ["python", "backend", "data", "machine learning", "ai", "developer", "engineer", "remote"]
    )
    exclude_keywords: list[str] = Field(
        default_factory=lambda: ["sales", "marketing", "nurse", "driver", "on-site", "onsite", "unpaid"]
    )
    min_score: float = 2.0
    learned_include_keywords: list[str] = Field(default_factory=list)
    learned_exclude_keywords: list[str] = Field(default_factory=list)
    
    default_inbox: str = "./inbox"
    default_db: str = "./jobs.db"
    default_report_dir: str = "./outbox"
    default_model: str = ""
    language_translation_model_1: str = ""
    language_translation_source_1: str = ""
    language_translation_model_2: str = ""
    language_translation_source_2: str = ""
    language_translation_model_3: str = ""
    language_translation_source_3: str = ""

    language_checker_engine: str = "fasttext"
    language_checker_model_path: str = ""
    language_checker_threshold: float = 0.8
    language_checker_min_letters: int = 4
    
    max_input_chars: int = 24000
    n_ctx: int = 8192
    
    server_host: str = "127.0.0.1"
    server_port: int = 8765
    
    report_max_relevant_positions: int = 7
    report_max_not_relevant_positions: int = 42
    
    skill_learning_max_positions: int = 180
    skill_learning_min_occurrences: int = 3
    skill_learning_max_new_patterns: int = 20
    skill_match_weight: float = 1.2
    skill_missing_penalty: float = 0.15
    easy_apply_bonus: float = 0.75
    missing_skills_max_items: int = 25
    skill_new_confidence_threshold: float = 0.9
    skill_new_max_per_job: int = 2
    
    user_skills: list[str] = Field(default_factory=list)
    blocked_skills: list[str] = Field(default_factory=list)
    skill_extraction_antipatterns: list[str] = Field(default_factory=list)
    skill_antipattern_synthesis_count: int = 3
    skill_antipattern_validation_runs: int = 3
    skill_antipattern_prompt_max_items: int = 40
    missing_skills_suggestions: list[str] = Field(default_factory=list)
    known_skill_patterns: list[dict] = Field(default_factory=list)

    @classmethod
    def load(cls, profile_path: str = None) -> "AppConfig":
        if profile_path is None:
            profile_path = _default_profile_file_path()
            
        data = {}
        if os.path.exists(profile_path):
            try:
                with open(profile_path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                if isinstance(loaded, dict):
                    data = loaded
            except (OSError, json.JSONDecodeError, ValueError, TypeError):
                pass
                
        # Backward compatibility for report_max_positions
        if "report_max_positions" in data and "report_max_relevant_positions" not in data:
            data["report_max_relevant_positions"] = data["report_max_positions"]
            data["report_max_not_relevant_positions"] = data["report_max_positions"]

        _migrate_translation_model_fields(data)

        return cls(**data)

    def save(self, profile_path: str = None) -> None:
        if profile_path is None:
            profile_path = _default_profile_file_path()
        with open(profile_path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, indent=2, ensure_ascii=False)

def _migrate_translation_model_fields(data: dict) -> None:
    if "translation_model_path" in data:
        if "danish_translation_model_path" not in data:
            data["danish_translation_model_path"] = data["translation_model_path"]
        del data["translation_model_path"]

    legacy_slots = (
        ("danish_translation_model_path", "language_translation_model_1", "language_translation_source_1", "da"),
        ("ukrainian_translation_model_path", "language_translation_model_2", "language_translation_source_2", "uk"),
    )
    for legacy_path_key, model_key, source_key, default_source in legacy_slots:
        legacy_path = str(data.get(legacy_path_key, "") or "").strip()
        if legacy_path and not str(data.get(model_key, "") or "").strip():
            data[model_key] = legacy_path
            if not str(data.get(source_key, "") or "").strip():
                data[source_key] = default_source
        data.pop(legacy_path_key, None)


def load_profile(profile_path: str = None) -> AppConfig:
    return AppConfig.load(profile_path)

def save_profile(config: AppConfig, profile_path: str = None) -> None:
    config.save(profile_path)
