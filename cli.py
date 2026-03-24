import argparse
import html
import json
import os
import re
import sys
import threading
import time
import webbrowser
from collections import Counter
from contextlib import nullcontext, suppress
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

try:
    from . import parser as email_parser
    from .jobs import (
        DEFAULT_PROFILE,
        apply_relevance,
        ensure_db,
        delete_skill_from_db,
        get_applied_jobs,
        get_job_skills,
        get_jobs_by_category,
        get_jobs_for_description_refresh,
        get_relevant_jobs,
        get_viewed_jobs_count,
        ingest_docs_to_db,
        load_profile,
        migrate_profile_skill_patterns_to_db,
        set_job_applied,
        set_job_description,
        set_job_feedback,
        set_job_skills,
        set_job_summary,
        set_job_viewed,
        clear_job_skills_for_unviewed_jobs,
        update_profile_from_db_signals,
        upsert_skill_pattern,
    )
    from .jobs import (
        get_skill_patterns as get_db_skill_patterns,
    )
    from .llm import LocalLLM
except ImportError:
    import parser as email_parser

    from jobs import (
        DEFAULT_PROFILE,
        apply_relevance,
        ensure_db,
        delete_skill_from_db,
        get_applied_jobs,
        get_job_skills,
        get_jobs_by_category,
        get_jobs_for_description_refresh,
        get_relevant_jobs,
        get_viewed_jobs_count,
        ingest_docs_to_db,
        load_profile,
        migrate_profile_skill_patterns_to_db,
        set_job_applied,
        set_job_description,
        set_job_feedback,
        set_job_skills,
        set_job_summary,
        set_job_viewed,
        clear_job_skills_for_unviewed_jobs,
        update_profile_from_db_signals,
        upsert_skill_pattern,
    )
    from jobs import (
        get_skill_patterns as get_db_skill_patterns,
    )
    from llm import LocalLLM


DEFAULT_PROFILE_PATH = "./profile.json"

SKILL_CUE_PATTERN = re.compile(
    r"\b(requirements?|required|must have|qualifications?|you (?:will|should)|experience with|we are looking for)\b",
    flags=re.IGNORECASE,
)
EASY_APPLY_PATTERN = re.compile(r"\beasy\s*apply\b", flags=re.IGNORECASE)


def _is_linkedin_item(source: str, position_link: str) -> bool:
    source_low = (source or "").strip().lower()
    link_low = (position_link or "").strip().lower()
    return source_low == "linkedin" or "linkedin.com/" in link_low


def _has_easy_apply_signal(*parts: str) -> bool:
    compact = " ".join(" ".join((part or "").split()) for part in parts if part)
    return bool(compact and EASY_APPLY_PATTERN.search(compact))


def _is_easy_apply_item(item: dict) -> bool:
    source = str(item.get("source", ""))
    position_link = str(item.get("position_link", ""))
    if not _is_linkedin_item(source, position_link):
        return False
    return _has_easy_apply_signal(
        str(item.get("title", "")),
        str(item.get("summary", "")),
        str(item.get("description", "")),
        str(item.get("raw_text", "")),
    )


def _normalize_skill_name(skill: str) -> str:
    s = (skill or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"^[-*\d.)\s]+", "", s)
    s = re.sub(r"^(?:a|an|as|at|you|but)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^you\s+(?:will|can|have|are|should|must)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^(?:good|great|strong|solid|excellent|proven|quality|high\s+quality)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:degree|bachelor(?:'s)?|master(?:'s)?|phd|doctorate)\s+in\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(
        r"^(?:experience\s+with|experienced\s+with|hands-?on\s+with|knowledge\s+of|familiarity\s+with)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(
        r"\b(?:required|required:|requirements?|qualifications?|must have|nice to have)\b",
        "",
        s,
        flags=re.IGNORECASE,
    )
    s = re.sub(r"\s+", " ", s).strip(" ,.;:-")
    if not s:
        return ""
    if len(s.split()) > 5:
        return ""
    if len(s) < 2:
        return ""
    return s


def _blocked_skill_keys(profile: Optional[dict] = None) -> set[str]:
    values = (profile or {}).get("blocked_skills") or []
    return {
        _normalize_skill_name(str(item)).lower()
        for item in values
        if _normalize_skill_name(str(item))
    }


def _filter_blocked_skill_names(skills: list[str], profile: Optional[dict] = None) -> list[str]:
    blocked = _blocked_skill_keys(profile)
    out = []
    seen = set()
    for skill in skills:
        normalized = _normalize_skill_name(skill)
        key = normalized.lower()
        if not normalized or key in blocked or key in seen:
            continue
        seen.add(key)
        out.append(normalized)
    return out


def _get_skill_patterns(
    db_path: str, profile: Optional[dict] = None
) -> list[tuple[str, str]]:
    blocked_keys = _blocked_skill_keys(profile)
    db_rows = get_db_skill_patterns(db_path, enabled_only=True)
    if db_rows:
        patterns = []
        for row in db_rows:
            name = str(row.get("name", "")).strip()
            pattern = str(row.get("pattern", "")).strip()
            if name and pattern and _normalize_skill_name(name).lower() not in blocked_keys:
                patterns.append((name, pattern))
        if patterns:
            return patterns

    raw = (profile or {}).get("known_skill_patterns") if profile else None
    if not isinstance(raw, list) or not raw:
        raw = []

    patterns: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not name or not pattern or _normalize_skill_name(name).lower() in blocked_keys:
            continue
        patterns.append((name, pattern))

    return patterns


def _split_skills_from_text(text: str) -> list[str]:
    compact = (text or "").replace("\n", ",")
    compact = re.sub(r"[;|/]+", ",", compact)
    parts = [p.strip() for p in compact.split(",") if p.strip()]
    out = []
    seen = set()
    for part in parts:
        item = _normalize_skill_name(part)
        key = item.lower()
        if item and key not in seen:
            seen.add(key)
            out.append(item)
    return out


def _extract_skills_fallback(
    text: str, skill_patterns: list[tuple[str, str]], limit: int = 10
) -> list[str]:
    source = " ".join((text or "").split())
    if not source:
        return []

    hits = []
    low = source.lower()
    for label, pattern in skill_patterns:
        m = re.search(pattern, low, flags=re.IGNORECASE)
        if m:
            hits.append((m.start(), label))
    hits.sort(key=lambda x: x[0])

    ordered = []
    seen = set()
    for _, label in hits:
        key = label.lower()
        if key not in seen:
            seen.add(key)
            ordered.append(label)

    if len(ordered) >= limit:
        return ordered[:limit]

    sentences = re.split(r"(?<=[.!?])\s+", source)
    phrase_candidates = []
    for sentence in sentences:
        if not sentence:
            continue
        if not SKILL_CUE_PATTERN.search(sentence):
            continue
        cleaned_sentence = re.sub(
            r"^.*?\b(?:requirements?|qualifications?)\b\s*:?",
            "",
            sentence,
            flags=re.IGNORECASE,
        )
        chunks = re.split(r",|\band\b|\bor\b", cleaned_sentence, flags=re.IGNORECASE)
        for chunk in chunks:
            chunk = _normalize_skill_name(chunk)
            if chunk:
                phrase_candidates.append(chunk)

    for skill in phrase_candidates:
        key = skill.lower()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(skill)
        if len(ordered) >= limit:
            break

    return ordered[:limit]


def _format_skills(skills: list[str], limit: int = 10) -> str:
    compact = []
    seen = set()
    for skill in skills:
        normalized = _normalize_skill_name(skill)
        key = normalized.lower()
        if normalized and key not in seen:
            seen.add(key)
            compact.append(normalized)
        if len(compact) >= limit:
            break
    return ", ".join(compact)


def _extract_job_skills(
    db_path: str,
    raw_text: str,
    llm: LocalLLM = None,
    profile: Optional[dict] = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
    limit: int = 10,
) -> str:
    cleaned = " ".join((raw_text or "").split())
    skill_patterns = _get_skill_patterns(db_path, profile)
    profile_data = profile or {}
    new_skill_conf_threshold = float(
        profile_data.get("skill_new_confidence_threshold", 0.9) or 0.9
    )
    new_skill_max_per_job = int(profile_data.get("skill_new_max_per_job", 2) or 2)
    known_by_key = {
        _normalize_skill_name(name).lower(): _normalize_skill_name(name)
        for name, _ in skill_patterns
        if _normalize_skill_name(name)
    }
    known_list = [known_by_key[k] for k in sorted(known_by_key.keys())]
    user_skills = []
    for item in profile_data.get("user_skills", []) or []:
        skill = _normalize_skill_name(str(item))
        if skill:
            user_skills.append(skill)
    user_skills = user_skills[:200]

    def clean_model_output(text: str) -> str:
        out = text or ""
        out = out.replace("```", " ")
        out = re.sub(r"\bskills?\s*:\s*", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\boutput\s*:\s*", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\bplaintext\b", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\s*[-*]\s*", ", ", out)
        out = re.sub(r"\s*\d+[.)]\s*", ", ", out)
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def _extract_json_object(text: str) -> dict:
        payload = (text or "").strip()
        if not payload:
            return {}
        start = payload.find("{")
        end = payload.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return {}
        candidate = payload[start : end + 1]
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
        return {}

    def _to_items(value) -> list[dict]:
        if not isinstance(value, list):
            return []
        out = []
        for item in value:
            if isinstance(item, str):
                out.append({"name": item, "confidence": 1.0, "evidence": ""})
            elif isinstance(item, dict):
                out.append(item)
        return out

    def _is_candidate_strong(skill_name: str, evidence: str, confidence: float) -> bool:
        skill = _normalize_skill_name(skill_name)
        if not skill:
            return False
        if len(skill.split()) > 4:
            return False
        if confidence < new_skill_conf_threshold:
            return False

        corpus = cleaned.lower()
        skill_low = skill.lower()
        if skill_low not in corpus:
            token_pattern = _skill_to_regex(skill)
            if token_pattern and not re.search(token_pattern, corpus, flags=re.IGNORECASE):
                return False

        if evidence:
            evidence_low = " ".join(evidence.split()).lower()
            if evidence_low and evidence_low not in corpus:
                return False

        cue_window = 200
        idx = corpus.find(skill_low)
        if idx != -1:
            window_start = max(0, idx - cue_window)
            window_end = min(len(corpus), idx + len(skill_low) + cue_window)
            window = corpus[window_start:window_end]
            if not SKILL_CUE_PATTERN.search(window):
                return False

        return True

    def _passes_phrase_quality(skill_name: str) -> bool:
        skill = _normalize_skill_name(skill_name)
        if not skill:
            return False

        # Reject pronoun/connector-led fragments that are usually narrative clauses.
        if re.match(r"^(?:our|we|you|they|it|this|that|these|those|and|or|but)\b", skill):
            return False

        tokens = [t for t in re.findall(r"[a-z0-9+#.]+", skill) if t]
        if not tokens:
            return False

        stop_tokens = {
            "our",
            "we",
            "you",
            "their",
            "team",
            "colleague",
            "company",
            "role",
            "position",
            "work",
            "used",
            "across",
            "with",
            "for",
            "and",
            "or",
            "but",
            "the",
            "a",
            "an",
            "as",
            "at",
            "to",
        }

        # If all tokens are generic business stop words, treat as non-skill phrase.
        if all(t in stop_tokens for t in tokens):
            return False

        return True

    if llm and cleaned:
        known_skills_prompt = ", ".join(known_list[:300])
        user_skills_prompt = ", ".join(user_skills)
        prompt = (
            "Task: Extract required professional/technical skills from the job text.\n"
            "Known skills (prefer these): "
            f"{known_skills_prompt}\n\n"
            "Candidate skills from user profile (extra context): "
            f"{user_skills_prompt}\n\n"
            "Hard rules:\n"
            "1) Return only concrete skill entities (tools, languages, frameworks, methods, domains, certifications).\n"
            "2) Exclude all narrative, hiring, company, and generic phrases.\n"
            "3) Exclude pronoun-led, hiring, marketing, editorial, and business narrative phrasing.\n"
            "4) Do NOT return sentence fragments or clauses.\n"
            "5) Every returned skill must be supported by exact evidence from the text.\n"
            "6) First select explicitly required skills from Known skills.\n"
            "7) Add new skills only if strongly relevant and explicitly required.\n"
            "8) Skill names only (up to 4 words), translated to english and lowercase.\n"
            "9) Remove qualitative adjectives from skill names (example: 'good software design' -> 'software design').\n\n"
            "10) Remove education prefixes from skill names (example: 'degree in electrical engineering' -> 'electrical engineering').\n\n"
            "11) Remove qualification prefixes from skill names (example: 'experience with microsoft dynamics 365' -> 'microsoft dynamics 365').\n\n"
            "Validation before output:\n"
            "- If name has stopwords-only business phrasing, drop it.\n"
            "- If no concrete skills found, return empty arrays.\n\n"
            "Output format (strict JSON) with keys matched_known and new_candidates, each an array of objects: "
            "{\"name\": string, \"confidence\": number, \"evidence\": string}.\n\n"
            f"Description:\n{cleaned}\n\n"
            "JSON:"
        )
        try:
            out = llm.generate(prompt, max_tokens=320)
            parsed_json = _extract_json_object(out)

            selected: list[str] = []
            seen = set()

            for item in _to_items(parsed_json.get("matched_known")):
                skill = _normalize_skill_name(str(item.get("name", "")))
                key = skill.lower()
                if not key or key not in known_by_key or key in seen:
                    continue
                if not _passes_phrase_quality(skill):
                    continue
                if key in cleaned.lower():
                    selected.append(known_by_key[key])
                    seen.add(key)
                if len(selected) >= int(limit):
                    break

            if len(selected) < int(limit):
                added_new = 0
                for item in _to_items(parsed_json.get("new_candidates")):
                    skill = _normalize_skill_name(str(item.get("name", "")))
                    key = skill.lower()
                    if not key or key in seen or key in known_by_key:
                        continue
                    confidence_raw = item.get("confidence", 0.0)
                    try:
                        confidence = float(confidence_raw)
                    except Exception:
                        confidence = 0.0
                    evidence = str(item.get("evidence", ""))
                    if not _is_candidate_strong(skill, evidence, confidence):
                        continue
                    if not _passes_phrase_quality(skill):
                        continue
                    selected.append(skill)
                    seen.add(key)
                    added_new += 1
                    if added_new >= max(0, int(new_skill_max_per_job)):
                        break
                    if len(selected) >= int(limit):
                        break

            if selected:
                filtered_selected = _filter_blocked_skill_names(selected, profile)
                if filtered_selected:
                    return _format_skills(filtered_selected, limit=limit)

            parsed_text = _split_skills_from_text(clean_model_output(out))
            constrained = []
            for skill in parsed_text:
                key = skill.lower()
                if key in known_by_key and _passes_phrase_quality(skill):
                    constrained.append(known_by_key[key])
            filtered_constrained = _filter_blocked_skill_names(constrained, profile)
            if filtered_constrained:
                return _format_skills(filtered_constrained, limit=limit)
        except Exception:
            pass

    fallback_source = cleaned
    if (
        not fallback_source
        and position_link
        and page_context_cache
        and position_link in page_context_cache
    ):
        fallback_source = page_context_cache.get(position_link, "")
    fallback_skills = _filter_blocked_skill_names(
        _extract_skills_fallback(fallback_source, skill_patterns=skill_patterns, limit=limit),
        profile,
    )
    return _format_skills(fallback_skills, limit=limit)


def _get_or_extract_job_skills(
    db_path: str,
    job_id: int,
    raw_text: str,
    llm: LocalLLM = None,
    profile: Optional[dict] = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
    limit: int = 10,
) -> str:
    """Return skill tags for a job, reading from the job_skills cache or extracting + caching."""
    if job_id:
        cached = get_job_skills(db_path, job_id)
        if cached:
            return _format_skills(_filter_blocked_skill_names(cached, profile), limit=limit)
    skills_text = _extract_job_skills(
        db_path,
        raw_text,
        llm=llm,
        profile=profile,
        position_link=position_link,
        page_context_cache=page_context_cache,
        limit=limit,
    )
    if job_id and skills_text:
        set_job_skills(db_path, job_id, [s.strip() for s in skills_text.split(",") if s.strip()])
    return skills_text


def _skill_to_regex(skill_name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(r"[A-Za-z0-9+#.]+", skill_name or "") if t]
    if not tokens:
        return ""
    return r"\b" + r"\s+".join(tokens) + r"\b"


def _learn_skill_patterns_from_positions(
    db_path: str,
    runtime_profile: dict,
    llm: LocalLLM = None,
    progress: bool = False,
    progress_label: str = "Skill pattern learning",
) -> dict:
    # Learn from user-positive signals first: applied jobs, then relevant jobs.
    applied_rows = get_applied_jobs(db_path, limit=0)
    relevant_rows = get_jobs_by_category(db_path, "relevant", limit=0, unviewed_only=False)

    rows = []
    seen_ids = set()
    for row in applied_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        rows.append((row, 3))
    for row in relevant_rows:
        rid = int(row.get("id", 0) or 0)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)
        rows.append((row, 1))

    if not rows:
        if progress:
            print(f"{progress_label}: no applied/relevant positions found")
        return {
            "considered_positions": 0,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": len(_get_skill_patterns(db_path, runtime_profile)),
        }

    max_positions = int(runtime_profile.get("skill_learning_max_positions", 180) or 180)
    min_occurrences = int(runtime_profile.get("skill_learning_min_occurrences", 3) or 3)
    max_new = int(runtime_profile.get("skill_learning_max_new_patterns", 20) or 20)

    counts: Counter[str] = Counter()
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    considered = 0

    if progress:
        print(f"{progress_label}: starting (positions={min(len(rows), max_positions)})")

    for row, weight in rows[:max_positions]:
        raw = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            title_translation_cache=title_translation_cache,
        )
        skills_text = _get_or_extract_job_skills(
            db_path,
            row.get("id", 0),
            raw,
            llm=llm,
            profile=runtime_profile,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
            limit=10,
        )
        skills = [
            _normalize_skill_name(s) for s in skills_text.split(",") if _normalize_skill_name(s)
        ]
        for skill in skills:
            counts[skill] += int(weight)
        considered += 1
        if progress and (considered % 10 == 0 or considered == min(len(rows), max_positions)):
            print(f"{progress_label}: {considered}/{min(len(rows), max_positions)} processed")

    existing_patterns = _get_skill_patterns(db_path, runtime_profile)
    existing_names = {name.strip().lower() for name, _ in existing_patterns}
    existing_map = {name.strip().lower(): pattern for name, pattern in existing_patterns}

    for skill, score in counts.items():
        key = skill.strip().lower()
        if key not in existing_names:
            continue
        pattern = existing_map.get(key, "")
        if not pattern:
            continue
        upsert_skill_pattern(
            db_path,
            name=skill,
            pattern=pattern,
            source="learned",
            occurrences_inc=int(score),
            weight_inc=float(score),
            enabled=True,
        )

    candidates = [
        name
        for name, score in counts.most_common()
        if score >= min_occurrences and name.strip().lower() not in existing_names
    ]
    to_add = candidates[:max_new]
    if not to_add:
        if progress:
            print(f"{progress_label}: done (no new patterns)")
        return {
            "considered_positions": considered,
            "new_skill_patterns": 0,
            "total_known_skill_patterns": len(existing_patterns),
        }

    added = 0
    for skill in to_add:
        key = skill.strip().lower()
        if not key:
            continue
        pattern = _skill_to_regex(skill)
        if not pattern:
            continue
        ok = upsert_skill_pattern(
            db_path,
            name=skill,
            pattern=pattern,
            source="learned",
            occurrences_inc=int(counts.get(skill, 0)),
            weight_inc=float(counts.get(skill, 0)),
            enabled=True,
        )
        if ok:
            added += 1

    total_patterns = len(_get_skill_patterns(db_path, runtime_profile))
    if progress:
        print(
            f"{progress_label}: done (new_patterns={int(added)}, total_patterns={int(total_patterns)})"
        )
    return {
        "considered_positions": considered,
        "new_skill_patterns": int(added),
        "total_known_skill_patterns": int(total_patterns),
    }


def _ensure_skill_pattern_seed_migration(db_path: str, profile_path: str):
    with suppress(Exception):
        migrate_profile_skill_patterns_to_db(db_path, profile_path)


def _load_cv_text(cv_path: str, max_chars: int = 40000) -> str:
    path = (cv_path or "").strip()
    if not path:
        return ""

    if os.path.isfile(path):
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                return f.read()[:max_chars]
        except Exception:
            return ""

    if os.path.isdir(path):
        chunks = []
        total = 0
        allowed_ext = {".txt", ".md", ".rst", ".html", ".htm", ".eml"}
        for root, _, files in os.walk(path):
            for name in files:
                ext = os.path.splitext(name)[1].lower()
                if ext and ext not in allowed_ext:
                    continue
                fp = os.path.join(root, name)
                try:
                    with open(fp, encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                except Exception:
                    continue
                if not content:
                    continue
                header = f"\n\n[CV_FILE {fp}]\n"
                part = header + content
                remaining = max_chars - total
                if remaining <= 0:
                    return "".join(chunks)
                chunks.append(part[:remaining])
                total += min(len(part), remaining)
        return "".join(chunks)

    return ""


def _extract_user_skills_from_cv(
    cv_text: str,
    db_path: str,
    profile: dict,
    llm: LocalLLM = None,
    limit: int = 80,
) -> list[str]:
    compact = " ".join((cv_text or "").split())
    if not compact:
        return []

    def _cleanup(items: list[str], max_items: int) -> list[str]:
        out = []
        seen = set()
        for item in items:
            name = _normalize_skill_name(item)
            key = name.lower()
            if not name or key in seen:
                continue
            seen.add(key)
            out.append(name)
            if len(out) >= max_items:
                break
        return out

    if llm:
        prompt = (
            f"Extract up to {int(limit)} technical and professional skills from this CV. "
            "Return only a comma-separated list of skill names (1-4 words each). "
            "Do not include job titles, companies, or generic soft phrases.\n\n"
            f"CV:\n{compact[:40000]}\n\n"
            "Skills:"
        )
        try:
            out = llm.generate(prompt, max_tokens=320)
            parsed = _split_skills_from_text(out)
            cleaned = _cleanup(parsed, int(limit))
            if cleaned:
                return cleaned
        except Exception:
            pass

    fallback = _extract_skills_fallback(
        compact,
        skill_patterns=_get_skill_patterns(db_path, profile),
        limit=max(20, int(limit)),
    )
    return _cleanup(fallback, int(limit))


def cmd_sync_user_skills(args):
    profile_path = args.profile or DEFAULT_PROFILE_PATH
    runtime_profile = _load_runtime_profile(profile_path)
    db_path = args.db or runtime_profile.get("default_db") or "./jobs.db"
    model_path = args.model or runtime_profile.get("default_model") or ""
    cv_path = args.cv or "./CV"

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)

    print(f"Sync user skills: loading CV from {cv_path}")

    cv_text = _load_cv_text(cv_path, max_chars=int(args.max_chars))
    if not cv_text.strip():
        print(f"CV not found or empty: {cv_path}")
        return

    print(f"Sync user skills: CV text loaded (chars={len(cv_text)})")

    llm = LocalLLM(model_path=model_path, verbose=not args.quiet_model) if model_path else None
    if llm:
        print("Sync user skills: extracting with model")
    else:
        print("Sync user skills: extracting with fallback rules (no model)")
    extracted = _extract_user_skills_from_cv(
        cv_text,
        db_path=db_path,
        profile=runtime_profile,
        llm=llm,
        limit=int(args.limit),
    )

    if not extracted:
        print("No skills extracted from CV.")
        return

    print(f"Sync user skills: extracted {len(extracted)} skills, updating profile")

    profile_data = DEFAULT_PROFILE.copy()
    if profile_path and os.path.exists(profile_path):
        try:
            with open(profile_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                profile_data.update(loaded)
        except Exception:
            pass

    existing = [_normalize_skill_name(s) for s in (profile_data.get("user_skills") or [])]
    existing = [s for s in existing if s]

    if args.replace:
        merged = extracted
    else:
        merged = existing + extracted
        dedup = []
        seen = set()
        for s in merged:
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            dedup.append(s)
        merged = dedup

    profile_data["user_skills"] = merged
    os.makedirs(os.path.dirname(os.path.abspath(profile_path)), exist_ok=True)
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile_data, f, ensure_ascii=False, indent=2)

    print(
        f"User skills synced from CV: extracted={len(extracted)}, "
        f"total_user_skills={len(merged)}, profile={profile_path}"
    )
    print("Top extracted:", ", ".join(extracted[:20]))


def _load_runtime_profile(profile_path: str):
    path = profile_path or DEFAULT_PROFILE_PATH
    if path and os.path.exists(path):
        try:
            return load_profile(path)
        except Exception as exc:
            print(f"Could not load profile '{path}': {exc}. Using built-in defaults.")
    return DEFAULT_PROFILE.copy()


def _save_profile(profile_path: str, profile: dict) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(profile_path)), exist_ok=True)
    with open(profile_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)


def _toggle_profile_skill(profile: dict, field: str, skill_name: str, enabled: bool) -> bool:
    skill = _normalize_skill_name(skill_name)
    key = skill.lower()
    if not key:
        return False

    values = profile.get(field)
    if not isinstance(values, list):
        values = []

    seen = set()
    cleaned = []
    for item in values:
        normalized = _normalize_skill_name(str(item))
        normalized_key = normalized.lower()
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized)

    had = key in seen
    changed = False
    if enabled and not had:
        cleaned.append(skill)
        changed = True
    if not enabled and had:
        cleaned = [item for item in cleaned if item.lower() != key]
        changed = True

    profile[field] = cleaned
    return changed


def _remove_skill_from_profile(profile: dict, skill_name: str) -> dict[str, int]:
    key = _normalize_skill_name(skill_name).lower()
    if not key:
        return {"removed": 0}

    removed = 0
    list_fields = [
        "user_skills",
        "missing_skills_suggestions",
        "include_keywords",
        "exclude_keywords",
        "learned_include_keywords",
        "learned_exclude_keywords",
    ]
    for field in list_fields:
        values = profile.get(field)
        if not isinstance(values, list):
            continue
        kept = []
        for item in values:
            normalized = _normalize_skill_name(str(item))
            if normalized.lower() == key:
                removed += 1
                continue
            kept.append(item)
        profile[field] = kept

    patterns = profile.get("known_skill_patterns")
    if isinstance(patterns, list):
        kept_patterns = []
        for item in patterns:
            if not isinstance(item, dict):
                kept_patterns.append(item)
                continue
            name = _normalize_skill_name(str(item.get("name", "")))
            if name.lower() == key:
                removed += 1
                continue
            kept_patterns.append(item)
        profile["known_skill_patterns"] = kept_patterns

    return {"removed": int(removed)}


def _block_skill_in_profile(profile: dict, skill_name: str) -> dict[str, int]:
    skill = _normalize_skill_name(skill_name)
    key = skill.lower()
    if not key:
        return {"blocked_added": 0, "removed": 0}

    removed_info = _remove_skill_from_profile(profile, skill)
    blocked_values = profile.get("blocked_skills")
    if not isinstance(blocked_values, list):
        blocked_values = []

    cleaned = []
    seen = set()
    for item in blocked_values:
        normalized = _normalize_skill_name(str(item))
        normalized_key = normalized.lower()
        if not normalized_key or normalized_key in seen:
            continue
        seen.add(normalized_key)
        cleaned.append(normalized)

    blocked_added = 0
    if key not in seen:
        cleaned.append(skill)
        blocked_added = 1

    profile["blocked_skills"] = cleaned
    return {"blocked_added": int(blocked_added), "removed": int(removed_info.get("removed", 0))}


def _build_skills_tab_items(db_path: str, profile: dict) -> list[dict]:
    blocked_keys = _blocked_skill_keys(profile)
    user_keys = {
        _normalize_skill_name(str(s)).lower()
        for s in (profile.get("user_skills") or [])
        if _normalize_skill_name(str(s)) and _normalize_skill_name(str(s)).lower() not in blocked_keys
    }
    learn_keys = {
        _normalize_skill_name(str(s)).lower()
        for s in (profile.get("missing_skills_suggestions") or [])
        if _normalize_skill_name(str(s)) and _normalize_skill_name(str(s)).lower() not in blocked_keys
    }

    by_key: dict[str, dict] = {}

    def upsert(name: str, source: str, occurrences: int = 0, weight: float = 0.0):
        clean = _normalize_skill_name(name)
        key = clean.lower()
        if not key or key in blocked_keys:
            return
        row = by_key.get(key)
        if row is None:
            row = {
                "name": clean,
                "key": key,
                "source": source,
                "occurrences": int(occurrences),
                "weight": float(weight),
                "has_skill": key in user_keys,
                "want_to_learn": key in learn_keys,
            }
            by_key[key] = row
            return
        if source == "db":
            row["source"] = "db"
            row["occurrences"] = max(int(row.get("occurrences", 0)), int(occurrences))
            row["weight"] = max(float(row.get("weight", 0.0)), float(weight))
        row["has_skill"] = row["has_skill"] or (key in user_keys)
        row["want_to_learn"] = row["want_to_learn"] or (key in learn_keys)

    for item in get_db_skill_patterns(db_path, enabled_only=False):
        upsert(
            str(item.get("name", "")),
            "db",
            occurrences=int(item.get("occurrences", 0) or 0),
            weight=float(item.get("weight", 0.0) or 0.0),
        )

    for item in profile.get("known_skill_patterns", []) or []:
        if isinstance(item, dict):
            upsert(str(item.get("name", "")), "profile")

    for item in profile.get("user_skills", []) or []:
        upsert(str(item), "profile")

    for item in profile.get("missing_skills_suggestions", []) or []:
        upsert(str(item), "profile")

    rows = list(by_key.values())
    rows.sort(key=lambda x: (x["name"].lower(), -x.get("occurrences", 0)))
    return rows


def _render_html_from_items(items, out_html: str, title: str):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)

    cards = []
    for item in items:
        source = html.escape(str(item.get("source", "Unknown")))
        company = html.escape(str(item.get("company", "")))
        role = html.escape(str(item.get("title", "")))
        place = html.escape(str(item.get("place", "")))
        work_type = html.escape(str(item.get("work_type", "Unknown")))
        description = html.escape(str(item.get("description", "")))
        skills_text = str(item.get("skills", ""))
        skills_items = [html.escape(s.strip()) for s in skills_text.split(",") if s.strip()]
        skills_html = "".join([f'<span class="skill-tag">{skill}</span>' for skill in skills_items])
        link = str(item.get("position_link", ""))
        safe_link = html.escape(link, quote=True)
        is_easy_apply = _is_easy_apply_item(item)
        easy_apply_badge = (
            '<span class="easy-apply-badge" title="LinkedIn Easy Apply detected">Easy Apply</span>'
            if is_easy_apply
            else ""
        )
        card_class = "card easy-apply-card" if is_easy_apply else "card"

        cards.append(
            f"""
            <article class=\"{card_class}\">
                            <p><strong>Title:</strong> <a href=\"{safe_link}\" target=\"_blank\" rel=\"noopener noreferrer\">{role}</a> {easy_apply_badge}</p>
                            <p><strong>Source:</strong> {source}</p>
                            <p><strong>Company:</strong> {company}</p>
                            <p><strong>Place:</strong> {place}</p>
                            <p><strong>Type:</strong> {work_type}</p>
                            <p><strong>Description:</strong> {description}</p>
                            <p><strong>Skills:</strong> <span class=\"skill-tags\">{skills_html or '<span class="skills-empty">No skills extracted</span>'}</span></p>
            </article>
            """.strip()
        )

    content = f"""<!doctype html>
<html lang=\"en\">
    <head>
        <meta charset=\"utf-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>{html.escape(title)}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; background: #f7f7f7; color: #111; }}
            h1 {{ margin-bottom: 4px; }}
            .subtitle {{ margin-top: 0; color: #555; }}
            .grid {{ display: grid; gap: 12px; }}
            .card {{ background: #fff; border: 1px solid #ddd; border-radius: 10px; padding: 12px; }}
            .easy-apply-card {{ border-color: #d7a127; box-shadow: inset 0 0 0 1px #f6d36b; }}
            .easy-apply-badge {{ display: inline-block; margin-left: 6px; padding: 2px 8px; border-radius: 999px; border: 1px solid #d7a127; background: #fff6d9; color: #7a5a00; font-size: 11px; font-weight: 700; vertical-align: middle; }}
            .card p {{ margin: 6px 0; }}
            .skill-tags {{ display: inline-flex; flex-wrap: wrap; gap: 6px; vertical-align: middle; }}
            .skill-tag {{ display: inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid #c9d6f0; background: #eef4ff; color: #1f3a6d; font-size: 12px; }}
            .skills-empty {{ color: #777; font-size: 12px; }}
            a {{ color: #0a58ca; word-break: break-all; }}
        </style>
    </head>
    <body>
        <h1>{html.escape(title)}</h1>
        <p class=\"subtitle\">Items: {len(items)}</p>
        <section class=\"grid\">{"".join(cards) if cards else "<p>No records found.</p>"}</section>
    </body>
</html>
"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Wrote HTML report: {out_html} (items={len(items)})")


def _render_html_dashboard(
    relevant_items,
    not_relevant_items,
    applied_items,
    out_html: str,
    title: str,
    viewed_total: int = 0,
    skills_items: Optional[list[dict]] = None,
):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)

    def build_cards(items):
        cards = []
        for item in items:
            job_id = int(item.get("id", 0) or 0)
            source = html.escape(str(item.get("source", "Unknown")))
            company = html.escape(str(item.get("company", "")))
            role = html.escape(str(item.get("title", "")))
            place = html.escape(str(item.get("place", "")))
            work_type = html.escape(str(item.get("work_type", "Unknown")))
            description = html.escape(str(item.get("description", "")))
            skills_text = str(item.get("skills", ""))
            skill_tags = []
            for raw_skill in [s.strip() for s in skills_text.split(",") if s.strip()]:
                skill_label = html.escape(raw_skill)
                skill_key = html.escape(_normalize_skill_name(raw_skill), quote=True)
                if not skill_key:
                    continue
                skill_tags.append(
                    f'<button type="button" class="skill-tag skill-tag-btn" data-skill-key="{skill_key}" onclick="openSkillsForSkill(this.dataset.skillKey)">{skill_label}</button>'
                )
            skills_html = "".join(skill_tags)
            relevance_score = float(item.get("relevance_score", 0) or 0)
            link = str(item.get("position_link", ""))
            safe_link = html.escape(link, quote=True)
            is_easy_apply = _is_easy_apply_item(item)
            easy_apply_badge = (
                '<span class="easy-apply-badge" title="LinkedIn Easy Apply detected">Easy Apply</span>'
                if is_easy_apply
                else ""
            )
            card_class = "card easy-apply-card" if is_easy_apply else "card"

            cards.append(
                f"""
                <article class=\"{card_class}\" data-job-id=\"{job_id}\">
                    <span class=\"relevance-score\" title=\"Relevance score\">{relevance_score:.2f}</span>
                    <p><strong>Title:</strong> <a href=\"{safe_link}\" target=\"_blank\" rel=\"noopener noreferrer\">{role}</a> {easy_apply_badge}</p>
                    <p><strong>Source:</strong> {source}</p>
                    <p><strong>Company:</strong> {company}</p>
                    <p><strong>Place:</strong> {place}</p>
                    <p><strong>Type:</strong> {work_type}</p>
                    <p><strong>Description:</strong> {description}</p>
                    <p><strong>Skills:</strong> <span class=\"skill-tags\">{skills_html or '<span class="skills-empty">No skills extracted</span>'}</span></p>
                    <div class=\"feedback\">
                        <label class=\"relevant-wrap\"><input type=\"checkbox\" {"checked" if str(item.get("category", "")).strip().lower() == "relevant" else ""} onchange=\"setRelevant({job_id}, this.checked, this)\"/> Relevant</label>
                        <label class=\"viewed-wrap\"><input type=\"checkbox\" {"checked" if int(item.get("viewed", 0) or 0) == 1 else ""} onchange=\"setViewed({job_id}, this.checked, this)\"/> Viewed</label>
                        <label class=\"applied-wrap\"><input type=\"checkbox\" {"checked" if int(item.get("applied", 0) or 0) == 1 else ""} onchange=\"setApplied({job_id}, this.checked, this)\"/> Applied</label>
                        <span class=\"feedback-status\"></span>
                    </div>
                </article>
                """.strip()
            )
        return "".join(cards)

    relevant_cards = build_cards(relevant_items)
    not_relevant_cards = build_cards(not_relevant_items)
    applied_cards = build_cards(applied_items)
    skills_items = skills_items or []

    skills_rows = []
    for item in skills_items:
        skill_name = html.escape(str(item.get("name", "")))
        skill_key = html.escape(str(item.get("key", "")), quote=True)
        skill_key_js = html.escape(json.dumps(str(item.get("key", ""))), quote=True)
        source = html.escape(str(item.get("source", "")))
        occurrences = int(item.get("occurrences", 0) or 0)
        has_skill_checked = "checked" if bool(item.get("has_skill")) else ""
        learn_checked = "checked" if bool(item.get("want_to_learn")) else ""
        skills_rows.append(
            f"""
            <tr data-skill-key=\"{skill_key}\">
                <td>{skill_name}</td>
                <td>{source}</td>
                <td>{occurrences}</td>
                <td><input type=\"checkbox\" {has_skill_checked} onchange=\"setUserSkill({skill_key_js}, this.checked, this)\" /></td>
                <td><input type=\"checkbox\" {learn_checked} onchange=\"setLearnSkill({skill_key_js}, this.checked, this)\" /></td>
                <td><button type=\"button\" class=\"block-skill-btn\" onclick=\"blockSkill({skill_key_js}, this)\">Block</button><button type=\"button\" class=\"delete-skill-btn\" onclick=\"deleteSkill({skill_key_js}, this)\">Delete</button></td>
            </tr>
            """.strip()
        )

    skills_table_html = (
        """
        <table class=\"skills-table\">
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

    content = f"""<!doctype html>
<html lang=\"en\">
    <head>
        <meta charset=\"utf-8\" />
        <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
        <title>{html.escape(title)}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 24px; background: #f7f7f7; color: #111; }}
            h1 {{ margin-bottom: 8px; }}
            .controls {{ display: flex; gap: 8px; margin-bottom: 12px; }}
            .mode-btn {{ border: 1px solid #bbb; background: #fff; border-radius: 8px; padding: 8px 12px; cursor: pointer; }}
            .mode-btn.active {{ border-color: #0a58ca; font-weight: 700; }}
            .grid {{ display: grid; gap: 12px; }}
            .card {{ background: #fff; border: 1px solid #ddd; border-radius: 10px; padding: 12px; position: relative; }}
            .easy-apply-card {{ border-color: #d7a127; box-shadow: inset 0 0 0 1px #f6d36b; }}
            .easy-apply-badge {{ display: inline-block; margin-left: 6px; padding: 2px 8px; border-radius: 999px; border: 1px solid #d7a127; background: #fff6d9; color: #7a5a00; font-size: 11px; font-weight: 700; vertical-align: middle; }}
            .card p {{ margin: 6px 0; }}
            .skill-tags {{ display: inline-flex; flex-wrap: wrap; gap: 6px; vertical-align: middle; }}
            .skill-tag {{ display: inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid #c9d6f0; background: #eef4ff; color: #1f3a6d; font-size: 12px; }}
            .skill-tag-btn {{ cursor: pointer; }}
            .skills-empty {{ color: #777; font-size: 12px; }}
            .relevance-score {{ position: absolute; top: 8px; right: 10px; font-size: 11px; color: #9aa0a6; }}
            .feedback {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; flex-wrap: wrap; }}
            .relevant-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .viewed-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .applied-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .feedback-status {{ font-size: 12px; color: #555; }}
            a {{ color: #0a58ca; word-break: break-all; }}
            .hidden {{ display: none; }}
            .empty {{ background: #fff; border: 1px dashed #ccc; border-radius: 10px; padding: 12px; margin: 0; }}
            .skills-wrap {{ background: #fff; border: 1px solid #ddd; border-radius: 10px; padding: 12px; overflow-x: auto; }}
            .skills-table {{ width: 100%; border-collapse: collapse; min-width: 760px; }}
            .skills-table th, .skills-table td {{ border-bottom: 1px solid #eee; text-align: left; padding: 8px; font-size: 13px; }}
            .skills-table th {{ background: #fafafa; font-weight: 700; }}
            .skills-table tbody tr.skill-row-focus {{ outline: 2px solid #0a58ca; outline-offset: -2px; background: #eef4ff; transition: background-color 0.8s ease; }}
            .delete-skill-btn {{ border: 1px solid #cc8c8c; color: #7b1111; background: #fff; border-radius: 6px; padding: 4px 8px; cursor: pointer; }}
            .block-skill-btn {{ border: 1px solid #d2a24a; color: #7a5200; background: #fff7e8; border-radius: 6px; padding: 4px 8px; cursor: pointer; margin-right: 6px; }}
        </style>
    </head>
    <body>
        <h1>{html.escape(title)}</h1>
        <div class=\"controls\">
            <button id=\"btn-relevant\" class=\"mode-btn active\" type=\"button\">Relevant ({len(relevant_items)})</button>
            <button id=\"btn-not-relevant\" class=\"mode-btn\" type=\"button\">Not relevant ({len(not_relevant_items)})</button>
            <button id=\"btn-applied\" class=\"mode-btn\" type=\"button\">Applied ({len(applied_items)})</button>
            <button id="btn-skills" class="mode-btn" type="button">Skills ({len(skills_items)})</button>
        </div>
        <section id=\"panel-relevant\" class=\"grid\">{relevant_cards}</section>
        <section id=\"panel-not-relevant\" class=\"grid hidden\">{not_relevant_cards}</section>
        <section id=\"panel-applied\" class=\"grid hidden\">{applied_cards}</section>
        <section id="panel-skills" class="skills-wrap hidden">{skills_table_html}</section>

        <script>
            const btnRelevant = document.getElementById('btn-relevant');
            const btnNotRelevant = document.getElementById('btn-not-relevant');
            const btnApplied = document.getElementById('btn-applied');
            const btnSkills = document.getElementById('btn-skills');
            const panelRelevant = document.getElementById('panel-relevant');
            const panelNotRelevant = document.getElementById('panel-not-relevant');
            const panelApplied = document.getElementById('panel-applied');
            const panelSkills = document.getElementById('panel-skills');

            function apiUrl(path) {{
                if (window.location.protocol === 'file:') {{
                    return `http://127.0.0.1:8765${{path}}`;
                }}
                return path;
            }}

            function ensureEmptyState(panel) {{
                const hasCards = panel.querySelector('.card') !== null;
                let emptyEl = panel.querySelector('.empty');
                if (!hasCards && !emptyEl) {{
                    emptyEl = document.createElement('p');
                    emptyEl.className = 'empty';
                    emptyEl.textContent = 'No records found.';
                    panel.appendChild(emptyEl);
                }}
                if (hasCards && emptyEl) {{
                    emptyEl.remove();
                }}
            }}

            function refreshCounts() {{
                const relevantCount = panelRelevant.querySelectorAll('.card').length;
                const notRelevantCount = panelNotRelevant.querySelectorAll('.card').length;
                const appliedCount = panelApplied.querySelectorAll('.card').length;
                const skillsCount = panelSkills.querySelectorAll('tbody tr').length;
                btnRelevant.textContent = `Relevant (${{relevantCount}})`;
                btnNotRelevant.textContent = `Not relevant (${{notRelevantCount}})`;
                btnApplied.textContent = `Applied (${{appliedCount}})`;
                btnSkills.textContent = `Skills (${{skillsCount}})`;
                ensureEmptyState(panelRelevant);
                ensureEmptyState(panelNotRelevant);
                ensureEmptyState(panelApplied);
            }}

            function setMode(mode) {{
                const isRelevant = mode === 'relevant';
                const isNotRelevant = mode === 'not relevant';
                const isApplied = mode === 'applied';
                const isSkills = mode === 'skills';
                panelRelevant.classList.toggle('hidden', !isRelevant);
                panelNotRelevant.classList.toggle('hidden', !isNotRelevant);
                panelApplied.classList.toggle('hidden', !isApplied);
                panelSkills.classList.toggle('hidden', !isSkills);
                btnRelevant.classList.toggle('active', isRelevant);
                btnNotRelevant.classList.toggle('active', isNotRelevant);
                btnApplied.classList.toggle('active', isApplied);
                btnSkills.classList.toggle('active', isSkills);
            }}

            function focusSkillRow(skillKey) {{
                if (!skillKey) return;
                const row = panelSkills.querySelector(`tr[data-skill-key="${{CSS.escape(skillKey)}}"]`);
                if (!row) return;
                panelSkills.querySelectorAll('tbody tr.skill-row-focus').forEach((item) => item.classList.remove('skill-row-focus'));
                row.classList.add('skill-row-focus');
                row.setAttribute('tabindex', '-1');
                row.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
                window.setTimeout(() => row.focus({{ preventScroll: true }}), 120);
                window.setTimeout(() => row.classList.remove('skill-row-focus'), 2200);
            }}

            function openSkillsForSkill(skillKey) {{
                setMode('skills');
                focusSkillRow(skillKey);
            }}

            btnRelevant.addEventListener('click', () => setMode('relevant'));
            btnNotRelevant.addEventListener('click', () => setMode('not relevant'));
            btnApplied.addEventListener('click', () => setMode('applied'));
            btnSkills.addEventListener('click', () => setMode('skills'));

            async function setUserSkill(skillKey, hasSkill, inputEl) {{
                inputEl.disabled = true;
                try {{
                    const response = await fetch(apiUrl('/api/skill/user'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ skill: skillKey, has_skill: hasSkill }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                }} catch (err) {{
                    inputEl.checked = !hasSkill;
                    alert(`Failed to update skill: ${{err.message}}`);
                }} finally {{
                    inputEl.disabled = false;
                }}
            }}

            async function setLearnSkill(skillKey, learn, inputEl) {{
                inputEl.disabled = true;
                try {{
                    const response = await fetch(apiUrl('/api/skill/learn'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ skill: skillKey, learn }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                }} catch (err) {{
                    inputEl.checked = !learn;
                    alert(`Failed to update skill: ${{err.message}}`);
                }} finally {{
                    inputEl.disabled = false;
                }}
            }}

            async function deleteSkill(skillKey, btnEl) {{
                if (!confirm(`Delete skill '${{skillKey}}' from profile and DB?`)) return;
                btnEl.disabled = true;
                try {{
                    const response = await fetch(apiUrl('/api/skill/delete'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ skill: skillKey }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                    const row = btnEl.closest('tr');
                    if (row) row.remove();
                    refreshCounts();
                }} catch (err) {{
                    alert(`Failed to delete skill: ${{err.message}}`);
                    btnEl.disabled = false;
                }}
            }}

            async function blockSkill(skillKey, btnEl) {{
                if (!confirm(`Block skill '${{skillKey}}' and hide it from all positions and the skills tab?`)) return;
                btnEl.disabled = true;
                try {{
                    const response = await fetch(apiUrl('/api/skill/block'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ skill: skillKey }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                    const row = btnEl.closest('tr');
                    if (row) row.remove();
                    refreshCounts();
                }} catch (err) {{
                    alert(`Failed to block skill: ${{err.message}}`);
                    btnEl.disabled = false;
                }}
            }}

            async function setRelevant(jobId, isRelevant, inputEl) {{
                const signal = isRelevant ? 'relevant' : 'not relevant';
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {{
                    const response = await fetch(apiUrl('/api/feedback'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ job_id: jobId, signal }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                    if (statusEl) statusEl.textContent = `Saved: ${{signal}}`;
                    if (signal === 'not relevant') {{
                        const appliedCheckbox = card ? card.querySelector('.applied-wrap input') : null;
                        if (appliedCheckbox) appliedCheckbox.checked = false;
                    }}
                    const viewedCheckbox = card ? card.querySelector('.viewed-wrap input') : null;
                    let isViewed = viewedCheckbox ? viewedCheckbox.checked : false;
                    if (signal === 'not relevant' && viewedCheckbox && !isViewed) {{
                        const viewedResponse = await fetch(apiUrl('/api/viewed'), {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{ job_id: jobId, viewed: true }})
                        }});
                        const viewedData = await viewedResponse.json();
                        if (!viewedResponse.ok || !viewedData.ok) {{
                            throw new Error(viewedData.error || 'Failed to update viewed');
                        }}
                        viewedCheckbox.checked = true;
                        isViewed = true;
                    }}
                    const targetPanel = signal === 'relevant' ? panelRelevant : panelNotRelevant;
                    if (card && !isViewed && card.parentElement !== targetPanel) {{
                        targetPanel.prepend(card);
                    }} else if (card && isViewed) {{
                        card.remove();
                    }}
                    refreshCounts();
                }} catch (err) {{
                    if (statusEl) statusEl.textContent = `Error: ${{err.message}}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !isRelevant;
                }}
            }}

            async function setViewed(jobId, viewed, inputEl) {{
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {{
                    const response = await fetch(apiUrl('/api/viewed'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ job_id: jobId, viewed }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                    if (viewed) {{
                        if (card && card.parentElement !== panelApplied) card.remove();
                        refreshCounts();
                    }} else if (statusEl) {{
                        const appliedCheckbox = card ? card.querySelector('.applied-wrap input') : null;
                        if (appliedCheckbox) appliedCheckbox.checked = false;
                        if (card && card.parentElement === panelApplied) card.remove();
                        statusEl.textContent = 'Saved: unviewed';
                    }}
                }} catch (err) {{
                    if (statusEl) statusEl.textContent = `Error: ${{err.message}}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !viewed;
                }}
            }}

            async function setApplied(jobId, applied, inputEl) {{
                const card = inputEl.closest('.card');
                const statusEl = card ? card.querySelector('.feedback-status') : null;
                if (statusEl) statusEl.textContent = 'Saving...';
                try {{
                    const response = await fetch(apiUrl('/api/applied'), {{
                        method: 'POST',
                        headers: {{ 'Content-Type': 'application/json' }},
                        body: JSON.stringify({{ job_id: jobId, applied }})
                    }});
                    const data = await response.json();
                    if (!response.ok || !data.ok) {{
                        throw new Error(data.error || 'Request failed');
                    }}
                    if (card) {{
                        const relevantCheckbox = card.querySelector('.relevant-wrap input');
                        const viewedCheckbox = card.querySelector('.viewed-wrap input');
                        if (applied) {{
                            if (relevantCheckbox) relevantCheckbox.checked = true;
                            if (viewedCheckbox) viewedCheckbox.checked = true;
                            if (card.parentElement !== panelApplied) panelApplied.prepend(card);
                        }} else if (card.parentElement === panelApplied) {{
                            card.remove();
                        }}
                    }}
                    if (statusEl) statusEl.textContent = `Saved: ${{applied ? 'applied' : 'not applied'}}`;
                    refreshCounts();
                }} catch (err) {{
                    if (statusEl) statusEl.textContent = `Error: ${{err.message}}. Start: python -m spejder.cli serve-gui`;
                    inputEl.checked = !applied;
                }}
            }}

            window.setRelevant = setRelevant;
            window.setViewed = setViewed;
            window.setApplied = setApplied;
            window.setUserSkill = setUserSkill;
            window.setLearnSkill = setLearnSkill;
            window.openSkillsForSkill = openSkillsForSkill;
            window.blockSkill = blockSkill;
            window.deleteSkill = deleteSkill;
            refreshCounts();
        </script>
    </body>
</html>
"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    print(
        f"Wrote HTML dashboard: {out_html} "
        f"(relevant={len(relevant_items)}, not_relevant={len(not_relevant_items)}, applied={len(applied_items)}, viewed={int(viewed_total)})"
    )


def cmd_report_links(args):
    docs = email_parser.load_files(args.folder)
    link_counts = {}
    for d in docs:
        for lnk in d.get("links", []):
            link_counts[lnk] = link_counts.get(lnk, 0) + 1
    items = sorted(link_counts.items(), key=lambda x: x[1], reverse=True)
    for url, cnt in items[:200]:
        print(f"{cnt}\t{url}")


def cmd_summarize_file(args):
    if not os.path.exists(args.path):
        print("File not found:", args.path)
        return
    doc = email_parser.parse_html_file(args.path)
    llm = LocalLLM(model_path=args.model, verbose=bool(args.verbose_model))
    try:
        summary = llm.summarize(doc.get("text", ""), max_tokens=args.max_tokens)
        print("--- Summary ---")
        print(summary)
    except Exception as exc:
        print("LLM error:", exc)


def cmd_summarize_folder(args):
    docs = email_parser.load_files(args.folder)
    if not docs:
        print("No documents found in folder:", args.folder)
        return

    llm = LocalLLM(model_path=args.model, verbose=bool(args.verbose_model))
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    processed = 0
    failed = 0
    max_docs = args.limit if args.limit and args.limit > 0 else len(docs)

    with open(args.out, "w", encoding="utf-8") if args.out else nullcontext() as out_handle:
        for doc in docs[:max_docs]:
            path = doc.get("path")
            try:
                summary = llm.summarize(doc.get("text", ""), max_tokens=args.max_tokens)
                record = {
                    "path": path,
                    "summary": summary,
                    "links": doc.get("links", []),
                }
                if out_handle:
                    out_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                print(f"[OK] {path}")
                print(summary)
                print()
                processed += 1
            except Exception as exc:
                failed += 1
                print(f"[ERR] {path}: {exc}")

    print(f"Done. Processed={processed}, Failed={failed}, Total={max_docs}")


def _extract_position_page_text(
    position_link: str, max_chars: int = 3000, timeout_sec: int = 8
) -> str:
    link = (position_link or "").strip()
    if not link.startswith(("http://", "https://")):
        return ""

    req = Request(
        link,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        },
    )

    try:
        with urlopen(req, timeout=timeout_sec) as response:
            ctype = (response.headers.get("Content-Type") or "").lower()
            if ctype and "html" not in ctype and "text" not in ctype:
                return ""

            payload = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            html_text = payload.decode(charset, errors="ignore")
    except (HTTPError, URLError, TimeoutError, ValueError):
        return ""
    except Exception:
        return ""

    soup = BeautifulSoup(html_text, "html.parser")
    for node in soup(["script", "style", "noscript"]):
        node.decompose()

    text = " ".join(soup.get_text(" ", strip=True).split())
    if not text:
        return ""
    return text[:max_chars]


def _get_position_page_context(
    position_link: str, page_context_cache: Optional[dict] = None
) -> str:
    link = (position_link or "").strip()
    if not link:
        return ""
    if page_context_cache is not None and link in page_context_cache:
        return page_context_cache.get(link, "") or ""

    page_context = _extract_position_page_text(link)
    if page_context_cache is not None:
        page_context_cache[link] = page_context
    return page_context


def _append_page_context_to_raw_text(
    raw_text: str, position_link: str, page_context: str, max_chars: int = 9000
) -> str:
    base_raw = (raw_text or "").strip()
    link = (position_link or "").strip()
    context = (page_context or "").strip()
    if not context:
        return base_raw
    if not base_raw:
        return context[:max_chars]
    if not link:
        return base_raw

    marker = f"[POSITION_PAGE_CONTEXT {link}]"
    if marker in base_raw:
        return base_raw

    merged = f"{base_raw}\n\n{marker}\n{context}".strip()
    return merged[:max_chars]


def _normalize_title_compare_key(text: str) -> str:
    compact = " ".join((text or "").split()).strip().lower()
    compact = re.sub(r"[^a-z0-9]+", "", compact)
    return compact


def _clean_translated_title_output(text: str) -> str:
    cleaned = " ".join((text or "").replace("```", " ").split()).strip()
    if not cleaned:
        return ""

    cleaned = re.sub(r"^english\s+title\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^translated\s+title(?:\s+text)?\s*:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(" \"'`[]()")

    marker_pattern = re.compile(
        r"\b(?:translated\s+title|translated\s+title\s+text|original\s+title|original\s+text|english\s+title|english\s+translation|translation\s+result|return\s+value|return\s+only|unchanged\s+title|step\s+1|note:|you\s+are\s+an\s+ai\s+assistant|translate\s+this\s+job\s+title\s+to\s+english|this\s+translation|translation\s+conveys|the\s+translated\s+title|is\s+already\s+in\s+english)\b",
        flags=re.IGNORECASE,
    )
    match = marker_pattern.search(cleaned)
    if match:
        cleaned = cleaned[: match.start()].rstrip(" -|:;,/")

    while cleaned.count("(") > cleaned.count(")") and "(" in cleaned:
        cleaned = cleaned.rsplit("(", 1)[0].rstrip(" -|:;,/")

    return cleaned.strip(" \"'`[]()").rstrip(" -|:;,/")[:180]


def _is_plausible_translated_title(candidate: str, original: str) -> bool:
    text = " ".join((candidate or "").split()).strip()
    base = " ".join((original or "").split()).strip()
    if not text:
        return False
    if len(text) > max(80, int(len(base) * 2.2)):
        return False

    low = text.lower()
    bad_fragments = [
        "translation",
        "translated title",
        "original title",
        "you are an ai assistant",
        "return only",
        "step 1",
        "this title",
        "in english",
    ]
    if any(fragment in low for fragment in bad_fragments):
        return False

    words = re.findall(r"[a-zA-Z0-9+#.&/-]+", text)
    if len(words) > 14:
        return False
    if text.count(".") >= 2:
        return False
    return True


def _translate_title_to_english(
    title: str,
    llm: LocalLLM = None,
    title_translation_cache: Optional[dict] = None,
) -> str:
    title_clean = " ".join((title or "").split()).strip()
    if not title_clean:
        return ""

    cache_key = title_clean.lower()
    if title_translation_cache is not None and cache_key in title_translation_cache:
        return str(title_translation_cache.get(cache_key, title_clean) or title_clean)

    result = title_clean
    if llm:
        prompt = (
            "Translate this job title to English. "
            "If it is already English, return it unchanged. "
            "Return only the translated title text, no extra words.\n\n"
            f"Title: {title_clean}\n\n"
            "English title:"
        )
        try:
            out = llm.generate(prompt, max_tokens=64)
            english = _clean_translated_title_output(out)
            if english and _is_plausible_translated_title(english, title_clean):
                same = _normalize_title_compare_key(english) == _normalize_title_compare_key(
                    title_clean
                )
                if same:
                    result = title_clean
                else:
                    result = f"{title_clean} ({english})"
        except Exception:
            result = title_clean

    if title_translation_cache is not None:
        title_translation_cache[cache_key] = result
    return result


def _prepend_title_to_raw_text(
    title: str, raw_text: str, max_chars: int = 9000
) -> str:
    title_clean = " ".join((title or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not title_clean:
        return raw_clean

    prefixed = f"Title: {title_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]


def _prepend_summary_to_raw_text(
    summary: str, raw_text: str, max_chars: int = 9000
) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    raw_clean = (raw_text or "").strip()

    if not summary_clean:
        return raw_clean

    prefixed = f"Summary: {summary_clean}"
    if not raw_clean:
        return prefixed[:max_chars]

    raw_low = raw_clean.lower()
    prefixed_low = prefixed.lower()
    if raw_low.startswith(prefixed_low):
        return raw_clean[:max_chars]

    merged = f"{prefixed}\n\n{raw_clean}"
    return merged[:max_chars]


def _enrich_raw_text_with_position_page(
    db_path: str,
    row: dict,
    page_context_cache: Optional[dict] = None,
    llm: LocalLLM = None,
    title_translation_cache: Optional[dict] = None,
) -> str:
    raw = (row.get("raw_text") or "").strip()
    title_for_prompt = _translate_title_to_english(
        row.get("title", ""),
        llm=llm,
        title_translation_cache=title_translation_cache,
    )
    raw = _prepend_title_to_raw_text(title_for_prompt, raw)
    raw = _prepend_summary_to_raw_text(row.get("summary", ""), raw)
    link = (row.get("position_link") or "").strip()
    if not link:
        return raw

    page_context = _get_position_page_context(link, page_context_cache=page_context_cache)
    merged = _append_page_context_to_raw_text(raw, link, page_context)
    if merged:
        row["raw_text"] = merged
        return merged
    return raw


def _build_description_summary(
    raw_text: str,
    llm: LocalLLM = None,
    position_link: str = "",
    page_context_cache: Optional[dict] = None,
) -> str:
    cleaned = " ".join((raw_text or "").split())

    def clean_model_output(text: str) -> str:
        out = text or ""
        out = out.replace("```", " ")
        if re.search(r"\bto be concise\s*:", out, flags=re.IGNORECASE):
            out = re.split(r"\bto be concise\s*:\s*", out, maxsplit=1, flags=re.IGNORECASE)[-1]
        out = re.sub(r"\bplaintext\b", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\bsummary\s*:\s*", " ", out, flags=re.IGNORECASE)
        out = re.sub(r"\s+", " ", out).strip()
        return out

    def remove_repeated_phrases(text: str) -> str:
        words = text.split()
        if len(words) < 8:
            return text

        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[:size] == words[size : size * 2]:
                words = words[:size] + words[size * 2 :]
                break

        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[-size:] == words[-size * 2 : -size]:
                words = words[:-size]
                break

        return " ".join(words)

    if llm:
        page_context = _get_position_page_context(
            position_link, page_context_cache=page_context_cache
        )
        if not cleaned and not page_context:
            return ""

        page_block = (
            f"Position page context (if useful):\n{page_context}\n\n" if page_context else ""
        )
        prompt = (
            "Summarize this job description in English. "
            "Keep only key responsibilities, key requirements, and main purpose, no general information and common words. "
            "Use the Description as primary truth; use page context only to refine missing details.\n\n"
            "Do not include job title, company name, location, or other metadata in the summary unless they are explicitly explain the position. "
            f"Description:\n{cleaned}\n\n"
            f"{page_block}"
            "Summary:"
        )
        try:
            out = llm.generate(prompt, max_tokens=1024)
            return remove_repeated_phrases(clean_model_output(out))
        except Exception:
            pass

    return ""


def _fallback_description_text(description: str, raw_text: str, max_chars: int = 280) -> str:
    if (description or "").strip():
        return description
    compact = " ".join((raw_text or "").split())
    compact = re.sub(r"\[POSITION_PAGE_CONTEXT[^\]]*\]", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\(\s*settings\s*\)", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bPUBLISHED\s*:\s*\d{1,2}-\d{1,2}-\d{4}\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bRetrieved\s+from\s+Jobcenter\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bCheck\s+the\s+job\s+satisfaction\s*:\s*[^.\n]*", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\b\d{1,3}(?:,\d{3})*\s+ratings\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bSave\s+job\b", " ", compact, flags=re.IGNORECASE)
    compact = re.sub(r"\bView\s+job\b", " ", compact, flags=re.IGNORECASE)
    compact = " ".join(compact.split())
    if not compact:
        return ""

    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars].rstrip() + "..."


def _has_invalid_description_marker(text: str) -> bool:
    return "[POSITION_PAGE_CONTEXT" in (text or "").upper()


def _is_low_quality_description(
    description: str,
    raw_text: str = "",
    title: str = "",
) -> bool:
    desc = " ".join((description or "").split()).strip()
    if not desc:
        return False

    low = desc.lower()
    bad_markers = [
        "you are an ai assistant",
        "translated title",
        "original title",
        "english title",
        "return only",
        "step 1",
    ]
    if any(marker in low for marker in bad_markers):
        return True

    # Word spam / repeated fragments are usually model failure artifacts.
    if re.search(r"\b(\w+)(?:\s+\1){4,}\b", low):
        return True

    words = re.findall(r"[a-zA-Z0-9+#.-]+", low)
    if len(words) >= 45:
        unique_ratio = len(set(words)) / max(1, len(words))
        if unique_ratio < 0.42:
            return True

    raw_clean = " ".join((raw_text or "").split()).strip()
    if raw_clean and len(raw_clean) < 240 and len(desc) > 360:
        return True

    title_tokens = [
        token
        for token in re.findall(r"[a-zA-Z0-9+#.-]+", (title or "").lower())
        if len(token) > 2
    ]
    if title_tokens and len(words) >= 30:
        repeats = sum(low.count(token) for token in set(title_tokens))
        if repeats > max(6, len(words) // 4):
            return True

    return False


def _generate_missing_descriptions_for_ingest(
    db_path: str,
    llm: LocalLLM = None,
    allow_empty: bool = False,
    progress: bool = False,
    progress_label: str = "Description generation",
) -> tuple[int, int]:
    rows = get_jobs_for_description_refresh(
        db_path,
        category="",
        source="",
        links=[],
        job_ids=[],
        limit=0,
        missing_only=True,
        unviewed_only=True,
    )

    updated = 0
    skipped = 0
    total_rows = len(rows)
    started_at = time.monotonic()

    def _fmt_eta(seconds: float) -> str:
        seconds = max(0, int(seconds))
        mins, secs = divmod(seconds, 60)
        hrs, mins = divmod(mins, 60)
        if hrs > 0:
            return f"{hrs}h {mins}m {secs}s"
        if mins > 0:
            return f"{mins}m {secs}s"
        return f"{secs}s"

    if progress:
        print(f"{progress_label}: starting ({total_rows} items)")

    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    for idx, row in enumerate(rows, start=1):
        if progress:
            elapsed = time.monotonic() - started_at
            avg_per_item = elapsed / max(1, idx - 1)
            remaining = max(0, total_rows - idx + 1)
            eta_sec = avg_per_item * remaining if idx > 1 else 0
            print(
                f"{progress_label}: {idx}/{total_rows} "
                f"(updated={updated}, skipped={skipped}, elapsed={_fmt_eta(elapsed)}, eta={_fmt_eta(eta_sec)})"
            )

        source_raw = row.get("raw_text", "") or ""
        raw = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            title_translation_cache=title_translation_cache,
        )
        if not raw:
            skipped += 1
            continue

        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
        )
        if (
            not description
            or _has_invalid_description_marker(description)
            or _is_low_quality_description(
                description,
                raw_text=raw,
                title=row.get("title", ""),
            )
        ):
            description = _fallback_description_text("", source_raw or raw)
        if not description and not allow_empty:
            skipped += 1
            continue

        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    if progress:
        total_elapsed = time.monotonic() - started_at
        print(
            f"{progress_label}: done (updated={updated}, skipped={skipped}, elapsed={_fmt_eta(total_elapsed)})"
        )

    return updated, skipped


def cmd_process_inbox(args):
    profile_path = args.profile or DEFAULT_PROFILE_PATH
    profile = _load_runtime_profile(profile_path)
    inbox = args.inbox or profile.get("default_inbox") or "./inbox"
    db_path = args.db or profile.get("default_db") or "./jobs.db"
    report_dir = args.report_dir or profile.get("default_report_dir") or "./outbox"
    model_path = args.model or profile.get("default_model") or ""
    max_input_chars = (
        args.max_input_chars
        if args.max_input_chars is not None
        else int(profile.get("max_input_chars", 4500) or 4500)
    )

    docs = email_parser.load_files(inbox)
    if not docs:
        print("No documents found in inbox:", inbox)
        return

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)
    ingest_stats = ingest_docs_to_db(db_path, docs)
    print(
        "Ingestion done: "
        f"processed={ingest_stats.get('processed', 0)}, "
        f"inserted_new={ingest_stats.get('inserted_new', 0)}, "
        f"skipped_existing={ingest_stats.get('skipped_existing', 0)} "
        f"into DB: {db_path}"
    )

    total, relevant_count = apply_relevance(
        db_path, profile, prune_irrelevant=args.prune_irrelevant
    )
    print(f"Scored {total} positions; relevant={relevant_count}")

    relevant_jobs = get_relevant_jobs(db_path, limit=args.limit)
    llm = LocalLLM(model_path=model_path, verbose=bool(args.verbose)) if model_path else None

    desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
        db_path, llm=llm, allow_empty=False
    )
    print(f"Descriptions generated during ingest: updated={desc_updated}, skipped={desc_skipped}")

    skill_learning = _learn_skill_patterns_from_positions(
        db_path,
        runtime_profile=profile,
        llm=llm,
        progress=True,
        progress_label="Skill pattern learning",
    )
    print(
        "Skill pattern learning: "
        f"considered={skill_learning.get('considered_positions', 0)}, "
        f"new_patterns={skill_learning.get('new_skill_patterns', 0)}, "
        f"total_patterns={skill_learning.get('total_known_skill_patterns', 0)}"
    )

    learning_info = update_profile_from_db_signals(db_path, profile_path)
    print(
        "Profile learning: "
        f"labeled={learning_info.get('labeled_count', 0)}, "
        f"include={learning_info.get('learned_include_count', 0)}, "
        f"exclude={learning_info.get('learned_exclude_count', 0)}, "
        f"missing_skills={learning_info.get('missing_skills_count', 0)}"
    )

    for job in relevant_jobs:
        company = job.get("company", "")
        title = job.get("title", "")
        link = job.get("position_link", "")
        text = job.get("raw_text", "")

        compact = re.sub(r"https?://\S+", "[link]", text)
        compact = re.sub(r"\s+", " ", compact).strip()
        text_for_llm = (
            compact[:max_input_chars] if max_input_chars and max_input_chars > 0 else compact
        )

        if llm:
            prompt = (
                "Summarize this job posting in 4 bullets: role focus, key requirements, "
                "location/remote info, and why it may fit the candidate.\n\n"
                f"Company: {company}\nTitle: {title}\nLink: {link}\n\n"
                f"Content:\n{text_for_llm}\n\nSummary:"
            )
            try:
                summary = llm.generate(prompt, max_tokens=args.max_tokens)
            except Exception as exc:
                summary = f"LLM summary failed: {exc}"
        else:
            snippet = " ".join(text.split())[:260]
            summary = f"{title} at {company}. Link: {link}. Snippet: {snippet}"

        set_job_summary(db_path, job["id"], summary)

        print(f"[{job.get('relevance_score', 0):.1f}] {title} @ {company}")
        print(f"    {link}")
        print(f"    {summary[:300]}")
        print()

    os.makedirs(report_dir, exist_ok=True)

    for legacy_name in [
        "other.html",
        "unrelevant.html",
        "relevant.html",
        "not_relevant.html",
    ]:
        legacy_path = os.path.join(report_dir, legacy_name)
        if os.path.exists(legacy_path):
            os.remove(legacy_path)

    report_data = {}
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    for cat in ["relevant", "not relevant"]:
        rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
        records = []
        for row in rows:
            summary = row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260]
            description = row.get("description") or ""
            source_raw = row.get("raw_text", "") or ""
            raw_text = _enrich_raw_text_with_position_page(
                db_path,
                row,
                page_context_cache=page_context_cache,
                llm=llm,
                title_translation_cache=title_translation_cache,
            )
            skills = _get_or_extract_job_skills(
                db_path,
                row.get("id", 0),
                raw_text,
                llm=llm,
                profile=profile,
                position_link=row.get("position_link", ""),
                page_context_cache=page_context_cache,
                limit=10,
            )
            if not description:
                generated = _build_description_summary(
                    raw_text,
                    llm=llm,
                    position_link=row.get("position_link", ""),
                    page_context_cache=page_context_cache,
                )
                if (
                    generated
                    and not _has_invalid_description_marker(generated)
                    and not _is_low_quality_description(
                        generated,
                        raw_text=raw_text,
                        title=row.get("title", ""),
                    )
                ):
                    description = generated
                    set_job_description(db_path, row.get("id", 0), description)
            if not description:
                description = _fallback_description_text("", source_raw or raw_text)
                if description:
                    set_job_description(db_path, row.get("id", 0), description)
            records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    "title": _translate_title_to_english(
                        row.get("title", ""),
                        llm=llm,
                        title_translation_cache=title_translation_cache,
                    ),
                    "place": row.get("place", ""),
                    "work_type": row.get("work_type", "Unknown"),
                    "description": description,
                    "skills": skills,
                    "position_link": row.get("position_link", ""),
                    "relevance_score": row.get("relevance_score", 0),
                    "summary": summary,
                    "category": cat,
                    "viewed": row.get("viewed", 0),
                    "applied": row.get("applied", 0),
                }
            )
        report_data[cat] = records

    applied_rows = get_applied_jobs(db_path, limit=0)
    applied_records = []
    for row in applied_rows:
        summary = row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260]
        description = row.get("description") or ""
        raw_text = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            title_translation_cache=title_translation_cache,
        )
        skills = _get_or_extract_job_skills(
            db_path,
            row.get("id", 0),
            raw_text,
            llm=llm,
            profile=profile,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
            limit=10,
        )
        applied_records.append(
            {
                "id": row.get("id", 0),
                "source": row.get("source", "Unknown"),
                "company": row.get("company", ""),
                "title": _translate_title_to_english(
                    row.get("title", ""),
                    llm=llm,
                    title_translation_cache=title_translation_cache,
                ),
                "place": row.get("place", ""),
                "work_type": row.get("work_type", "Unknown"),
                "description": description,
                "skills": skills,
                "position_link": row.get("position_link", ""),
                "relevance_score": row.get("relevance_score", 0),
                "summary": summary,
                "category": row.get("category", "relevant"),
                "viewed": row.get("viewed", 1),
                "applied": row.get("applied", 1),
            }
        )

    dashboard_path = os.path.join(report_dir, "report.html")
    _render_html_dashboard(
        report_data.get("relevant", []),
        report_data.get("not relevant", []),
        applied_records,
        dashboard_path,
        "Positions Report",
        viewed_total=get_viewed_jobs_count(db_path),
        skills_items=_build_skills_tab_items(db_path, profile),
    )
    print(f"Report written: {dashboard_path}")

    if not relevant_jobs:
        print("No relevant positions after filtering.")

    print(f"Done. Relevant summarized={len(relevant_jobs)}")


def cmd_serve_gui(args):
    cli_verbose = bool(getattr(args, "verbose", False))
    profile_path = os.path.abspath(args.profile or DEFAULT_PROFILE_PATH)
    runtime_profile = _load_runtime_profile(profile_path)
    report_dir = os.path.abspath(
        args.report_dir or runtime_profile.get("default_report_dir") or "./outbox"
    )
    db_path = os.path.abspath(args.db or runtime_profile.get("default_db") or "./jobs.db")
    inbox_path = os.path.abspath(runtime_profile.get("default_inbox") or "./inbox")
    model_path = runtime_profile.get("default_model") or ""
    host = args.host or runtime_profile.get("server_host") or "127.0.0.1"
    port = args.port if args.port is not None else int(runtime_profile.get("server_port") or 8765)

    print(f"Serve GUI: initializing (profile={profile_path})")
    print(f"Serve GUI: db={db_path}, report_dir={report_dir}, inbox={inbox_path}")
    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)
    print("Serve GUI: database ready")
    os.makedirs(report_dir, exist_ok=True)
    dashboard_path = os.path.join(report_dir, "report.html")
    dashboard_lock = threading.Lock()
    title_translation_cache: dict[str, str] = {}
    title_translation_llm: Optional[LocalLLM] = None

    def _get_title_translation_llm() -> Optional[LocalLLM]:
        nonlocal title_translation_llm
        if title_translation_llm is not None:
            return title_translation_llm
        if not model_path:
            return None
        title_translation_llm = LocalLLM(model_path=model_path, verbose=False)
        return title_translation_llm

    def _persist_runtime_profile() -> None:
        _save_profile(profile_path, runtime_profile)

    def _reload_runtime_profile() -> None:
        fresh = _load_runtime_profile(profile_path)
        runtime_profile.clear()
        runtime_profile.update(fresh)

    def _regain_skills_for_unviewed_positions(llm_for_skills: LocalLLM = None) -> int:
        affected = clear_job_skills_for_unviewed_jobs(db_path)

        rows = []
        seen = set()
        for cat in ["relevant", "not relevant"]:
            for row in get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True):
                row_id = int(row.get("id", 0) or 0)
                if not row_id or row_id in seen:
                    continue
                seen.add(row_id)
                rows.append(row)

        updated = _populate_missing_dashboard_skills(
            rows,
            llm=llm_for_skills,
            progress_label="Skill rebuild: unviewed",
        )
        return max(int(affected), int(updated))

    def _build_dashboard_record(
        row: dict,
        default_category: str,
        default_viewed: int = 0,
        default_applied: int = 0,
    ) -> dict:
        summary = row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260]
        cached_skills = get_job_skills(db_path, int(row.get("id", 0) or 0))
        return {
            "id": row.get("id", 0),
            "source": row.get("source", "Unknown"),
            "company": row.get("company", ""),
            "title": _translate_title_to_english(
                row.get("title", ""),
                llm=_get_title_translation_llm(),
                title_translation_cache=title_translation_cache,
            ),
            "place": row.get("place", ""),
            "work_type": row.get("work_type", "Unknown"),
            "description": _fallback_description_text(
                row.get("description") or "", row.get("raw_text") or ""
            ),
            "skills": _format_skills(cached_skills, limit=10),
            "position_link": row.get("position_link", ""),
            "relevance_score": row.get("relevance_score", 0),
            "summary": summary,
            "category": row.get("category", default_category),
            "viewed": row.get("viewed", default_viewed),
            "applied": row.get("applied", default_applied),
        }

    def _populate_missing_dashboard_skills(
        rows: list[dict], llm: LocalLLM = None, progress_label: str = ""
    ) -> int:
        if not rows:
            return 0

        page_context_cache: dict[str, str] = {}
        title_translation_cache: dict[str, str] = {}
        updated = 0
        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            job_id = int(row.get("id", 0) or 0)
            if not job_id or get_job_skills(db_path, job_id):
                continue

            raw_text = _enrich_raw_text_with_position_page(
                db_path,
                row,
                page_context_cache=page_context_cache,
                llm=llm,
                title_translation_cache=title_translation_cache,
            )
            skills = _get_or_extract_job_skills(
                db_path,
                job_id,
                raw_text,
                llm=llm,
                profile=runtime_profile,
                position_link=row.get("position_link", ""),
                page_context_cache=page_context_cache,
                limit=10,
            )
            if skills:
                updated += 1
            if progress_label and (idx % 25 == 0 or idx == total):
                print(f"{progress_label}: checked={idx}/{total}, updated={updated}")
        return updated

    def _rebuild_dashboard(reason: str = ""):
        quiet_reason_prefixes = ("applied ", "viewed ")
        should_log_rebuild = bool(reason) and not any(
            reason.startswith(prefix) for prefix in quiet_reason_prefixes
        )

        if should_log_rebuild:
            print(f"Dashboard rebuild: started ({reason})")
        for attempt in range(3):
            try:
                with dashboard_lock:
                    refreshed_report_data = {}
                    for cat in ["relevant", "not relevant"]:
                        rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
                        if reason == "startup snapshot":
                            print(f"Dashboard rebuild: collecting {cat} ({len(rows)} rows)")
                        refreshed_report_data[cat] = [
                            _build_dashboard_record(
                                row,
                                default_category=cat,
                                default_viewed=0,
                                default_applied=0,
                            )
                            for row in rows
                        ]

                    refreshed_applied_rows = get_applied_jobs(db_path, limit=0)
                    if reason == "startup snapshot":
                        print(
                            f"Dashboard rebuild: collecting applied ({len(refreshed_applied_rows)} rows)"
                        )
                    refreshed_applied_records = [
                        _build_dashboard_record(
                            row,
                            default_category="relevant",
                            default_viewed=1,
                            default_applied=1,
                        )
                        for row in refreshed_applied_rows
                    ]

                    _render_html_dashboard(
                        refreshed_report_data.get("relevant", []),
                        refreshed_report_data.get("not relevant", []),
                        refreshed_applied_records,
                        dashboard_path,
                        "Positions Report",
                        viewed_total=get_viewed_jobs_count(db_path),
                        skills_items=_build_skills_tab_items(db_path, runtime_profile),
                    )
                if should_log_rebuild and not reason.startswith("new record"):
                    print(f"Dashboard rebuild: done ({reason})")
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"Report regeneration failed ({reason or 'unknown'}): {exc}")
                else:
                    time.sleep(0.2)

    def _sync_inbox_in_background():
        try:
            docs = []
            if os.path.isdir(inbox_path):
                docs = email_parser.load_files(inbox_path)

            missing_descriptions = get_jobs_for_description_refresh(
                db_path, missing_only=True, limit=1
            )
            has_missing_descriptions = bool(missing_descriptions)

            if not docs and not has_missing_descriptions:
                print(f"Background sync: no documents in inbox and no missing descriptions, skipping")
                return

            llm_for_sync = (
                LocalLLM(model_path=model_path, verbose=cli_verbose) if model_path else None
            )
            if docs:
                print(f"Background sync started: files={len(docs)}")
            else:
                print("Background sync started: backfilling missing descriptions/skills")

            new_records = 0
            last_inserted_logged = -1

            def _on_new_record():
                nonlocal new_records
                new_records += 1

            def _on_progress(processed: int, inserted_new: int, skipped_existing: int):
                nonlocal last_inserted_logged
                if inserted_new != last_inserted_logged:
                    print(
                        f"Background sync progress: processed={processed}, inserted={inserted_new}, "
                        f"skipped_existing={skipped_existing}"
                    )
                    last_inserted_logged = inserted_new

            if docs:
                ingest_stats = ingest_docs_to_db(
                    db_path,
                    docs,
                    on_new_record=_on_new_record,
                    on_progress=_on_progress,
                )
            else:
                ingest_stats = {"processed": 0, "inserted_new": 0, "skipped_existing": 0}

            print("Background sync: scoring relevance...")
            total, relevant_count = apply_relevance(
                db_path, runtime_profile, prune_irrelevant=False
            )
            print(f"Background sync: relevance scored (total={total}, relevant={relevant_count})")

            relevant_rows = get_jobs_by_category(db_path, "relevant", limit=0, unviewed_only=True)
            not_relevant_rows = get_jobs_by_category(
                db_path, "not relevant", limit=0, unviewed_only=True
            )
            applied_rows = get_applied_jobs(db_path, limit=0)
            skill_rows = []
            seen_job_ids = set()
            for row in relevant_rows + not_relevant_rows + applied_rows:
                row_id = int(row.get("id", 0) or 0)
                if not row_id or row_id in seen_job_ids:
                    continue
                seen_job_ids.add(row_id)
                skill_rows.append(row)

            skills_updated = _populate_missing_dashboard_skills(
                skill_rows,
                llm=llm_for_sync,
                progress_label="Background sync: skills",
            )
            print(f"Background sync: missing skills populated ({skills_updated} jobs updated)")

            _rebuild_dashboard(reason="relevance re-scored")

            desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(
                db_path,
                llm=llm_for_sync,
                allow_empty=False,
                progress=True,
                progress_label="Background sync: descriptions",
            )
            if desc_updated > 0:
                _rebuild_dashboard(reason=f"descriptions updated {desc_updated}")

            skill_learning = _learn_skill_patterns_from_positions(
                db_path,
                runtime_profile=runtime_profile,
                llm=llm_for_sync,
                progress=True,
                progress_label="Background sync: skill patterns",
            )
            if skill_learning.get("new_skill_patterns", 0) > 0:
                _rebuild_dashboard(
                    reason=f"skill patterns learned {skill_learning.get('new_skill_patterns', 0)}"
                )
            print(
                "Background sync: skill pattern learning "
                f"(considered={skill_learning.get('considered_positions', 0)}, "
                f"new={skill_learning.get('new_skill_patterns', 0)}, "
                f"total={skill_learning.get('total_known_skill_patterns', 0)})"
            )

            print(
                f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
                f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
                f"total_jobs={total}, relevant={relevant_count}"
            )
        except Exception as exc:
            print(f"Background sync failed: {exc}")

    sync_thread = threading.Thread(
        target=_sync_inbox_in_background, name="spejder-inbox-sync", daemon=True
    )

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *handler_args, **handler_kwargs):
            super().__init__(*handler_args, directory=report_dir, **handler_kwargs)

        def log_message(self, format, *log_args):
            if cli_verbose:
                super().log_message(format, *log_args)
                return
            return

        def _write_json(self, status_code: int, payload: dict):
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_OPTIONS(self):
            self.send_response(204)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            path = self.path.rstrip("/")
            if path not in {
                "/api/feedback",
                "/api/viewed",
                "/api/applied",
                "/api/skill/user",
                "/api/skill/learn",
                "/api/skill/block",
                "/api/skill/delete",
            }:
                self._write_json(404, {"ok": False, "error": "Not found"})
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                payload = json.loads(body.decode("utf-8"))

                if path in {"/api/feedback", "/api/viewed", "/api/applied"}:
                    job_id = int(payload.get("job_id"))

                if path == "/api/feedback":
                    signal = str(payload.get("signal", "")).strip().lower()
                    if signal not in {"relevant", "not relevant"}:
                        self._write_json(
                            400,
                            {
                                "ok": False,
                                "error": "signal must be 'relevant' or 'not relevant'",
                            },
                        )
                        return
                    updated = set_job_feedback(db_path, job_id, signal)
                    learning_info = update_profile_from_db_signals(db_path, profile_path)
                elif path == "/api/applied":
                    applied_raw = payload.get("applied")
                    if not isinstance(applied_raw, bool):
                        self._write_json(400, {"ok": False, "error": "applied must be boolean"})
                        return
                    updated = set_job_applied(db_path, job_id, applied_raw)
                    learning_info = (
                        update_profile_from_db_signals(db_path, profile_path)
                        if applied_raw
                        else None
                    )
                elif path == "/api/viewed":
                    viewed_raw = payload.get("viewed")
                    if not isinstance(viewed_raw, bool):
                        self._write_json(400, {"ok": False, "error": "viewed must be boolean"})
                        return
                    updated = set_job_viewed(db_path, job_id, viewed_raw)
                elif path == "/api/skill/user":
                    skill = _normalize_skill_name(str(payload.get("skill", "")))
                    has_skill = payload.get("has_skill")
                    if not skill:
                        self._write_json(400, {"ok": False, "error": "skill is required"})
                        return
                    if not isinstance(has_skill, bool):
                        self._write_json(
                            400,
                            {"ok": False, "error": "has_skill must be boolean"},
                        )
                        return
                    changed = _toggle_profile_skill(runtime_profile, "user_skills", skill, has_skill)
                    if changed:
                        _persist_runtime_profile()
                        _reload_runtime_profile()
                        _rebuild_dashboard(
                            reason=f"skill have {'on' if has_skill else 'off'} {skill}"
                        )
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "skill": skill,
                            "has_skill": has_skill,
                            "changed": bool(changed),
                        },
                    )
                    return
                elif path == "/api/skill/learn":
                    skill = _normalize_skill_name(str(payload.get("skill", "")))
                    learn = payload.get("learn")
                    if not skill:
                        self._write_json(400, {"ok": False, "error": "skill is required"})
                        return
                    if not isinstance(learn, bool):
                        self._write_json(400, {"ok": False, "error": "learn must be boolean"})
                        return
                    changed = _toggle_profile_skill(
                        runtime_profile,
                        "missing_skills_suggestions",
                        skill,
                        learn,
                    )
                    if changed:
                        _persist_runtime_profile()
                        _reload_runtime_profile()
                        _rebuild_dashboard(
                            reason=f"skill learn {'on' if learn else 'off'} {skill}"
                        )
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "skill": skill,
                            "learn": learn,
                            "changed": bool(changed),
                        },
                    )
                    return
                elif path == "/api/skill/delete":
                    skill = _normalize_skill_name(str(payload.get("skill", "")))
                    if not skill:
                        self._write_json(400, {"ok": False, "error": "skill is required"})
                        return

                    profile_removed = _remove_skill_from_profile(runtime_profile, skill)
                    _persist_runtime_profile()
                    _reload_runtime_profile()

                    db_deleted = delete_skill_from_db(db_path, skill)
                    _rebuild_dashboard(reason=f"skill deleted cleanup {skill}")
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "skill": skill,
                            "profile_removed": profile_removed,
                            "db_deleted": db_deleted,
                        },
                    )
                    return
                elif path == "/api/skill/block":
                    skill = _normalize_skill_name(str(payload.get("skill", "")))
                    if not skill:
                        self._write_json(400, {"ok": False, "error": "skill is required"})
                        return

                    profile_blocked = _block_skill_in_profile(runtime_profile, skill)
                    _persist_runtime_profile()
                    _reload_runtime_profile()

                    db_deleted = delete_skill_from_db(db_path, skill)
                    _rebuild_dashboard(reason=f"skill blocked cleanup {skill}")
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "skill": skill,
                            "profile_blocked": profile_blocked,
                            "db_deleted": db_deleted,
                        },
                    )
                    return

                if not updated:
                    self._write_json(404, {"ok": False, "error": "job id not found"})
                    return

                if path == "/api/feedback":
                    rebuild_reason = f"feedback {signal} job {job_id}"
                elif path == "/api/applied":
                    rebuild_reason = f"applied {'on' if applied_raw else 'off'} job {job_id}"
                else:
                    rebuild_reason = f"viewed {'on' if viewed_raw else 'off'} job {job_id}"

                _rebuild_dashboard(reason=rebuild_reason)

                if path == "/api/feedback":
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "job_id": job_id,
                            "signal": signal,
                            "profile_learning": learning_info,
                        },
                    )
                elif path == "/api/applied":
                    self._write_json(
                        200,
                        {
                            "ok": True,
                            "job_id": job_id,
                            "applied": applied_raw,
                            "profile_learning": learning_info,
                        },
                    )
                else:
                    self._write_json(200, {"ok": True, "job_id": job_id, "viewed": viewed_raw})
            except Exception as exc:
                self._write_json(500, {"ok": False, "error": str(exc)})

    server = None
    selected_port = port
    max_port_attempts = 20
    for port_offset in range(max_port_attempts + 1):
        candidate_port = port + port_offset
        try:
            server = ThreadingHTTPServer((host, candidate_port), Handler)
            selected_port = candidate_port
            break
        except OSError as exc:
            if getattr(exc, "errno", None) == 98 and port_offset < max_port_attempts:
                continue
            if getattr(exc, "errno", None) == 98:
                raise OSError(
                    f"Address in use for all tried ports: {port}-{port + max_port_attempts}"
                ) from exc
            raise

    if selected_port != port:
        print(f"Requested port {port} is busy; using port {selected_port} instead.")

    report_url = f"http://{host}:{selected_port}/report.html"
    print(f"Serving GUI at {report_url}")
    print(f"Feedback API at http://{host}:{selected_port}/api/feedback")
    print(f"Viewed API at http://{host}:{selected_port}/api/viewed")
    print(f"Applied API at http://{host}:{selected_port}/api/applied")
    print(f"Skill API (have): http://{host}:{selected_port}/api/skill/user")
    print(f"Skill API (learn): http://{host}:{selected_port}/api/skill/learn")
    print(f"Skill API (block): http://{host}:{selected_port}/api/skill/block")
    print(f"Skill API (delete): http://{host}:{selected_port}/api/skill/delete")

    if not args.no_open:
        try:
            opened = webbrowser.open(report_url, new=2)
            if opened:
                print(f"Opened in default browser: {report_url}")
            else:
                print(f"Could not auto-open browser. Open manually: {report_url}")
        except Exception as exc:
            print(f"Could not auto-open browser: {exc}. Open manually: {report_url}")

    print("Serve GUI: starting startup tasks in background")
    threading.Thread(
        target=lambda: _rebuild_dashboard(reason="startup snapshot"),
        name="spejder-startup-dashboard",
        daemon=True,
    ).start()
    sync_thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Stopping server...")
    finally:
        server.server_close()


def cmd_init_profile(args):
    if os.path.exists(args.path) and not args.force:
        print("Profile already exists. Use --force to overwrite:", args.path)
        return
    os.makedirs(os.path.dirname(os.path.abspath(args.path)), exist_ok=True)
    with open(args.path, "w", encoding="utf-8") as f:
        json.dump(DEFAULT_PROFILE, f, ensure_ascii=False, indent=2)
    print("Created profile:", args.path)


def cmd_render_html(args):
    if not os.path.exists(args.input):
        print("Input JSONL not found:", args.input)
        return

    items = []
    with open(args.input, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except Exception:
                continue

    _render_html_from_items(items, args.out, args.title)


def cmd_refresh_descriptions(args):
    profile_path = args.profile or DEFAULT_PROFILE_PATH
    runtime_profile = _load_runtime_profile(profile_path)
    db_path = args.db or runtime_profile.get("default_db") or "./jobs.db"
    model_path = args.model or runtime_profile.get("default_model") or ""

    ensure_db(db_path)
    _ensure_skill_pattern_seed_migration(db_path, profile_path)

    llm = LocalLLM(model_path=model_path, verbose=not args.quiet_model) if model_path else None
    if not llm:
        print(
            "No --model provided, summaries will remain empty unless your logic sets fallback text."
        )

    rows = get_jobs_for_description_refresh(
        db_path,
        category=args.category,
        source=args.source,
        links=args.link,
        job_ids=args.job_id,
        limit=args.limit,
        missing_only=not args.overwrite,
    )

    if not rows:
        print("No matching jobs found for description refresh.")
        return

    updated = 0
    skipped = 0
    page_context_cache: dict[str, str] = {}
    title_translation_cache: dict[str, str] = {}
    for row in rows:
        source_raw = row.get("raw_text", "") or ""
        raw = _enrich_raw_text_with_position_page(
            db_path,
            row,
            page_context_cache=page_context_cache,
            llm=llm,
            title_translation_cache=title_translation_cache,
        )
        if not raw:
            skipped += 1
            continue
        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
        )
        if (
            not description
            or _has_invalid_description_marker(description)
            or _is_low_quality_description(
                description,
                raw_text=raw,
                title=row.get("title", ""),
            )
        ):
            description = _fallback_description_text("", source_raw or raw)
        if not description and not args.allow_empty:
            skipped += 1
            continue
        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    print(f"Description refresh done. matched={len(rows)}, updated={updated}, skipped={skipped}")

    skill_learning = _learn_skill_patterns_from_positions(
        db_path,
        runtime_profile=runtime_profile,
        llm=llm,
        progress=True,
        progress_label="Skill pattern learning",
    )
    print(
        "Skill pattern learning: "
        f"considered={skill_learning.get('considered_positions', 0)}, "
        f"new_patterns={skill_learning.get('new_skill_patterns', 0)}, "
        f"total_patterns={skill_learning.get('total_known_skill_patterns', 0)}"
    )

    if args.report_dir:
        os.makedirs(args.report_dir, exist_ok=True)
        report_data = {}
        page_context_cache: dict[str, str] = {}
        title_translation_cache: dict[str, str] = {}
        for cat in ["relevant", "not relevant"]:
            cat_rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
            records = []
            for row in cat_rows:
                raw_text = _enrich_raw_text_with_position_page(
                    db_path,
                    row,
                    page_context_cache=page_context_cache,
                    llm=llm,
                    title_translation_cache=title_translation_cache,
                )
                skills = _get_or_extract_job_skills(
                    db_path,
                    row.get("id", 0),
                    raw_text,
                    llm=llm,
                    profile=runtime_profile,
                    position_link=row.get("position_link", ""),
                    page_context_cache=page_context_cache,
                    limit=10,
                )
                records.append(
                    {
                        "id": row.get("id", 0),
                        "source": row.get("source", "Unknown"),
                        "company": row.get("company", ""),
                        "title": _translate_title_to_english(
                            row.get("title", ""),
                            llm=llm,
                            title_translation_cache=title_translation_cache,
                        ),
                        "place": row.get("place", ""),
                        "work_type": row.get("work_type", "Unknown"),
                        "description": _fallback_description_text(
                            row.get("description") or "",
                            row.get("raw_text") or "",
                        ),
                        "skills": skills,
                        "position_link": row.get("position_link", ""),
                        "relevance_score": row.get("relevance_score", 0),
                        "summary": row.get("summary")
                        or " ".join((row.get("raw_text") or "").split())[:260],
                        "category": cat,
                        "viewed": row.get("viewed", 0),
                        "applied": row.get("applied", 0),
                    }
                )
            report_data[cat] = records

        applied_rows = get_applied_jobs(db_path, limit=0)
        applied_records = []
        for row in applied_rows:
            raw_text = _enrich_raw_text_with_position_page(
                db_path,
                row,
                page_context_cache=page_context_cache,
                llm=llm,
                title_translation_cache=title_translation_cache,
            )
            skills = _get_or_extract_job_skills(
                db_path,
                row.get("id", 0),
                raw_text,
                llm=llm,
                profile=runtime_profile,
                position_link=row.get("position_link", ""),
                page_context_cache=page_context_cache,
                limit=10,
            )
            applied_records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    "title": _translate_title_to_english(
                        row.get("title", ""),
                        llm=llm,
                        title_translation_cache=title_translation_cache,
                    ),
                    "place": row.get("place", ""),
                    "work_type": row.get("work_type", "Unknown"),
                    "description": _fallback_description_text(
                        row.get("description") or "", row.get("raw_text") or ""
                    ),
                    "skills": skills,
                    "position_link": row.get("position_link", ""),
                    "relevance_score": row.get("relevance_score", 0),
                    "summary": row.get("summary")
                    or " ".join((row.get("raw_text") or "").split())[:260],
                    "category": row.get("category", "relevant"),
                    "viewed": row.get("viewed", 1),
                    "applied": row.get("applied", 1),
                }
            )

        dashboard_path = os.path.join(args.report_dir, "report.html")
        _render_html_dashboard(
            report_data.get("relevant", []),
            report_data.get("not relevant", []),
            applied_records,
            dashboard_path,
            "Positions Report",
            viewed_total=get_viewed_jobs_count(db_path),
            skills_items=_build_skills_tab_items(db_path, runtime_profile),
        )
        print(f"Report written: {dashboard_path}")


def main(argv=None):
    p = argparse.ArgumentParser(prog="spejder")
    sub = p.add_subparsers(dest="cmd")

    pr = sub.add_parser("report-links")
    pr.add_argument("folder")
    pr.set_defaults(func=cmd_report_links)

    psm = sub.add_parser("summarize-file")
    psm.add_argument("--path", required=True)
    psm.add_argument("--model", required=True)
    psm.add_argument("--max-tokens", type=int, default=200)
    psm.add_argument("--verbose-model", action="store_true")
    psm.set_defaults(func=cmd_summarize_file)

    psf = sub.add_parser("summarize-folder")
    psf.add_argument("--folder", required=True)
    psf.add_argument("--model", required=True)
    psf.add_argument("--max-tokens", type=int, default=200)
    psf.add_argument("--limit", type=int, default=0)
    psf.add_argument("--out", default="")
    psf.add_argument("--verbose-model", action="store_true")
    psf.set_defaults(func=cmd_summarize_folder)

    pip = sub.add_parser("process-inbox")
    pip.add_argument("--inbox", default=None)
    pip.add_argument("--db", default=None)
    pip.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    pip.add_argument("--model", default="")
    pip.add_argument("--report-dir", default=None)
    pip.add_argument("--limit", type=int, default=0)
    pip.add_argument("--max-tokens", type=int, default=220)
    pip.add_argument("--max-input-chars", type=int, default=None)
    pip.add_argument("--prune-irrelevant", action="store_true")
    pip.add_argument("--verbose", action="store_true")
    pip.set_defaults(func=cmd_process_inbox)

    pprof = sub.add_parser("init-profile")
    pprof.add_argument("--path", default=DEFAULT_PROFILE_PATH)
    pprof.add_argument("--force", action="store_true")
    pprof.set_defaults(func=cmd_init_profile)

    phr = sub.add_parser("render-html")
    phr.add_argument("--input", default="./outbox/relevant_positions.jsonl")
    phr.add_argument("--out", default="./outbox/relevant_positions.html")
    phr.add_argument("--title", default="Relevant Positions")
    phr.set_defaults(func=cmd_render_html)

    psg = sub.add_parser("serve-gui")
    psg.add_argument("--report-dir", default=None)
    psg.add_argument("--db", default=None)
    psg.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    psg.add_argument("--host", default=None)
    psg.add_argument("--port", type=int, default=None)
    psg.add_argument("--no-open", action="store_true")
    psg.add_argument("--verbose", action="store_true")
    psg.set_defaults(func=cmd_serve_gui)

    prd = sub.add_parser("refresh-descriptions")
    prd.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    prd.add_argument("--db", default=None)
    prd.add_argument("--model", default="")
    prd.add_argument("--source", default="")
    prd.add_argument("--category", default="", choices=["", "relevant", "not relevant"])
    prd.add_argument("--link", action="append", default=[])
    prd.add_argument("--job-id", action="append", type=int, default=[])
    prd.add_argument("--limit", type=int, default=0)
    prd.add_argument("--overwrite", action="store_true")
    prd.add_argument("--allow-empty", action="store_true")
    prd.add_argument("--quiet-model", action="store_true")
    prd.add_argument("--report-dir", default="")
    prd.set_defaults(func=cmd_refresh_descriptions)

    psk = sub.add_parser("sync-user-skills")
    psk.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    psk.add_argument("--db", default=None)
    psk.add_argument("--model", default="")
    psk.add_argument("--cv", default="./CV")
    psk.add_argument("--limit", type=int, default=80)
    psk.add_argument("--max-chars", type=int, default=40000)
    psk.add_argument("--replace", action="store_true")
    psk.add_argument("--quiet-model", action="store_true")
    psk.set_defaults(func=cmd_sync_user_skills)

    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
