"""skill_patterns table: get/upsert/migrate."""
import json
import os
import re
from datetime import datetime, timezone

from .connection import _connect, ensure_db
from .utils import _normalize_skill_name_key

def _skill_to_regex_simple(name: str) -> str:
    tokens = [re.escape(t) for t in re.findall(
        r"[A-Za-z0-9+#.]+", name or "") if t]
    if not tokens:
        return name
    return r"\b" + r"\s+".join(tokens) + r"\b"

def get_skill_patterns(db_path: str, enabled_only: bool = True) -> list[dict]:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        q = (
            "SELECT name, pattern, source, occurrences, weight, enabled, last_seen_at, created_at "
            "FROM skill_patterns"
        )
        params: list = []
        if enabled_only:
            q += " WHERE enabled=1"
        q += " ORDER BY weight DESC, occurrences DESC, name ASC"
        cur.execute(q, params)
        rows = cur.fetchall()
        return [
            {
                "name": r[0] or "",
                "pattern": r[1] or "",
                "source": r[2] or "",
                "occurrences": int(r[3] or 0),
                "weight": float(r[4] or 0),
                "enabled": int(r[5] or 0),
                "last_seen_at": r[6] or "",
                "created_at": r[7] or "",
            }
            for r in rows
        ]
    finally:
        conn.close()


def upsert_skill_pattern(
    db_path: str,
    name: str,
    pattern: str,
    source: str = "learned",
    occurrences_inc: int = 0,
    weight_inc: float = 0.0,
    enabled: bool = True,
) -> bool:
    """Insert or update a skill pattern.

    ``occurrences_inc`` / ``weight_inc`` remain additive for seed/helpers.
    Learning passes must use ``reconcile_skill_pattern_learning_scores`` so
    Learned reflects current batch evidence rather than lifetime sums.
    """
    name_clean = (name or "").strip()
    pattern_clean = (pattern or "").strip()
    name_key = _normalize_skill_name_key(name_clean)
    if not name_clean or not name_key or not pattern_clean:
        return False

    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO skill_patterns
                (name, name_key, pattern, source, occurrences, weight, enabled, created_at, updated_at, last_seen_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name_key) DO UPDATE SET
                name=excluded.name,
                pattern=CASE
                    WHEN excluded.pattern IS NOT NULL AND TRIM(excluded.pattern)<>'' THEN excluded.pattern
                    ELSE skill_patterns.pattern
                END,
                source=CASE
                    WHEN skill_patterns.source IS NULL OR TRIM(skill_patterns.source)='' THEN excluded.source
                    ELSE skill_patterns.source
                END,
                occurrences=skill_patterns.occurrences + excluded.occurrences,
                weight=skill_patterns.weight + excluded.weight,
                enabled=excluded.enabled,
                updated_at=excluded.updated_at,
                last_seen_at=excluded.last_seen_at
            """,
            (
                name_clean,
                name_key,
                pattern_clean,
                source,
                max(0, int(occurrences_inc)),
                max(0.0, float(weight_inc)),
                1 if enabled else 0,
                now,
                now,
                now,
            ),
        )
        conn.commit()
        return True
    finally:
        conn.close()


def reconcile_skill_pattern_learning_scores(
    db_path: str, scores_by_name: dict[str, int]
) -> dict[str, int]:
    """Set ``occurrences`` / ``weight`` from the current learning batch.

    Every existing ``skill_patterns`` row is reconciled in one transaction:
    scores present in ``scores_by_name`` (matched by ``name_key``) are stored
    as absolute values; all other rows are reset to 0. This is the one-time
    migration path for inflated additive Learned values — the next learning
    pass after upgrade replaces lifetime sums with current evidence.

    Does not insert new patterns (callers still use ``upsert_skill_pattern``
    for admission of new names). Rows with a positive score also bump
    ``last_seen_at``; zeroed rows keep their prior ``last_seen_at``.
    """
    ensure_db(db_path)
    now = datetime.now(timezone.utc).isoformat()
    keyed: dict[str, int] = {}
    for raw_name, raw_score in (scores_by_name or {}).items():
        name_key = _normalize_skill_name_key(str(raw_name or ""))
        if not name_key:
            continue
        score = max(0, int(raw_score or 0))
        # Prefer the highest score if duplicate keys collide after normalize.
        keyed[name_key] = max(score, keyed.get(name_key, 0))

    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE skill_patterns
            SET occurrences=0, weight=0, updated_at=?
            """,
            (now,),
        )
        reset_count = int(cur.rowcount or 0)
        scored = 0
        for name_key, score in keyed.items():
            if score <= 0:
                continue
            cur.execute(
                """
                UPDATE skill_patterns
                SET occurrences=?, weight=?, updated_at=?, last_seen_at=?
                WHERE name_key=?
                """,
                (score, float(score), now, now, name_key),
            )
            if cur.rowcount:
                scored += 1
        conn.commit()
        return {
            "patterns_reset": reset_count,
            "patterns_scored": scored,
            "score_keys": len(keyed),
        }
    finally:
        conn.close()


def migrate_profile_skill_patterns_to_db(
    db_path: str, profile_path: str
) -> dict[str, int]:
    ensure_db(db_path)

    profile: AppConfig = {}
    if profile_path and os.path.exists(profile_path):
        try:
            with open(profile_path, encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                profile = loaded
        except Exception:
            profile = {}

    raw = profile.known_skill_patterns
    if not isinstance(raw, list) or not raw:
        raw = getattr(DEFAULT_PROFILE, "known_skill_patterns", [])

    inserted = 0
    seed_count = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        pattern = str(item.get("pattern", "")).strip()
        if not name or not pattern:
            continue
        seed_count += 1
        if upsert_skill_pattern(
            db_path,
            name=name,
            pattern=pattern,
            source="profile_seed",
            occurrences_inc=0,
            weight_inc=0.0,
            enabled=True,
        ):
            inserted += 1

    return {"seed_count": int(seed_count), "inserted": int(inserted)}

