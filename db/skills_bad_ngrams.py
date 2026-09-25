"""bad_ngram_weights accumulator."""
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from .connection import _connect, ensure_db


def count_bad_ngrams(db_path: str) -> int:
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bad_ngram_weights")
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def summarize_bad_ngrams(db_path: str) -> dict:
    """Return cloud size / weight totals for operator-facing status."""
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*), COALESCE(SUM(weight), 0), COALESCE(MAX(weight), 0) "
            "FROM bad_ngram_weights"
        )
        row = cur.fetchone()
        if not row:
            return {"ngram_count": 0, "total_weight": 0, "max_weight": 0}
        return {
            "ngram_count": int(row[0] or 0),
            "total_weight": int(row[1] or 0),
            "max_weight": int(row[2] or 0),
        }
    except sqlite3.OperationalError:
        return {"ngram_count": 0, "total_weight": 0, "max_weight": 0}
    finally:
        conn.close()


def get_bad_ngram_weights(
    db_path: str,
    ngrams: list[tuple[str, int]],
) -> dict[tuple[str, int], int]:
    if not ngrams:
        return {}
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        weights: dict[tuple[str, int], int] = {}
        chunk_size = 400
        for start in range(0, len(ngrams), chunk_size):
            chunk = ngrams[start : start + chunk_size]
            placeholders = ",".join("(?, ?)" for _ in chunk)
            params: list = []
            for ngram, gram_size in chunk:
                params.extend([ngram, int(gram_size)])
            cur.execute(
                f"SELECT ngram, gram_size, weight FROM bad_ngram_weights "
                f"WHERE (ngram, gram_size) IN ({placeholders})",
                params,
            )
            for ngram, gram_size, weight in cur.fetchall():
                weights[(str(ngram), int(gram_size))] = int(weight or 0)
        return weights
    except sqlite3.OperationalError:
        return {}
    finally:
        conn.close()


def upsert_bad_ngrams(
    db_path: str,
    ngrams: list[tuple[str, int]],
    increment: int = 1,
    max_weight: Optional[int] = None,
) -> int:
    if not ngrams or increment <= 0:
        return 0
    counts: dict[tuple[str, int], int] = {}
    for ngram in ngrams:
        counts[ngram] = counts.get(ngram, 0) + int(increment)
    return upsert_bad_ngram_counts(db_path, counts, max_weight=max_weight)


def upsert_bad_ngram_counts(
    db_path: str,
    counts: dict[tuple[str, int], int],
    max_weight: Optional[int] = None,
) -> int:
    if not counts:
        return 0
    updated = _upsert_bad_ngram_counts_once(db_path, counts, max_weight=max_weight)
    if updated > 0:
        return updated
    ensure_db(db_path)
    return _upsert_bad_ngram_counts_once(db_path, counts, max_weight=max_weight)


def _upsert_bad_ngram_counts_once(
    db_path: str,
    counts: dict[tuple[str, int], int],
    max_weight: Optional[int] = None,
) -> int:
    if not counts:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    cap = int(max_weight) if max_weight is not None and int(max_weight) > 0 else None
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        updated = 0
        for (ngram, gram_size), increment in counts.items():
            text = (ngram or "").strip()
            size = int(gram_size)
            amount = int(increment)
            if not text or size not in (1, 2) or amount <= 0:
                continue
            if cap is None:
                cur.execute(
                    """
                    INSERT INTO bad_ngram_weights (ngram, gram_size, weight, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(ngram, gram_size) DO UPDATE SET
                        weight = bad_ngram_weights.weight + excluded.weight,
                        updated_at = excluded.updated_at
                    """,
                    (text, size, amount, now),
                )
            else:
                insert_weight = min(amount, cap)
                cur.execute(
                    """
                    INSERT INTO bad_ngram_weights (ngram, gram_size, weight, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(ngram, gram_size) DO UPDATE SET
                        weight = MIN(?, bad_ngram_weights.weight + excluded.weight),
                        updated_at = excluded.updated_at
                    """,
                    (text, size, insert_weight, now, cap),
                )
            updated += 1
        conn.commit()
        return updated
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()


def decrement_bad_ngram_counts(
    db_path: str,
    counts: dict[tuple[str, int], int],
) -> int:
    """Subtract weights; delete rows that reach zero or below. Returns keys touched."""
    if not counts:
        return 0
    touched = _decrement_bad_ngram_counts_once(db_path, counts)
    if touched > 0:
        return touched
    ensure_db(db_path)
    return _decrement_bad_ngram_counts_once(db_path, counts)


def _decrement_bad_ngram_counts_once(
    db_path: str,
    counts: dict[tuple[str, int], int],
) -> int:
    if not counts:
        return 0
    now = datetime.now(timezone.utc).isoformat()
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        touched = 0
        for (ngram, gram_size), amount in counts.items():
            text = (ngram or "").strip()
            size = int(gram_size)
            delta = int(amount)
            if not text or size not in (1, 2) or delta <= 0:
                continue
            cur.execute(
                "SELECT weight FROM bad_ngram_weights WHERE ngram=? AND gram_size=?",
                (text, size),
            )
            row = cur.fetchone()
            if not row:
                continue
            new_weight = int(row[0] or 0) - delta
            if new_weight <= 0:
                cur.execute(
                    "DELETE FROM bad_ngram_weights WHERE ngram=? AND gram_size=?",
                    (text, size),
                )
            else:
                cur.execute(
                    """
                    UPDATE bad_ngram_weights
                    SET weight=?, updated_at=?
                    WHERE ngram=? AND gram_size=?
                    """,
                    (new_weight, now, text, size),
                )
            touched += 1
        conn.commit()
        return touched
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()
