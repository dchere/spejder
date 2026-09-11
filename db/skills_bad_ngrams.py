"""bad_ngram_weights accumulator."""
import sqlite3
from datetime import datetime, timezone

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
) -> int:
    if not ngrams or increment <= 0:
        return 0
    counts: dict[tuple[str, int], int] = {}
    for ngram in ngrams:
        counts[ngram] = counts.get(ngram, 0) + int(increment)
    return upsert_bad_ngram_counts(db_path, counts)


def upsert_bad_ngram_counts(
    db_path: str,
    counts: dict[tuple[str, int], int],
) -> int:
    if not counts:
        return 0
    updated = _upsert_bad_ngram_counts_once(db_path, counts)
    if updated > 0:
        return updated
    ensure_db(db_path)
    return _upsert_bad_ngram_counts_once(db_path, counts)


def _upsert_bad_ngram_counts_once(
    db_path: str,
    counts: dict[tuple[str, int], int],
) -> int:
    if not counts:
        return 0
    now = datetime.now(timezone.utc).isoformat()
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
            updated += 1
        conn.commit()
        return updated
    except sqlite3.OperationalError:
        return 0
    finally:
        conn.close()

