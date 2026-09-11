import os
import sqlite3

from spejder.db.utils import SQLITE_TIMEOUT_SECONDS, SQLITE_BUSY_TIMEOUT_MS
from spejder.db.schema import apply_schema
from spejder.db.maintenance import apply_maintenance


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_job_link(db_path: str, job_id: int):
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT position_link FROM jobs WHERE id=?", (job_id,))
        return cur.fetchone()
    finally:
        conn.close()


def ensure_db(db_path: str):
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        apply_schema(cur)
        apply_maintenance(cur)
        conn.commit()
    finally:
        conn.close()
