"""Shared helpers for skills-related tests."""

from spejder.db.connection import _connect


def stamp_skill_patterns_created_at(
    db_path: str, created_at_by_name_key: dict[str, str]
) -> None:
    """Set skill_patterns.created_at for test fixtures.

    Keys must be DB name_key values (normalized lowercase), not display names.
    """
    conn = _connect(db_path)
    try:
        cur = conn.cursor()
        for name_key, created_at in created_at_by_name_key.items():
            cur.execute(
                "UPDATE skill_patterns SET created_at=? WHERE name_key=?",
                (created_at, name_key),
            )
        conn.commit()
    finally:
        conn.close()
