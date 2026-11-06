"""Company catalog orchestration: merge, rename, delete."""

from __future__ import annotations

from typing import Callable, Optional

from spejder.db.companies import (
    delete_company_row,
    delete_company_rows,
    merge_company_rows,
    prune_empty_companies,
    rename_company_row,
)
from spejder.jobs.deduplication import merge_duplicate_positions

StageCallback = Callable[[str, str], None]


def run_empty_company_prune(
    db_path: str,
    *,
    on_stage: Optional[StageCallback] = None,
) -> list[int]:
    """Emit ``empty_companies`` then prune zero-job catalog companies (full catalog)."""
    if on_stage is not None:
        on_stage("empty_companies", "Pruning empty companies")
    return prune_empty_companies(db_path)


def merge_companies(
    db_path: str,
    company_ids: list[int],
    display_name: str,
) -> dict:
    """Merge 2+ catalog companies; always run position dedupe afterward."""
    result = merge_company_rows(db_path, company_ids, display_name)
    dedupe = merge_duplicate_positions(db_path)
    pruned = prune_empty_companies(db_path, result.get("deleted_ids") or [])
    return {
        **result,
        "dedupe": dedupe,
        "pruned_empty_ids": pruned,
    }


def rename_company(db_path: str, company_id: int, display_name: str) -> dict:
    """Change display name, rewrite member jobs, ensure display is an alias."""
    return rename_company_row(db_path, company_id, display_name)


def delete_company(db_path: str, company_id: int) -> dict:
    """Delete catalog company + membership jobs. Idempotent if already gone."""
    return delete_company_row(db_path, company_id)


def delete_companies(db_path: str, company_ids: list[int]) -> dict:
    """Delete many catalog companies + membership jobs in one DB transaction."""
    return delete_company_rows(db_path, company_ids)
