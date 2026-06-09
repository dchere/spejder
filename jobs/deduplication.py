import re
from datetime import datetime, timezone

from spejder.db import (
    _provider_from_link,
    batch_update_and_delete_jobs,
    get_all_jobs_for_dedupe,
    sanitize_job_title,
)

COMPANY_NOISE_TOKENS = {
    "danmark",
    "denmark",
    "aps",
    "a",
    "s",
    "as",
    "ab",
    "oy",
    "ltd",
    "llc",
    "inc",
    "group",
    "holding",
}


def _normalize_title_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


COMPANY_NOISE_TOKENS = {
    "danmark",
    "denmark",
    "aps",
    "a",
    "s",
    "as",
    "ab",
    "oy",
    "ltd",
    "llc",
    "inc",
    "group",
    "holding",
}



def _normalize_company_key(value: str) -> str:
    tokens = re.findall(r"[a-z0-9]+", (value or "").lower())
    kept = [token for token in tokens if token and token not in COMPANY_NOISE_TOKENS]
    if not kept:
        kept = tokens
    return "".join(kept)



def _cross_source_dedupe_key(source: str, company: str, title: str) -> str:
    src = (source or "").strip().lower()
    if src not in {"linkedin", "jobindex"}:
        return ""
    company_key = _normalize_company_key(company)
    title_key = _normalize_title_key(sanitize_job_title(title))
    if not company_key or not title_key:
        return ""
    return f"{company_key}|{title_key}"



def _canonical_source_rank(source: str) -> int:
    src = (source or "").strip().lower()
    if src == "jobindex":
        return 2
    if src == "linkedin":
        return 1
    return 0



def merge_cross_source_duplicates(db_path: str) -> dict[str, int]:
    rows = get_all_jobs_for_dedupe(db_path)
    by_key: dict[str, list[dict]] = {}
    for row in rows:
        rid = int(row[0] or 0)
        src = str(row[1] or "").strip() or _provider_from_link(str(row[6] or ""))
        key = _cross_source_dedupe_key(src, str(row[2] or ""), str(row[3] or ""))
        if not key:
            continue
        by_key.setdefault(key, []).append({
            "id": rid, "source": src, "company": str(row[2] or ""),
            "title": str(row[3] or ""), "place": str(row[4] or ""),
            "work_type": str(row[5] or ""), "position_link": str(row[6] or ""),
            "raw_text": str(row[7] or ""), "viewed": int(row[8] or 0),
            "applied": int(row[9] or 0)
        })

    list_to_delete: set[int] = set()
    pending_updates: dict[int, dict] = {}
    merged_groups = 0

    for key, items in by_key.items():
        if len(items) < 2: continue
        merged_groups += 1
        items.sort(key=lambda i: _canonical_source_rank(i["source"]), reverse=True)
        keep = items[0]

        for d in items[1:]:
            if not keep["company"] and d["company"]: keep["company"] = d["company"]
            if not keep["title"] and d["title"]: keep["title"] = d["title"]
            if not keep["place"] and d["place"]: keep["place"] = d["place"]
            if not keep["work_type"] and d["work_type"]: keep["work_type"] = d["work_type"]
            if not keep["raw_text"] and d["raw_text"]: keep["raw_text"] = d["raw_text"]
            if d["viewed"] > keep["viewed"]: keep["viewed"] = d["viewed"]
            if d["applied"] > keep["applied"]: keep["applied"] = d["applied"]

        pending_updates[keep["id"]] = keep
        list_to_delete.update({i["id"] for i in items[1:]})

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    update_tuples = [(u["company"], u["title"], u["place"], u["work_type"],
                     u["raw_text"], u["viewed"], u["applied"], now, b_id)
                     for b_id, u in pending_updates.items()]

    batch_update_and_delete_jobs(db_path, update_tuples, list(list_to_delete))
    return {"rows_deleted": len(list_to_delete), "rows_updated": len(update_tuples), "groups_merged": merged_groups}


