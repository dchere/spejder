# pylint: disable=all
from spejder.db import *
from spejder.db import _provider_from_link, _normalize_position_link
from spejder.jobs.parsing.core import extract_job_entries
import re
import json
import base64
from datetime import datetime, timezone
from urllib.parse import parse_qs, unquote, urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from collections.abc import Callable
from typing import Optional
from html import unescape
from bs4 import BeautifulSoup
from collections import Counter
from spejder.config import AppConfig

COMPANY_NOISE_TOKENS = {'danmark', 'denmark', 'aps', 'a', 's', 'as', 'ab', 'oy', 'ltd', 'llc', 'inc', 'group', 'holding'}
LEARNING_STOPWORDS = {'about', 'above', 'after', 'again', 'against', 'all', 'also', 'and', 'any', 'are', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'can', 'company', 'could', 'danish', 'denmark', 'developer', 'email', 'for', 'from', 'have', 'into', 'job', 'jobs', 'just', 'more', 'not', 'our', 'out', 'position', 'role', 'than', 'that', 'the', 'their', 'them', 'there', 'these', 'this', 'those', 'through', 'under', 'using', 'very', 'want', 'when', 'where', 'which', 'with', 'you', 'your'}
EASY_APPLY_PATTERN = re.compile(r'\beasy\s*apply\b', flags=re.IGNORECASE)
def ingest_docs_to_db(
    db_path: str,
    docs: list[dict],
    entry_transform: Optional[Callable[[dict], dict]] = None,
    on_new_record: Optional[Callable[[], None]] = None,
    on_progress: Optional[Callable[[int, int, int], None]] = None,
) -> dict[str, object]:
    processed = 0
    inserted_new = 0
    skipped_existing = 0
    positions_by_file: list[dict[str, object]] = []
    for doc in docs:
        file_path = str(doc.get("path") or doc.get("id") or "")
        entries = extract_job_entries(doc)
        file_found = 0
        file_inserted = 0
        file_skipped = 0
        for entry in entries:
            if not entry.get("position_link"):
                continue
            if entry_transform is not None:
                entry = entry_transform(dict(entry))
            file_found += 1
            is_new_record = upsert_job(db_path, entry)
            if is_new_record and on_new_record:
                on_new_record()
            if is_new_record:
                inserted_new += 1
                file_inserted += 1
            else:
                skipped_existing += 1
                file_skipped += 1
            processed += 1
            if on_progress:
                on_progress(processed, inserted_new, skipped_existing)
        positions_by_file.append(
            {
                "file": file_path,
                "found": int(file_found),
                "inserted_new": int(file_inserted),
                "skipped_existing": int(file_skipped),
            }
        )
    return {
        "processed": int(processed),
        "inserted_new": int(inserted_new),
        "skipped_existing": int(skipped_existing),
        "positions_by_file": positions_by_file,
    }

