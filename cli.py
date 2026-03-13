import argparse
import html
import json
import os
import re
import sys
import threading
import time
import webbrowser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

try:
    from . import parser as email_parser
    from .llm import LocalLLM
    from .jobs import (
        DEFAULT_PROFILE,
        apply_relevance,
        ensure_db,
        get_applied_jobs,
        get_jobs_by_category,
        get_jobs_for_description_refresh,
        get_relevant_jobs,
        get_viewed_jobs_count,
        ingest_docs_to_db,
        load_profile,
        set_job_applied,
        set_job_description,
        set_job_feedback,
        set_job_summary,
        set_job_viewed,
        update_profile_from_db_signals,
    )
except ImportError:
    import parser as email_parser
    from llm import LocalLLM
    from jobs import (
        DEFAULT_PROFILE,
        apply_relevance,
        ensure_db,
        get_applied_jobs,
        get_jobs_by_category,
        get_jobs_for_description_refresh,
        get_relevant_jobs,
        get_viewed_jobs_count,
        ingest_docs_to_db,
        load_profile,
        set_job_applied,
        set_job_description,
        set_job_feedback,
        set_job_summary,
        set_job_viewed,
        update_profile_from_db_signals,
    )


DEFAULT_PROFILE_PATH = "./profile.json"


def _load_runtime_profile(profile_path: str):
    path = profile_path or DEFAULT_PROFILE_PATH
    if path and os.path.exists(path):
        try:
            return load_profile(path)
        except Exception as exc:
            print(f"Could not load profile '{path}': {exc}. Using built-in defaults.")
    return DEFAULT_PROFILE.copy()


def _render_html_from_items(items, out_html: str, title: str):
    os.makedirs(os.path.dirname(os.path.abspath(out_html)), exist_ok=True)

    cards = []
    for idx, item in enumerate(items, start=1):
        source = html.escape(str(item.get("source", "Unknown")))
        company = html.escape(str(item.get("company", "")))
        role = html.escape(str(item.get("title", "")))
        place = html.escape(str(item.get("place", "")))
        work_type = html.escape(str(item.get("work_type", "Unknown")))
        description = html.escape(str(item.get("description", "")))
        link = str(item.get("position_link", ""))
        safe_link = html.escape(link, quote=True)

        cards.append(
            f"""
            <article class=\"card\">
                            <p><strong>Title:</strong> <a href=\"{safe_link}\" target=\"_blank\" rel=\"noopener noreferrer\">{role}</a></p>
                            <p><strong>Source:</strong> {source}</p>
                            <p><strong>Company:</strong> {company}</p>
                            <p><strong>Place:</strong> {place}</p>
                            <p><strong>Type:</strong> {work_type}</p>
                            <p><strong>Description:</strong> {description}</p>
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
            .card p {{ margin: 6px 0; }}
            a {{ color: #0a58ca; word-break: break-all; }}
        </style>
    </head>
    <body>
        <h1>{html.escape(title)}</h1>
        <p class=\"subtitle\">Items: {len(items)}</p>
        <section class=\"grid\">{''.join(cards) if cards else '<p>No records found.</p>'}</section>
    </body>
</html>
"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Wrote HTML report: {out_html} (items={len(items)})")


def _render_html_dashboard(relevant_items, not_relevant_items, applied_items, out_html: str, title: str, viewed_total: int = 0):
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
            summary = html.escape(str(item.get("summary", "")))
            description = html.escape(str(item.get("description", "")))
            relevance_score = float(item.get("relevance_score", 0) or 0)
            link = str(item.get("position_link", ""))
            safe_link = html.escape(link, quote=True)

            cards.append(
                f"""
                <article class=\"card\" data-job-id=\"{job_id}\">
                    <span class=\"relevance-score\" title=\"Relevance score\">{relevance_score:.2f}</span>
                    <p><strong>Title:</strong> <a href=\"{safe_link}\" target=\"_blank\" rel=\"noopener noreferrer\">{role}</a></p>
                    <p><strong>Source:</strong> {source}</p>
                    <p><strong>Company:</strong> {company}</p>
                    <p><strong>Place:</strong> {place}</p>
                    <p><strong>Type:</strong> {work_type}</p>
                    <p><strong>Summary:</strong> {summary}</p>
                    <p><strong>Description:</strong> {description}</p>
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
            .card p {{ margin: 6px 0; }}
            .relevance-score {{ position: absolute; top: 8px; right: 10px; font-size: 11px; color: #9aa0a6; }}
            .feedback {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; flex-wrap: wrap; }}
            .relevant-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .viewed-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .applied-wrap {{ font-size: 13px; color: #333; display: inline-flex; gap: 5px; align-items: center; }}
            .feedback-status {{ font-size: 12px; color: #555; }}
            a {{ color: #0a58ca; word-break: break-all; }}
            .hidden {{ display: none; }}
            .empty {{ background: #fff; border: 1px dashed #ccc; border-radius: 10px; padding: 12px; margin: 0; }}
        </style>
    </head>
    <body>
        <h1>{html.escape(title)}</h1>
        <div class=\"controls\">
            <button id=\"btn-relevant\" class=\"mode-btn active\" type=\"button\">Relevant ({len(relevant_items)})</button>
            <button id=\"btn-not-relevant\" class=\"mode-btn\" type=\"button\">Not relevant ({len(not_relevant_items)})</button>
            <button id=\"btn-applied\" class=\"mode-btn\" type=\"button\">Applied ({len(applied_items)})</button>
        </div>
        <section id=\"panel-relevant\" class=\"grid\">{relevant_cards}</section>
        <section id=\"panel-not-relevant\" class=\"grid hidden\">{not_relevant_cards}</section>
        <section id=\"panel-applied\" class=\"grid hidden\">{applied_cards}</section>

        <script>
            const btnRelevant = document.getElementById('btn-relevant');
            const btnNotRelevant = document.getElementById('btn-not-relevant');
            const btnApplied = document.getElementById('btn-applied');
            const panelRelevant = document.getElementById('panel-relevant');
            const panelNotRelevant = document.getElementById('panel-not-relevant');
            const panelApplied = document.getElementById('panel-applied');

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
                btnRelevant.textContent = `Relevant (${{relevantCount}})`;
                btnNotRelevant.textContent = `Not relevant (${{notRelevantCount}})`;
                btnApplied.textContent = `Applied (${{appliedCount}})`;
                ensureEmptyState(panelRelevant);
                ensureEmptyState(panelNotRelevant);
                ensureEmptyState(panelApplied);
            }}

            function setMode(mode) {{
                const isRelevant = mode === 'relevant';
                const isApplied = mode === 'applied';
                panelRelevant.classList.toggle('hidden', !isRelevant);
                panelNotRelevant.classList.toggle('hidden', isRelevant || isApplied);
                panelApplied.classList.toggle('hidden', !isApplied);
                btnRelevant.classList.toggle('active', isRelevant);
                btnNotRelevant.classList.toggle('active', !isRelevant && !isApplied);
                btnApplied.classList.toggle('active', isApplied);
            }}

            btnRelevant.addEventListener('click', () => setMode('relevant'));
            btnNotRelevant.addEventListener('click', () => setMode('not relevant'));
            btnApplied.addEventListener('click', () => setMode('applied'));

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
    llm = LocalLLM(model_path=args.model)
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

    llm = LocalLLM(model_path=args.model)
    out_handle = None
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        out_handle = open(args.out, "w", encoding="utf-8")

    processed = 0
    failed = 0
    max_docs = args.limit if args.limit and args.limit > 0 else len(docs)

    try:
        for doc in docs[:max_docs]:
            path = doc.get("path")
            try:
                summary = llm.summarize(doc.get("text", ""), max_tokens=args.max_tokens)
                record = {"path": path, "summary": summary, "links": doc.get("links", [])}
                if out_handle:
                    out_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
                print(f"[OK] {path}")
                print(summary)
                print()
                processed += 1
            except Exception as exc:
                failed += 1
                print(f"[ERR] {path}: {exc}")
    finally:
        if out_handle:
            out_handle.close()

    print(f"Done. Processed={processed}, Failed={failed}, Total={max_docs}")


def _limit_words(text: str, max_words: int) -> str:
    words = (text or "").split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words])


def _extract_position_page_text(position_link: str, max_chars: int = 3000, timeout_sec: int = 8) -> str:
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


def _get_position_page_context(position_link: str, page_context_cache: dict | None = None) -> str:
    link = (position_link or "").strip()
    if not link:
        return ""
    if page_context_cache is not None and link in page_context_cache:
        return page_context_cache.get(link, "") or ""

    page_context = _extract_position_page_text(link)
    if page_context_cache is not None:
        page_context_cache[link] = page_context
    return page_context


def _append_page_context_to_description_raw(description_raw: str, position_link: str, page_context: str, max_chars: int = 9000) -> str:
    base_raw = (description_raw or "").strip()
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


def _prepend_title_to_description_raw(title: str, description_raw: str, max_chars: int = 9000) -> str:
    title_clean = " ".join((title or "").split()).strip()
    raw_clean = (description_raw or "").strip()

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

def _prepend_summary_to_description_raw(summary: str, description_raw: str, max_chars: int = 9000) -> str:
    summary_clean = " ".join((summary or "").split()).strip()
    raw_clean = (description_raw or "").strip()

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


def _enrich_description_raw_with_position_page(db_path: str, row: dict, page_context_cache: dict | None = None) -> str:
    raw = (row.get("description_raw") or "").strip()
    raw = _prepend_title_to_description_raw(row.get("title", ""), raw)
    raw = _prepend_summary_to_description_raw(row.get("summary", ""), raw)
    link = (row.get("position_link") or "").strip()
    if not link:
        return raw

    page_context = _get_position_page_context(link, page_context_cache=page_context_cache)
    merged = _append_page_context_to_description_raw(raw, link, page_context)
    if merged:
        row["description_raw"] = merged
        return merged
    return raw


def _build_description_summary(
    description_raw: str,
    llm: LocalLLM = None,
    position_link: str = "",
    page_context_cache: dict | None = None,
) -> str:
    cleaned = " ".join((description_raw or "").split())

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

        # remove duplicated prefix chunks: A A -> A
        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[:size] == words[size: size * 2]:
                words = words[:size] + words[size * 2:]
                break

        # remove duplicated tail chunks: ... B B -> ... B
        for size in range(min(len(words) // 2, 30), 4, -1):
            if words[-size:] == words[-size * 2: -size]:
                words = words[:-size]
                break

        return " ".join(words)

    if llm:
        page_context = _get_position_page_context(position_link, page_context_cache=page_context_cache)
        if not cleaned and not page_context:
            return ""

        page_block = f"Position page context (if useful):\n{page_context}\n\n" if page_context else ""
        prompt = (
            "Summarize this job description in English in at most 42 words. "
            "Keep only key responsibilities and context, no bullets, no extra commentary. "
            "Use the Description as primary truth; use page context only to refine missing details.\n\n"
            f"Description:\n{cleaned}\n\n"
            f"{page_block}"
            "Summary:"
        )
        try:
            out = llm.generate(prompt, max_tokens=110)
            return _limit_words(remove_repeated_phrases(clean_model_output(out)), 42)
        except Exception:
            pass

    return ""


def _fallback_description_text(description: str, description_raw: str, max_chars: int = 280) -> str:
    if (description or "").strip():
        return description
    compact = " ".join((description_raw or "").split())
    if not compact:
        return ""
    if len(compact) <= max_chars:
        return compact
    return compact[:max_chars].rstrip() + "..."


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

        raw = _enrich_description_raw_with_position_page(db_path, row, page_context_cache=page_context_cache)
        if not raw:
            skipped += 1
            continue

        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
        )
        if not description and not allow_empty:
            skipped += 1
            continue

        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    if progress:
        total_elapsed = time.monotonic() - started_at
        print(f"{progress_label}: done (updated={updated}, skipped={skipped}, elapsed={_fmt_eta(total_elapsed)})")

    return updated, skipped


def cmd_process_inbox(args):
    profile_path = args.profile or DEFAULT_PROFILE_PATH
    profile = _load_runtime_profile(profile_path)
    inbox = args.inbox or profile.get("default_inbox") or "./inbox"
    db_path = args.db or profile.get("default_db") or "./jobs.db"
    report_dir = args.report_dir or profile.get("default_report_dir") or "./outbox"
    model_path = args.model or profile.get("default_model") or ""
    max_input_chars = args.max_input_chars if args.max_input_chars is not None else int(profile.get("max_input_chars", 4500) or 4500)

    docs = email_parser.load_files(inbox)
    if not docs:
        print("No documents found in inbox:", inbox)
        return

    ensure_db(db_path)
    ingest_stats = ingest_docs_to_db(db_path, docs)
    print(
        "Ingestion done: "
        f"processed={ingest_stats.get('processed', 0)}, "
        f"inserted_new={ingest_stats.get('inserted_new', 0)}, "
        f"skipped_existing={ingest_stats.get('skipped_existing', 0)} "
        f"into DB: {db_path}"
    )

    total, relevant_count = apply_relevance(db_path, profile, prune_irrelevant=args.prune_irrelevant)
    print(f"Scored {total} positions; relevant={relevant_count}")

    relevant_jobs = get_relevant_jobs(db_path, limit=args.limit)
    llm = LocalLLM(model_path=model_path, verbose=bool(args.verbose)) if model_path else None

    desc_updated, desc_skipped = _generate_missing_descriptions_for_ingest(db_path, llm=llm, allow_empty=False)
    print(f"Descriptions generated during ingest: updated={desc_updated}, skipped={desc_skipped}")

    for job in relevant_jobs:
        company = job.get("company", "")
        title = job.get("title", "")
        link = job.get("position_link", "")
        text = job.get("raw_text", "")

        compact = re.sub(r"https?://\S+", "[link]", text)
        compact = re.sub(r"\s+", " ", compact).strip()
        text_for_llm = compact[:max_input_chars] if max_input_chars and max_input_chars > 0 else compact

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

    for legacy_name in ["other.html", "unrelevant.html", "relevant.html", "not_relevant.html"]:
        legacy_path = os.path.join(report_dir, legacy_name)
        if os.path.exists(legacy_path):
            os.remove(legacy_path)

    report_data = {}
    page_context_cache: dict[str, str] = {}
    for cat in ["relevant", "not relevant"]:
        rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
        records = []
        for row in rows:
            summary = row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260]
            description = row.get("description") or ""
            description_raw = _enrich_description_raw_with_position_page(db_path, row, page_context_cache=page_context_cache)
            if not description:
                description = _build_description_summary(
                    description_raw,
                    llm=llm,
                    position_link=row.get("position_link", ""),
                    page_context_cache=page_context_cache,
                )
                set_job_description(db_path, row.get("id", 0), description)
            records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    "title": row.get("title", ""),
                    "place": row.get("place", ""),
                    "work_type": row.get("work_type", "Unknown"),
                    "description": description,
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
        applied_records.append(
            {
                "id": row.get("id", 0),
                "source": row.get("source", "Unknown"),
                "company": row.get("company", ""),
                "title": row.get("title", ""),
                "place": row.get("place", ""),
                "work_type": row.get("work_type", "Unknown"),
                "description": description,
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
    )
    print(f"Report written: {dashboard_path}")

    if not relevant_jobs:
        print("No relevant positions after filtering.")

    print(f"Done. Relevant summarized={len(relevant_jobs)}")


def cmd_serve_gui(args):
    cli_verbose = bool(getattr(args, "verbose", False))
    profile_path = os.path.abspath(args.profile or DEFAULT_PROFILE_PATH)
    runtime_profile = _load_runtime_profile(profile_path)
    report_dir = os.path.abspath(args.report_dir or runtime_profile.get("default_report_dir") or "./outbox")
    db_path = os.path.abspath(args.db or runtime_profile.get("default_db") or "./jobs.db")
    inbox_path = os.path.abspath(runtime_profile.get("default_inbox") or "./inbox")
    model_path = runtime_profile.get("default_model") or ""
    host = args.host or runtime_profile.get("server_host") or "127.0.0.1"
    port = args.port if args.port is not None else int(runtime_profile.get("server_port") or 8765)
    ensure_db(db_path)
    os.makedirs(report_dir, exist_ok=True)
    dashboard_path = os.path.join(report_dir, "report.html")
    dashboard_lock = threading.Lock()

    def _rebuild_dashboard(reason: str = ""):
        for attempt in range(3):
            try:
                with dashboard_lock:
                    refreshed_report_data = {}
                    for cat in ["relevant", "not relevant"]:
                        rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
                        records = []
                        for row in rows:
                            records.append(
                                {
                                    "id": row.get("id", 0),
                                    "source": row.get("source", "Unknown"),
                                    "company": row.get("company", ""),
                                    "title": row.get("title", ""),
                                    "place": row.get("place", ""),
                                    "work_type": row.get("work_type", "Unknown"),
                                    "description": _fallback_description_text(row.get("description") or "", row.get("description_raw") or ""),
                                    "position_link": row.get("position_link", ""),
                                    "relevance_score": row.get("relevance_score", 0),
                                    "summary": row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260],
                                    "category": cat,
                                    "viewed": row.get("viewed", 0),
                                    "applied": row.get("applied", 0),
                                }
                            )
                        refreshed_report_data[cat] = records

                    refreshed_applied_rows = get_applied_jobs(db_path, limit=0)
                    refreshed_applied_records = []
                    for row in refreshed_applied_rows:
                        refreshed_applied_records.append(
                            {
                                "id": row.get("id", 0),
                                "source": row.get("source", "Unknown"),
                                "company": row.get("company", ""),
                                "title": row.get("title", ""),
                                "place": row.get("place", ""),
                                "work_type": row.get("work_type", "Unknown"),
                                "description": _fallback_description_text(row.get("description") or "", row.get("description_raw") or ""),
                                "position_link": row.get("position_link", ""),
                                "relevance_score": row.get("relevance_score", 0),
                                "summary": row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260],
                                "category": row.get("category", "relevant"),
                                "viewed": row.get("viewed", 1),
                                "applied": row.get("applied", 1),
                            }
                        )

                    _render_html_dashboard(
                        refreshed_report_data.get("relevant", []),
                        refreshed_report_data.get("not relevant", []),
                        refreshed_applied_records,
                        dashboard_path,
                        "Positions Report",
                        viewed_total=get_viewed_jobs_count(db_path),
                    )
                if reason and not reason.startswith("new record"):
                    print(f"Report regenerated: {reason}")
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"Report regeneration failed ({reason or 'unknown'}): {exc}")
                else:
                    time.sleep(0.2)

    _rebuild_dashboard(reason="startup snapshot")

    def _sync_inbox_in_background():
        try:
            if not os.path.isdir(inbox_path):
                print(f"Inbox directory not found, skipping background sync: {inbox_path}")
                return

            docs = email_parser.load_files(inbox_path)
            if not docs:
                print(f"Background sync: no documents found in inbox: {inbox_path}")
                return

            llm_for_sync = LocalLLM(model_path=model_path, verbose=cli_verbose) if model_path else None
            print(f"Background sync started: files={len(docs)}")

            new_records = 0
            last_inserted_logged = -1

            def _on_new_record():
                nonlocal new_records
                new_records += 1
                _rebuild_dashboard(reason=f"new record {new_records}")

            def _on_progress(processed: int, inserted_new: int, skipped_existing: int):
                nonlocal last_inserted_logged
                if inserted_new != last_inserted_logged:
                    print(
                        f"Background sync progress: processed={processed}, inserted={inserted_new}, "
                        f"skipped_existing={skipped_existing}"
                    )
                    last_inserted_logged = inserted_new

            ingest_stats = ingest_docs_to_db(
                db_path,
                docs,
                on_new_record=_on_new_record,
                on_progress=_on_progress,
            )
            print("Background sync: scoring relevance...")
            total, relevant_count = apply_relevance(db_path, runtime_profile, prune_irrelevant=False)
            print(f"Background sync: relevance scored (total={total}, relevant={relevant_count})")
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

            print(
                f"Background sync done: input_files={len(docs)}, processed={ingest_stats.get('processed', 0)}, "
                f"inserted={ingest_stats.get('inserted_new', 0)}, skipped_existing={ingest_stats.get('skipped_existing', 0)}, "
                f"total_jobs={total}, relevant={relevant_count}"
            )
        except Exception as exc:
            print(f"Background sync failed: {exc}")

    sync_thread = threading.Thread(target=_sync_inbox_in_background, name="spejder-inbox-sync", daemon=True)
    sync_thread.start()

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
            if path not in {"/api/feedback", "/api/viewed", "/api/applied"}:
                self._write_json(404, {"ok": False, "error": "Not found"})
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                payload = json.loads(body.decode("utf-8"))

                job_id = int(payload.get("job_id"))
                if path == "/api/feedback":
                    signal = str(payload.get("signal", "")).strip().lower()
                    if signal not in {"relevant", "not relevant"}:
                        self._write_json(400, {"ok": False, "error": "signal must be 'relevant' or 'not relevant'"})
                        return
                    updated = set_job_feedback(db_path, job_id, signal)
                    learning_info = update_profile_from_db_signals(db_path, profile_path)
                elif path == "/api/applied":
                    applied_raw = payload.get("applied")
                    if not isinstance(applied_raw, bool):
                        self._write_json(400, {"ok": False, "error": "applied must be boolean"})
                        return
                    updated = set_job_applied(db_path, job_id, applied_raw)
                    learning_info = update_profile_from_db_signals(db_path, profile_path) if applied_raw else None
                else:
                    viewed_raw = payload.get("viewed")
                    if not isinstance(viewed_raw, bool):
                        self._write_json(400, {"ok": False, "error": "viewed must be boolean"})
                        return
                    updated = set_job_viewed(db_path, job_id, viewed_raw)

                if not updated:
                    self._write_json(404, {"ok": False, "error": "job id not found"})
                    return

                _rebuild_dashboard()

                if path == "/api/feedback":
                    self._write_json(200, {"ok": True, "job_id": job_id, "signal": signal, "profile_learning": learning_info})
                elif path == "/api/applied":
                    self._write_json(200, {"ok": True, "job_id": job_id, "applied": applied_raw, "profile_learning": learning_info})
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

    if not args.no_open:
        try:
            opened = webbrowser.open(report_url, new=2)
            if opened:
                print(f"Opened in default browser: {report_url}")
            else:
                print(f"Could not auto-open browser. Open manually: {report_url}")
        except Exception as exc:
            print(f"Could not auto-open browser: {exc}. Open manually: {report_url}")

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
    with open(args.input, "r", encoding="utf-8") as f:
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

    llm = LocalLLM(model_path=model_path, verbose=not args.quiet_model) if model_path else None
    if not llm:
        print("No --model provided, summaries will remain empty unless your logic sets fallback text.")

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
    for row in rows:
        raw = _enrich_description_raw_with_position_page(db_path, row, page_context_cache=page_context_cache)
        if not raw:
            skipped += 1
            continue
        description = _build_description_summary(
            raw,
            llm=llm,
            position_link=row.get("position_link", ""),
            page_context_cache=page_context_cache,
        )
        if not description and not args.allow_empty:
            skipped += 1
            continue
        set_job_description(db_path, row.get("id", 0), description)
        updated += 1

    print(f"Description refresh done. matched={len(rows)}, updated={updated}, skipped={skipped}")

    if args.report_dir:
        os.makedirs(args.report_dir, exist_ok=True)
        report_data = {}
        for cat in ["relevant", "not relevant"]:
            cat_rows = get_jobs_by_category(db_path, cat, limit=0, unviewed_only=True)
            records = []
            for row in cat_rows:
                records.append(
                    {
                        "id": row.get("id", 0),
                        "source": row.get("source", "Unknown"),
                        "company": row.get("company", ""),
                        "title": row.get("title", ""),
                        "place": row.get("place", ""),
                        "work_type": row.get("work_type", "Unknown"),
                        "description": _fallback_description_text(row.get("description") or "", row.get("description_raw") or ""),
                        "position_link": row.get("position_link", ""),
                        "relevance_score": row.get("relevance_score", 0),
                        "summary": row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260],
                        "category": cat,
                        "viewed": row.get("viewed", 0),
                        "applied": row.get("applied", 0),
                    }
                )
            report_data[cat] = records

        applied_rows = get_applied_jobs(db_path, limit=0)
        applied_records = []
        for row in applied_rows:
            applied_records.append(
                {
                    "id": row.get("id", 0),
                    "source": row.get("source", "Unknown"),
                    "company": row.get("company", ""),
                    "title": row.get("title", ""),
                    "place": row.get("place", ""),
                    "work_type": row.get("work_type", "Unknown"),
                    "description": _fallback_description_text(row.get("description") or "", row.get("description_raw") or ""),
                    "position_link": row.get("position_link", ""),
                    "relevance_score": row.get("relevance_score", 0),
                    "summary": row.get("summary") or " ".join((row.get("raw_text") or "").split())[:260],
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
    psm.set_defaults(func=cmd_summarize_file)

    psf = sub.add_parser("summarize-folder")
    psf.add_argument("--folder", required=True)
    psf.add_argument("--model", required=True)
    psf.add_argument("--max-tokens", type=int, default=200)
    psf.add_argument("--limit", type=int, default=0)
    psf.add_argument("--out", default="")
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

    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
