import json
import os
from contextlib import nullcontext

from spejder.core import DEFAULT_PROFILE_PATH, load_runtime_profile
from spejder.llm import LocalLLM
from spejder.parsers import email_parser
from spejder.workflows.job_enrichment import _translate_text_to_english_if_needed


def summarize_file(path: str, model: str, profile: str = None, max_tokens: int = 200, verbose_model: bool = False):
    if not os.path.exists(path):
        print("File not found:", path)
        return
    runtime_profile = load_runtime_profile(profile or DEFAULT_PROFILE_PATH)
    doc = email_parser.parse_email_file(path)
    llm = LocalLLM(model_path=model, verbose=bool(verbose_model))
    try:
        source_text = doc.get("text", "")
        normalized_text = _translate_text_to_english_if_needed(
            source_text,
            runtime_profile=runtime_profile,
        )
        summary = llm.summarize(normalized_text, max_tokens=max_tokens)
        print("--- Summary ---")
        print(summary)
    except Exception as exc:
        print("LLM error:", exc)


def summarize_folder(folder: str, model: str, profile: str = None, max_tokens: int = 200, limit: int = 0, out: str = "", verbose_model: bool = False):
    docs = email_parser.load_files(folder)
    if not docs:
        print("No documents found in folder:", folder)
        return

    runtime_profile = load_runtime_profile(profile or DEFAULT_PROFILE_PATH)
    llm = LocalLLM(model_path=model, verbose=bool(verbose_model))
    if out:
        os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)

    processed = 0
    failed = 0
    max_docs = limit if limit and limit > 0 else len(docs)

    with open(out, "w", encoding="utf-8") if out else nullcontext() as out_handle:
        for doc in docs[:max_docs]:
            path = doc.get("path")
            try:
                source_text = doc.get("text", "")
                normalized_text = _translate_text_to_english_if_needed(
                    source_text,
                    runtime_profile=runtime_profile,
                )
                summary = llm.summarize(normalized_text, max_tokens=max_tokens)
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
