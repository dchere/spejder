"""CLI for Spejder"""
# pylint: disable=too-many-locals,too-many-statements,missing-function-docstring,line-too-long,wrong-import-order,no-name-in-module,unused-import
import argparse
import sys
import os

from spejder.workflows import (
    report_links, summarize_file, summarize_folder, serve_gui,
    init_profile, render_html, refresh_descriptions,
    dedupe_jobs, process_inbox, _initialize_llm_or_exit
)
from spejder.core import DEFAULT_PROFILE_PATH
from spejder.extractors.skill_extractor import sync_user_skills, cleanup_skills
from spejder.managers.language_manager import initialize_language_checker_or_exit, initialize_translation_or_exit

LANGUAGE_CHECKER_REQUIRED_COMMANDS = ['process_inbox', 'dedupe_jobs', 'summarize_file', 'summarize_folder', 'refresh_descriptions']
TRANSLATION_REQUIRED_COMMANDS = ['process_inbox', 'dedupe_jobs', 'summarize_file', 'summarize_folder', 'refresh_descriptions']
LLM_REQUIRED_COMMANDS = ['process_inbox', 'dedupe_jobs', 'summarize_file', 'summarize_folder', 'refresh_descriptions']

def cmd_report_links(args):
    report_links(folder=args.folder)

def cmd_summarize_file(args):
    summarize_file(path=args.path, model=args.model, profile=args.profile, max_tokens=args.max_tokens, verbose_model=args.verbose_model)

def cmd_summarize_folder(args):
    summarize_folder(folder=args.folder, model=args.model, profile=args.profile, max_tokens=args.max_tokens, limit=args.limit, out=args.out, verbose_model=args.verbose_model)

def cmd_process_inbox(args):
    process_inbox(inbox=args.inbox, db=args.db, profile=args.profile, model=args.model, report_dir=args.report_dir, limit=args.limit, max_tokens=args.max_tokens, max_input_chars=args.max_input_chars, prune_irrelevant=args.prune_irrelevant, verbose=args.verbose)

def cmd_serve_gui(args):
    serve_gui(profile=args.profile, report_dir=args.report_dir, db=args.db, host=args.host, port=args.port, no_open=args.no_open, verbose=args.verbose)

def cmd_init_profile(args):
    init_profile(path=args.path, force=args.force)

def cmd_render_html(args):
    render_html(input_val=args.input, out=args.out, title=args.title)

def cmd_refresh_descriptions(args):
    refresh_descriptions(profile=args.profile, db=args.db, model=args.model, source=args.source, category=args.category, link=args.link, job_id=args.job_id, limit=args.limit, overwrite=args.overwrite, allow_empty=args.allow_empty, quiet_model=args.quiet_model, report_dir=args.report_dir)

def cmd_sync_user_skills(args):
    sync_user_skills(profile=args.profile, db=args.db, model=args.model, cv=args.cv, limit=args.limit, max_chars=args.max_chars, replace=args.replace, quiet_model=args.quiet_model)

def cmd_cleanup_skills(args):
    cleanup_skills(profile=args.profile, db=args.db, limit=args.limit, dry_run=args.dry_run)

def cmd_dedupe_jobs(args):
    dedupe_jobs(profile=args.profile, db=args.db)

def main(argv=None):
    p = argparse.ArgumentParser(prog="spejder")
    sub = p.add_subparsers(dest="cmd")

    pr = sub.add_parser("report-links")
    pr.add_argument("folder")
    pr.set_defaults(func=cmd_report_links)

    psm = sub.add_parser("summarize-file")
    psm.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    psm.add_argument("--path", required=True)
    psm.add_argument("--model", required=True)
    psm.add_argument("--max-tokens", type=int, default=200)
    psm.add_argument("--verbose-model", action="store_true")
    psm.set_defaults(func=cmd_summarize_file)

    psf = sub.add_parser("summarize-folder")
    psf.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    psf.add_argument("--folder", required=True)
    psf.add_argument("--model", required=True)
    psf.add_argument("--max-tokens", type=int, default=200)
    psf.add_argument("--limit", type=int, default=0)
    psf.add_argument("--out", default="")
    psf.add_argument("--verbose-model", action="store_true")
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

    psk = sub.add_parser("sync-user-skills")
    psk.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    psk.add_argument("--db", default=None)
    psk.add_argument("--model", default="")
    psk.add_argument("--cv", default="./CV")
    psk.add_argument("--limit", type=int, default=80)
    psk.add_argument("--max-chars", type=int, default=40000)
    psk.add_argument("--replace", action="store_true")
    psk.add_argument("--quiet-model", action="store_true")
    psk.set_defaults(func=cmd_sync_user_skills)

    pcs = sub.add_parser("cleanup-skills")
    pcs.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    pcs.add_argument("--db", default=None)
    pcs.add_argument("--limit", type=int, default=0)
    pcs.add_argument("--dry-run", action="store_true")
    pcs.set_defaults(func=cmd_cleanup_skills)

    pdj = sub.add_parser("dedupe-jobs")
    pdj.add_argument("--profile", default=DEFAULT_PROFILE_PATH)
    pdj.add_argument("--db", default=None)
    pdj.set_defaults(func=cmd_dedupe_jobs)

    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        sys.exit(1)
    
    cmd_name = getattr(args, "cmd", "")
    if cmd_name:
        cmd_name = cmd_name.replace("-", "_")
        
    if cmd_name in LANGUAGE_CHECKER_REQUIRED_COMMANDS:
        initialize_language_checker_or_exit(args.profile or DEFAULT_PROFILE_PATH)
    if cmd_name in TRANSLATION_REQUIRED_COMMANDS:
        initialize_translation_or_exit(args.profile or DEFAULT_PROFILE_PATH)
    if cmd_name in LLM_REQUIRED_COMMANDS:
        override_model_path = str(getattr(args, "model", "") or "")
        _initialize_llm_or_exit(
            args.profile or DEFAULT_PROFILE_PATH,
            override_model_path=override_model_path,
        )
    args.func(args)


if __name__ == "__main__":
    main()
