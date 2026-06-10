"""Smoke tests for workspace path resolution and CLI init map."""
import argparse
import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest import mock

from spejder.cli import COMMAND_INIT, _profile_path_from_args, _resolve_args_paths, main
from spejder.core import resolve_user_path, workspace_root


def _write_profile(tmpdir):
    profile = os.path.join(tmpdir, "profile.json")
    with open(profile, "w", encoding="utf-8") as f:
        f.write("{}")
    return profile


def _run_main_with_mocks(argv, tmpdir, extra_patches=None):
    _write_profile(tmpdir)
    with ExitStack() as stack:
        stack.enter_context(mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": tmpdir}, clear=False))
        stack.enter_context(mock.patch("spejder.cli.initialize_language_checker_or_exit"))
        stack.enter_context(mock.patch("spejder.cli.initialize_translation_or_exit"))
        mock_llm = stack.enter_context(
            mock.patch("spejder.cli.initialize_llm_or_exit", return_value=object())
        )
        for target, kwargs in extra_patches or []:
            stack.enter_context(mock.patch(target, **kwargs))
        main(argv)
        return mock_llm


class ResolveUserPathTests(unittest.TestCase):
    def test_absolute_path_unchanged(self):
        self.assertEqual(resolve_user_path("/tmp/profile.json"), "/tmp/profile.json")

    def test_relative_path_uses_workspace(self):
        with mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "/ws"}, clear=False):
            self.assertEqual(
                resolve_user_path("./profile.json"),
                os.path.normpath("/ws/profile.json"),
            )

    def test_workspace_root_from_env(self):
        with mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "/ws"}, clear=False):
            self.assertEqual(workspace_root(), "/ws")

    def test_workspace_root_relative_env_uses_cwd(self):
        patches = (
            mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "./sub"}, clear=False),
            mock.patch("os.getcwd", return_value="/cwd"),
        )
        with ExitStack() as stack:
            for patch in patches:
                stack.enter_context(patch)
            self.assertEqual(workspace_root(), os.path.normpath("/cwd/sub"))


class CommandInitTests(unittest.TestCase):
    def test_dedupe_jobs_not_in_init_map(self):
        self.assertNotIn("dedupe_jobs", COMMAND_INIT)

    def test_sync_user_skills_requires_llm(self):
        self.assertIn("llm", COMMAND_INIT["sync_user_skills"])

    def test_serve_gui_requires_translation_not_llm(self):
        required = COMMAND_INIT["serve_gui"]
        self.assertIn("language_checker", required)
        self.assertIn("translation", required)
        self.assertNotIn("llm", required)


class ProfilePathFromArgsTests(unittest.TestCase):
    def test_uses_profile_when_set(self):
        args = argparse.Namespace(profile="/resolved/profile.json", path="./inbox/a.eml")
        self.assertEqual(_profile_path_from_args("summarize_file", args), "/resolved/profile.json")

    def test_init_profile_uses_path_not_eml_semantics(self):
        args = argparse.Namespace(path="/resolved/new-profile.json")
        self.assertEqual(_profile_path_from_args("init_profile", args), "/resolved/new-profile.json")

    def test_default_profile_when_no_args(self):
        with mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "/ws"}, clear=False):
            expected = os.path.normpath("/ws/profile.json")
            self.assertEqual(_profile_path_from_args("dedupe_jobs", argparse.Namespace()), expected)


class ResolveArgsPathsTests(unittest.TestCase):
    def test_resolves_db_and_cv(self):
        with mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "/ws"}, clear=False):
            args = argparse.Namespace(db="./jobs.db", cv="./CV")
            _resolve_args_paths(args)
            self.assertEqual(args.db, os.path.normpath("/ws/jobs.db"))
            self.assertEqual(args.cv, os.path.normpath("/ws/CV"))

    def test_resolves_report_links_folder(self):
        with mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": "/ws"}, clear=False):
            args = argparse.Namespace(folder="./inbox")
            _resolve_args_paths(args)
            self.assertEqual(args.folder, os.path.normpath("/ws/inbox"))


class CliMainInitTests(unittest.TestCase):
    def test_sync_user_skills_sets_llm_on_args(self):
        fake_llm = object()
        captured = []

        def capture_sync(args):
            captured.append(args)

        with tempfile.TemporaryDirectory() as tmp:
            _write_profile(tmp)
            with ExitStack() as stack:
                stack.enter_context(mock.patch.dict(os.environ, {"SPEJDER_WORKSPACE": tmp}, clear=False))
                stack.enter_context(mock.patch("spejder.cli.initialize_language_checker_or_exit"))
                stack.enter_context(mock.patch("spejder.cli.initialize_translation_or_exit"))
                stack.enter_context(mock.patch("spejder.cli.initialize_llm_or_exit", return_value=fake_llm))
                stack.enter_context(mock.patch("spejder.cli.cmd_sync_user_skills", side_effect=capture_sync))
                main(["sync-user-skills", "--profile", "./profile.json"])

            self.assertEqual(len(captured), 1)
            self.assertIs(captured[0]._llm, fake_llm)

    def test_serve_gui_runs_translation_init_not_llm(self):
        captured = []

        def capture_gui(args):
            captured.append(args)

        with tempfile.TemporaryDirectory() as tmp:
            mock_llm = _run_main_with_mocks(
                ["serve-gui", "--profile", "./profile.json", "--no-open"],
                tmp,
                extra_patches=[("spejder.cli.cmd_serve_gui", {"side_effect": capture_gui})],
            )
            mock_llm.assert_not_called()
            self.assertEqual(len(captured), 1)

    def test_process_inbox_does_not_set_llm_on_args(self):
        captured = []

        def capture_inbox(args):
            captured.append(args)

        with tempfile.TemporaryDirectory() as tmp:
            mock_llm = _run_main_with_mocks(
                ["process-inbox", "--profile", "./profile.json"],
                tmp,
                extra_patches=[("spejder.cli.cmd_process_inbox", {"side_effect": capture_inbox})],
            )
            self.assertIsNotNone(mock_llm)
            self.assertEqual(len(captured), 1)
            self.assertFalse(hasattr(captured[0], "_llm"))

    def test_sync_user_skills_verbose_when_not_quiet(self):
        with tempfile.TemporaryDirectory() as tmp:
            mock_llm = _run_main_with_mocks(
                ["sync-user-skills", "--profile", "./profile.json"],
                tmp,
                extra_patches=[("spejder.cli.cmd_sync_user_skills", {})],
            )
            self.assertTrue(mock_llm.call_args.kwargs.get("verbose"))

    def test_sync_user_skills_quiet_model_disables_verbose(self):
        with tempfile.TemporaryDirectory() as tmp:
            mock_llm = _run_main_with_mocks(
                ["sync-user-skills", "--profile", "./profile.json", "--quiet-model"],
                tmp,
                extra_patches=[("spejder.cli.cmd_sync_user_skills", {})],
            )
            self.assertFalse(mock_llm.call_args.kwargs.get("verbose"))

    def test_empty_profile_exits(self):
        with self.assertRaises(SystemExit):
            main(["dedupe-jobs", "--profile", ""])


if __name__ == "__main__":
    unittest.main()
