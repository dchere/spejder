"""API and port-probe tests for GUI server shutdown."""

import errno
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from spejder.config import AppConfig
from spejder.db import ensure_db
from spejder.server import create_app
from spejder.server.app import _select_listen_port, start_server


def _minimal_app(**kwargs):
    defaults = dict(
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=lambda reason="": None,
        cli_verbose=False,
    )
    defaults.update(kwargs)
    return create_app(**defaults)


def _minimal_factory_kwargs(db_path, report_dir, **kwargs):
    defaults = dict(
        db_path=db_path,
        profile_path=os.path.join(report_dir, "profile.json"),
        runtime_profile=AppConfig(),
        model_path="",
        report_dir=report_dir,
        get_title_translation_llm=lambda: None,
        persist_runtime_profile=lambda: None,
        reload_runtime_profile=lambda: None,
        queue_dashboard_rebuild=lambda reason="": None,
        cli_verbose=False,
    )
    defaults.update(kwargs)
    return defaults


class SelectListenPortTest(unittest.TestCase):
    def test_retries_eaddrinuse_then_succeeds(self):
        sock = MagicMock()
        calls = {"n": 0}

        def bind(_addr):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError(errno.EADDRINUSE, "Address already in use")

        sock.bind.side_effect = bind
        sock.__enter__ = lambda self: sock
        sock.__exit__ = lambda *args: None

        with patch("spejder.server.app.socket.socket", return_value=sock):
            selected = _select_listen_port("127.0.0.1", 8765, max_port_attempts=20)

        self.assertEqual(selected, 8766)
        self.assertEqual(calls["n"], 2)

    def test_exhausted_ports_raise_aggregate_message(self):
        sock = MagicMock()
        sock.bind.side_effect = OSError(errno.EADDRINUSE, "Address already in use")
        sock.__enter__ = lambda self: sock
        sock.__exit__ = lambda *args: None

        with patch("spejder.server.app.socket.socket", return_value=sock):
            with self.assertRaises(OSError) as ctx:
                _select_listen_port("127.0.0.1", 8765, max_port_attempts=2)

        self.assertEqual(
            str(ctx.exception),
            "Address in use for all tried ports: 8765-8767",
        )
        self.assertEqual(sock.bind.call_count, 3)

    def test_non_eaddrinuse_is_preserved(self):
        sock = MagicMock()
        original = OSError(errno.EACCES, "Permission denied")
        sock.bind.side_effect = original
        sock.__enter__ = lambda self: sock
        sock.__exit__ = lambda *args: None

        with patch("spejder.server.app.socket.socket", return_value=sock):
            with self.assertRaises(OSError) as ctx:
                _select_listen_port("127.0.0.1", 8765)

        self.assertIs(ctx.exception, original)


class ServerShutdownApiTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_shutdown_schedules_once_and_returns_ok(self):
        scheduled = []

        def request_shutdown():
            scheduled.append(True)

        app = _minimal_app(
            db_path=self.db_path,
            profile_path=os.path.join(self.report_dir, "profile.json"),
            runtime_profile=AppConfig(),
            model_path="",
            report_dir=self.report_dir,
            request_shutdown=request_shutdown,
        )
        client = TestClient(app)
        response = client.post("/api/shutdown")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"ok": True})
        self.assertEqual(scheduled, [True])

    def test_shutdown_unwired_returns_503(self):
        app = _minimal_app(
            db_path=self.db_path,
            profile_path=os.path.join(self.report_dir, "profile.json"),
            runtime_profile=AppConfig(),
            model_path="",
            report_dir=self.report_dir,
        )
        client = TestClient(app)
        response = client.post("/api/shutdown")
        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["ok"])
        self.assertEqual(response.json()["error"], "shutdown not available")


class StartServerShutdownHookTest(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmpdir.name, "jobs.db")
        self.report_dir = os.path.join(self._tmpdir.name, "outbox")
        os.makedirs(self.report_dir, exist_ok=True)
        ensure_db(self.db_path)

    def tearDown(self):
        self._tmpdir.cleanup()

    def test_start_server_timer_sets_should_exit_and_prints(self):
        apps = []
        real_create_app = create_app

        def capture_create_app(**kwargs):
            app = real_create_app(**kwargs)
            apps.append(app)
            return app

        mock_server = MagicMock()
        mock_server.should_exit = False
        pending_timers = []

        def fake_timer(interval, function, *args, **kwargs):
            timer = MagicMock()
            timer.daemon = False

            def start():
                pending_timers.append((interval, function, timer))

            timer.start = start
            return timer

        with patch("spejder.server.app.create_app", side_effect=capture_create_app), \
                patch("spejder.server.app._select_listen_port", return_value=8765), \
                patch("uvicorn.Config") as mock_config_cls, \
                patch("uvicorn.Server", return_value=mock_server) as mock_server_cls, \
                patch("spejder.server.app.threading.Timer", side_effect=fake_timer):
            with redirect_stdout(io.StringIO()) as startup_out:
                start_server(
                    "127.0.0.1",
                    8765,
                    _minimal_factory_kwargs(self.db_path, self.report_dir),
                )

            self.assertEqual(len(apps), 1)
            mock_config_cls.assert_called_once()
            mock_server_cls.assert_called_once()
            mock_server.run.assert_called_once()
            self.assertIn(
                "Serving GUI at http://127.0.0.1:8765/report.html",
                startup_out.getvalue(),
            )

            request_shutdown = apps[0].state.runtime.request_shutdown
            self.assertIsNotNone(request_shutdown)

            with redirect_stdout(io.StringIO()) as shutdown_out:
                request_shutdown()
                self.assertEqual(len(pending_timers), 1)
                interval, exit_fn, timer = pending_timers[0]
                self.assertEqual(interval, 0.25)
                self.assertTrue(timer.daemon)
                self.assertFalse(mock_server.should_exit)
                exit_fn()

            self.assertTrue(mock_server.should_exit)
            self.assertIn(
                "Shutting down Spejder GUI server.",
                shutdown_out.getvalue(),
            )


if __name__ == "__main__":
    unittest.main()
