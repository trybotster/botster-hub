#!/usr/bin/env python3
"""Check adapter process handling without starting a Hub or a client harness.

Run with python3 script/test-harness-control.py.
Run Rust transport tests separately with:
cargo test -p botster-hub-client --example harness_control
"""

import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SHARED = runpy.run_path(str(ROOT / "script/prove-north-star-shared-session"))
SEND = SHARED["send_daemon_request"]
RUBY_PROBE = ROOT / "script/probe-hub-resources"
FAKE = '''import json, os, sys, time
from pathlib import Path
Path(os.environ["FAKE_PID"]).write_text(str(os.getpid()))
behavior = os.environ.get("FAKE_BEHAVIOR", "ok")
if behavior == "fail":
    print("fixture adapter failure", file=sys.stderr)
    sys.exit(7)
if behavior == "hang_ready":
    time.sleep(60)
if sys.argv[1] == "connection":
    print('{"ready":true}', flush=True)
for line in sys.stdin:
    with open(os.environ["FAKE_REQUESTS"], "a") as output:
        output.write(line)
    if behavior == "hang":
        time.sleep(60)
    if behavior == "invalid":
        print("not json", flush=True)
    else:
        print(json.dumps({"kind":"operator_error", "error":{"code":"fixture_refusal"}}), flush=True)
    if sys.argv[1] == "request":
        break
'''


class HarnessProcessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.adapter = self.root / "adapter with spaces"
        self.adapter.write_text(f"#!{sys.executable}\n" + FAKE)
        self.adapter.chmod(0o700)
        self.pid = self.root / "pid"
        self.requests = self.root / "requests"
        self.environment = {
            "BOTSTER_HUB_CLIENT_ADAPTER_BIN": str(self.adapter),
            "FAKE_PID": str(self.pid),
            "FAKE_REQUESTS": str(self.requests),
            "FAKE_BEHAVIOR": "ok",
        }
        self.env_patch = patch.dict(os.environ, self.environment)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    def assert_child_reaped(self):
        pid = int(self.pid.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def ruby(self, source):
        return subprocess.run(
            ["ruby", "-e", "load ARGV.fetch(0)\n" + source, str(RUBY_PROBE)],
            text=True, capture_output=True, timeout=8,
        )

    def test_one_shot_preserves_request_and_daemon_error(self):
        request = {"type": "spawn_session_type", "session_type_id": "device/test",
                   "session_id": "kept", "request": {"target_id": "target"}}
        result = SEND(Path("/unused socket"), request)
        self.assertEqual(result["error"]["code"], "fixture_refusal")
        self.assertEqual(json.loads(self.requests.read_text()), request)
        self.assert_child_reaped()

    def test_missing_adapter_is_explicit(self):
        os.environ.pop("BOTSTER_HUB_CLIENT_ADAPTER_BIN")
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors), self.assertRaises(SystemExit):
            SEND(Path("/unused"), {"type": "status"})
        self.assertIn("missing required env BOTSTER_HUB_CLIENT_ADAPTER_BIN", errors.getvalue())
        result = self.ruby("ControlAdapter.executable")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("BOTSTER_HUB_CLIENT_ADAPTER_BIN must name an executable file", result.stderr)

    def test_one_shot_adapter_failure_is_not_a_daemon_response(self):
        os.environ["FAKE_BEHAVIOR"] = "fail"
        errors = io.StringIO()
        with contextlib.redirect_stderr(errors), self.assertRaises(SystemExit):
            SEND(Path("/unused"), {"type": "status"})
        self.assertIn("code=7", errors.getvalue())
        self.assertIn("fixture adapter failure", errors.getvalue())
        self.assert_child_reaped()

    def test_one_shot_timeout_kills_and_reaps_child(self):
        os.environ["FAKE_BEHAVIOR"] = "hang"
        run = subprocess.run

        def shortened(*args, **kwargs):
            self.assertEqual(kwargs["timeout"], 30)
            kwargs["timeout"] = 0.3
            return run(*args, **kwargs)

        with patch.object(subprocess, "run", side_effect=shortened):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                SEND(Path("/unused"), {"type": "status"})
        self.assert_child_reaped()

    def test_persistent_process_handles_two_requests_and_daemon_errors(self):
        result = self.ruby('''
adapter = ControlAdapter.new("/unused", 1)
2.times do
  response = adapter.request({type: "status"}, 1)
  raise "daemon error was lost" unless response.fetch("error").fetch("code") == "fixture_refusal"
end
adapter.close
''')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.requests.read_text().splitlines()), 2)
        self.assert_child_reaped()

    def test_persistent_error_includes_adapter_stderr(self):
        os.environ["FAKE_BEHAVIOR"] = "fail"
        result = self.ruby('ControlAdapter.new("/unused", 1)')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fixture adapter failure", result.stderr)
        self.assert_child_reaped()

    def test_persistent_timeout_cleans_up_connection_and_request(self):
        for behavior in ["hang_ready", "hang"]:
            with self.subTest(behavior=behavior):
                os.environ["FAKE_BEHAVIOR"] = behavior
                result = self.ruby('''
adapter = ControlAdapter.new("/unused", 0.2)
adapter.request({type: "status"}, 0.2)
''')
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Timeout::Error", result.stderr)
                self.assert_child_reaped()

    def test_exit_cleans_up_open_persistent_process(self):
        result = self.ruby('ControlAdapter.new("/unused", 1); abort("fixture exit")')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fixture exit", result.stderr)
        self.assert_child_reaped()

    def test_invalid_response_cleans_up_both_modes(self):
        os.environ["FAKE_BEHAVIOR"] = "invalid"
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            SEND(Path("/unused"), {"type": "status"})
        self.assert_child_reaped()
        result = self.ruby('ControlAdapter.new("/unused", 1).request({type: "status"}, 1)')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("JSON::ParserError", result.stderr)
        self.assert_child_reaped()


if __name__ == "__main__":
    unittest.main()
