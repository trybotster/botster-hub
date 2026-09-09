#!/usr/bin/env python3
"""Test development artifact selection with a fake Cargo executable.

Run with python3 script/test-build-dev-artifacts.py. This test does not compile
code or start services. It checks recorded adapter bytes independently; the
existing two-binary manifest verifiers do not check the adapter.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


BUILDER = Path(__file__).resolve().with_name("build-dev-artifacts")
CORE_REVISION = "a" * 40
HUB_COMMAND = ["build", "--locked", "-p", "botster-hub", "--bin", "botster-hub"]
WORKER_COMMAND = ["build", "--locked", "-p", "botster-core-daemon", "--bin", "botster-session-worker"]
ADAPTER_COMMAND = ["build", "--locked", "-p", "botster-hub-client", "--example", "harness_control"]
FAKE_CARGO = '''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
record = {"args": args, "cwd": os.getcwd(),
          "revision": os.environ.get("BOTSTER_BUILD_REVISION"),
          "jobs": os.environ.get("CARGO_BUILD_JOBS"),
          "incremental": os.environ.get("CARGO_INCREMENTAL")}
with open(os.environ["BUILD_COMMAND_LOG"], "a") as log:
    log.write(json.dumps(record) + "\\n")
example = "--example" in args
if example and os.environ.get("FAIL_ADAPTER") == "1":
    print("fixture adapter build failed", file=sys.stderr)
    sys.exit(17)
if example and os.environ.get("OMIT_ADAPTER") == "1":
    sys.exit(0)
target = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "debug"
if example:
    target /= "examples"
target.mkdir(parents=True, exist_ok=True)
artifact = target / args[-1]
artifact.write_bytes((json.dumps(record, sort_keys=True) + "\\n").encode())
artifact.chmod(0o755)
'''


class BuilderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="builder-fixture-")
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.repo = self.root / "Hub checkout"
        (self.repo / "script").mkdir(parents=True)
        self.builder = self.repo / "script/build-dev-artifacts"
        shutil.copy2(BUILDER, self.builder)
        (self.repo / "Cargo.lock").write_text(
            'version = 4\n[[package]]\nname = "botster-core"\nversion = "0.0.0"\n'
            f'source = "git+https://example.invalid/core#{CORE_REVISION}"\n'
        )
        self.bin_dir = self.root / "fake tools"
        self.bin_dir.mkdir()
        cargo = self.bin_dir / "cargo"
        cargo.write_text(f"#!{sys.executable}\n" + FAKE_CARGO)
        cargo.chmod(0o755)
        self.log = self.root / "commands.jsonl"
        self.output = self.root / "candidate output"
        self.env = os.environ.copy()
        for name in ["CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "BOTSTER_BUILD_REVISION",
                     "FAIL_ADAPTER", "OMIT_ADAPTER", "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE"]:
            self.env.pop(name, None)
        self.env.update({
            "PATH": str(self.bin_dir) + os.pathsep + os.environ["PATH"],
            "BUILD_COMMAND_LOG": str(self.log),
            "GIT_AUTHOR_NAME": "Builder Fixture", "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_NAME": "Builder Fixture", "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        })
        self.git("init", "--quiet")
        self.git("add", ".")
        self.git("-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "-m", "fixture")
        self.revision = self.git("rev-parse", "HEAD").strip()

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, env=self.env, text=True)

    def build(self, *args):
        return subprocess.run(
            ["sh", str(self.builder), "--out-dir", str(self.output), *args],
            cwd=self.root, env=self.env, text=True, capture_output=True, timeout=15,
        )

    def commands(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def check_candidate(self, result, adapter):
        self.assertEqual(result.returncode, 0, result.stderr)
        exports = dict(line.split("=", 1) for line in result.stdout.splitlines())
        expected = {
            "BOTSTER_HUB_BIN": str(self.output / "botster-hub"),
            "BOTSTER_SESSION_WORKER_BIN": str(self.output / "botster-session-worker"),
            "BOTSTER_CANDIDATE_MANIFEST": str(self.output / "install-manifest.json"),
        }
        names = ["botster-hub", "botster-session-worker"]
        if adapter:
            expected["BOTSTER_HUB_CLIENT_ADAPTER_BIN"] = str(self.output / "harness_control")
            names.append("harness_control")
        self.assertEqual(exports, expected)
        manifest = json.loads(Path(exports["BOTSTER_CANDIDATE_MANIFEST"]).read_text())
        self.assertEqual(manifest["source_revisions"], {
            "botster_hub": self.revision, "botster_core": CORE_REVISION,
        })
        self.assertEqual([item["name"] for item in manifest["artifacts"]], names)
        for item in manifest["artifacts"]:
            artifact = self.output / item["name"]
            contents = artifact.read_bytes()
            self.assertEqual(item["size"], len(contents))
            self.assertEqual(item["sha256"], hashlib.sha256(contents).hexdigest())
            self.assertTrue(os.access(artifact, os.X_OK))
        return manifest

    def test_default_keeps_two_builds_and_three_exports(self):
        self.check_candidate(self.build(), adapter=False)
        commands = self.commands()
        self.assertEqual([row["args"] for row in commands], [HUB_COMMAND, WORKER_COMMAND])
        self.assertEqual(commands[0]["revision"], self.revision)
        self.assertIsNone(commands[1]["revision"])
        self.assertTrue(all(row["jobs"] == "2" and row["incremental"] == "0" for row in commands))
        self.assertFalse((self.output / "harness_control").exists())

    def test_optional_adapter_records_same_source_and_exact_bytes(self):
        self.env["CARGO_TARGET_DIR"] = str(self.root / "separate target directory")
        self.env["CARGO_BUILD_JOBS"] = "1"
        manifest = self.check_candidate(self.build("--with-harness-adapter"), adapter=True)
        commands = self.commands()
        self.assertEqual([row["args"] for row in commands], [HUB_COMMAND, WORKER_COMMAND, ADAPTER_COMMAND])
        self.assertEqual(commands[2]["revision"], manifest["source_revisions"]["botster_hub"])
        self.assertTrue(all(Path(row["cwd"]).resolve() == self.repo.resolve() for row in commands))
        self.assertTrue(all(row["jobs"] == "1" and row["incremental"] == "0" for row in commands))

    def test_adapter_build_failure_emits_no_candidate_exports(self):
        self.env["FAIL_ADAPTER"] = "1"
        result = self.build("--with-harness-adapter")
        self.assertEqual(result.returncode, 17)
        self.assertIn("fixture adapter build failed", result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertFalse((self.output / "install-manifest.json").exists())

    def test_missing_adapter_binary_emits_no_manifest(self):
        self.env["OMIT_ADAPTER"] = "1"
        result = self.build("--with-harness-adapter")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertFalse((self.output / "install-manifest.json").exists())

    def test_dirty_checkout_rejects_both_modes_before_build(self):
        self.builder.write_text(self.builder.read_text() + "\n# fixture edit\n")
        for args in [[], ["--with-harness-adapter"]]:
            with self.subTest(args=args):
                result = self.build(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("dirty checkout", result.stderr)
                self.assertFalse(self.log.exists())

    def test_unknown_option_does_not_build(self):
        result = self.build("--with-harness-adapters")
        self.assertEqual(result.returncode, 2)
        self.assertFalse(self.log.exists())

    def test_help_states_optional_verification_limit(self):
        result = self.build("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("two-binary manifest verifiers do not verify the optional adapter", result.stderr)
        self.assertFalse(self.log.exists())


if __name__ == "__main__":
    unittest.main()
