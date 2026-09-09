#!/usr/bin/env python3
"""Compile the sampler and check synthetic cases plus an owned process tree."""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent


def records(result):
    return [json.loads(line) for line in result.stdout.splitlines()]


def check_wire(rows):
    assert rows and all(row["version"] == 1 for row in rows)
    for row in rows:
        if row["event"] != "process":
            continue
        assert row["start_ticks"] > 0
        assert "rss_bytes" in row
        assert "physical_footprint_bytes" not in row
        assert "total_cpu_ns" not in row


def main():
    if sys.platform != "darwin":
        raise SystemExit("These checks require Darwin.")
    evidence = pathlib.Path(tempfile.mkdtemp(prefix="measure-processes-evidence-"))
    print(f"Raw evidence: {evidence}", flush=True)
    with tempfile.TemporaryDirectory(prefix="measure-processes-test-") as directory:
        binary = str(pathlib.Path(directory) / "measure-processes")
        unit = str(pathlib.Path(directory) / "unit")
        native = str(pathlib.Path(directory) / "native")
        flags = ["clang", "-std=c11", "-Wall", "-Wextra", "-Werror", "-O1", "-lproc"]
        for source, output in [(ROOT / "measure-processes.c", binary),
                               (HERE / "unit.c", unit), (HERE / "native.c", native)]:
            compiled = subprocess.run([*flags, str(source), "-o", output],
                                      capture_output=True, text=True, timeout=30)
            (evidence / f"{source.stem}-compile.log").write_text(compiled.stdout + compiled.stderr)
            compiled.check_returncode()
        result = subprocess.run([unit], capture_output=True, text=True, timeout=10)
        (evidence / "unit.jsonl").write_text(result.stdout)
        (evidence / "unit.stderr").write_text(result.stderr)
        result.check_returncode()
        synthetic = records(result)
        check_wire(synthetic)
        events = {row["event"] for row in synthetic}
        assert {"observed_birth", "observed_exit", "observed_reparenting",
                "observed_disappearance", "unresolved_reaped_child_accounting",
                "sampling_error"} <= events
        assert any(row.get("operation") == "identity_changed_during_read" for row in synthetic)
        assert any(row.get("operation") == "owned_root_not_live" for row in synthetic)
        native_result = subprocess.run([native], capture_output=True, text=True, timeout=10)
        (evidence / "native-cpu.jsonl").write_text(native_result.stdout)
        (evidence / "native-cpu.stderr").write_text(native_result.stderr)
        native_result.check_returncode()
        assert records(native_result)[0]["event"] == "native_cpu_units"
        for arguments in [[], ["--owned-root", "0:bad"],
                          ["--owned-root", "1:bad", "--interval-ms", "-1", "--samples", "1"]]:
            invalid = subprocess.run([binary, *arguments], capture_output=True, timeout=10)
            assert invalid.returncode == 2 and b"Usage:" in invalid.stderr

        # The test owns these processes. Pipe closure ends them without signals.
        fixture = """
import os, subprocess, sys
child = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read()'], stdin=subprocess.PIPE)
print(os.getpid(), child.pid, flush=True)
sys.stdin.buffer.read()
child.stdin.close()
child.wait(timeout=10)
"""
        owned = subprocess.Popen([sys.executable, "-u", "-c", fixture], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, text=True)
        try:
            root_pid, child_pid = map(int, owned.stdout.readline().split())
            sampled = subprocess.run([binary, "--owned-root", f"{root_pid}:owned-fixture",
                                      "--interval-ms", "10", "--samples", "2"],
                                     capture_output=True, text=True, timeout=30)
            (evidence / "owned-tree.jsonl").write_text(sampled.stdout)
            (evidence / "owned-tree.stderr").write_text(sampled.stderr)
            (evidence / "owned-tree-result.json").write_text(json.dumps({
                "root_pid": root_pid, "child_pid": child_pid,
                "sampler_exit_code": sampled.returncode,
            }) + "\n")
            assert sampled.returncode == 0, (sampled.stderr, sampled.stdout)
            live = records(sampled)
            check_wire(live)
            config = live[0]
            assert config["event"] == "configuration"
            assert config["timebase_numer"] > 0 and config["timebase_denom"] > 0
            assert config["interval_ns"] == 10_000_000
            assert config["cpu_raw_unit"] == "mach_ticks"
            samples = [row for row in live if row["event"] == "sample_end"]
            assert len(samples) == 2
            for index in (0, 1):
                processes = [row for row in live if row["event"] == "process" and row["sample"] == index]
                assert {root_pid, child_pid} <= {row["pid"] for row in processes}
                for row in processes:
                    assert row["role"] == "owned-fixture" and row["root_pid"] == root_pid
                    for name in ("own_user", "own_system", "reaped_child_user", "reaped_child_system"):
                        assert row[f"{name}_ns"] == row[f"{name}_ticks"] * config["timebase_numer"] // config["timebase_denom"]
            assert owned.poll() is None
            assert live[-1]["lifecycle_accounting_valid"] is False
            assert live[-1]["observations_valid"] is True
            assert live[-1]["unresolved_turnover"] is False
            assert not any(row["event"] == "sampling_error" for row in live)
            print(f"Owned-tree check: {len(samples)} valid scoped samples; ancestry discovery complete: {live[-1]['ancestry_discovery_complete']}.")
        finally:
            owned.stdin.close()
            owned.wait(timeout=15)
            owned.stdout.close()
    print("All sampler checks passed. No benchmark ran.")


if __name__ == "__main__":
    main()
