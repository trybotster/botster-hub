"""Check stable intervals and a native fork that exits between sample reads."""
import json
import os
import select
import subprocess
import time


class Lines:
    def __init__(self, stream):
        self.stream = stream
        self.pending = b""

    def next(self, timeout=10):
        deadline = time.monotonic() + timeout
        while b"\n" not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.stream], [], [], remaining)[0]:
                raise TimeoutError("The fixture did not produce its next line.")
            chunk = os.read(self.stream.fileno(), 65536)
            if not chunk:
                assert not self.pending, "Output ended with an incomplete JSONL record."
                return None
            self.pending += chunk
        line, self.pending = self.pending.split(b"\n", 1)
        return line.decode()


def check_case(binary, fixture, evidence, name, hidden_fork=False, setup_attempts=4):
    owned = subprocess.Popen([fixture], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    owned_lines = Lines(owned.stdout)
    sampler = None
    captured = []
    try:
        ready = owned_lines.next().split()
        assert ready[0] == "ready"
        root_pid, child_pid = map(int, ready[1:])
        arguments = [binary, "--owned-root", f"{root_pid}:stable-fixture",
                     "--interval-ms", "200", "--samples", "2", "--stable-baseline",
                     "--setup-attempts", str(setup_attempts)]
        with (evidence / f"{name}.stderr").open("w") as errors:
            sampler = subprocess.Popen(arguments, stdout=subprocess.PIPE, stderr=errors)
            output = Lines(sampler.stdout)
            fork_times = None
            while (line := output.next()) is not None:
                captured.append(line)
                row = json.loads(line)
                if hidden_fork and row["event"] == "stable_sample_complete" and row["sample"] == 0:
                    owned.stdin.write(b"f")
                    owned.stdin.flush()
                    reply = owned_lines.next().split()
                    assert reply[0] == "fork_exit"
                    transient_pid, fork_begin, fork_end = map(int, reply[1:])
                    fork_times = (transient_pid, fork_begin, fork_end)
            code = sampler.wait(timeout=10)
        assert owned.poll() is None
        rows = [json.loads(line) for line in captured]
        assert all(row["version"] == 1 for row in rows)
        summary = rows[-1]
        assert summary["event"] == "stable_summary"
        assert summary["historical_descendants_complete"] is False
        summaries = [row for row in rows if row["event"] == "stable_sample_complete"]
        process_rows = [row for row in rows if row["event"] == "process"]
        if setup_attempts == 1:
            assert code == 2 and summary["interval_valid"] is False
            assert any(row.get("operation") == "unable_to_stabilize" for row in rows)
            assert not process_rows
        else:
            assert len(summaries) == 2
            for index in (0, 1):
                sampled = [row for row in process_rows if row["sample"] == index]
                assert len(sampled) == 2
                assert {row["pid"] for row in sampled} == {root_pid, child_pid}
        if hidden_fork:
            assert code == 2 and summary["interval_valid"] is False
            assert fork_times is not None
            transient_pid, fork_begin, fork_end = fork_times
            assert summaries[0]["read_end_ticks"] <= fork_begin <= fork_end <= summaries[1]["read_begin_ticks"]
            assert transient_pid not in {row["pid"] for row in process_rows}
            # NOTE_FORK is required even when no child survives to a poll.
            assert any(row["event"] == "lifecycle_event" and row["baseline_active"]
                       and row["pid"] == root_pid and row["fflags"] & 0x40000000 for row in rows)
        elif setup_attempts != 1:
            assert code == 0 and summary["interval_valid"] is True
            assert summary["participants"] == 2
            assert summary["final_drain_ticks"] >= summaries[-1]["read_end_ticks"]
            assert len([row for row in rows if row["event"] == "stable_own_cpu_delta"]) == 2
            rss = [row for row in rows if row["event"] == "stable_rss_sample"]
            assert len(rss) == 2 and all(row["rss_sum_bytes"] > 0 for row in rss)
            by_sample = [{row["pid"]: row for row in process_rows if row["sample"] == index}
                         for index in (0, 1)]
            own_ticks = sum(by_sample[1][pid][name] - by_sample[0][pid][name]
                            for pid in (root_pid, child_pid)
                            for name in ("own_user_ticks", "own_system_ticks"))
            assert summary["own_cpu_delta_ticks"] == own_ticks
            assert summary["own_cpu_delta_ns"] == own_ticks * rows[0]["timebase_numer"] // rows[0]["timebase_denom"]
            for index, result in enumerate(rss):
                assert result["rss_sum_bytes"] == sum(row["rss_bytes"] for row in by_sample[index].values())
                assert result["atomic"] is False and result["peak"] is False
            assert not any(row["event"] == "lifecycle_event" and row["baseline_active"] for row in rows)
        if code == 2:
            assert summary["own_cpu_delta_ticks"] is None and summary["own_cpu_delta_ns"] is None
            assert not any(row["event"] in ("stable_own_cpu_delta", "stable_rss_sample") for row in rows)
        (evidence / f"{name}-result.json").write_text(json.dumps({
            "root_pid": root_pid, "child_pid": child_pid, "sampler_exit_code": code,
            "hidden_fork": fork_times, "interval_valid": summary["interval_valid"],
        }) + "\n")
        print(f"{name}: passed; interval_valid={summary['interval_valid']}; exit={code}.")
    finally:
        (evidence / f"{name}.jsonl").write_text("\n".join(captured) + "\n")
        owned.stdin.close()
        owned.wait(timeout=10)
        owned.stdout.close()
        if sampler is not None:
            sampler.wait(timeout=10)
            sampler.stdout.close()


def run(binary, fixture, evidence):
    check_case(binary, fixture, evidence, "stable-owned")
    check_case(binary, fixture, evidence, "stable-hidden-fork", hidden_fork=True)
    check_case(binary, fixture, evidence, "stable-setup-limit", setup_attempts=1)
