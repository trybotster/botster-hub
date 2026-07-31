# Hub resource-bound proof

The Hub resource regression has two complementary carriers:

- a normal Rust integration test with four deterministic plugin owners, suitable
  for CI; and
- the exact-coordinate production package campaign, which installs only
  `botster-web`, `botster-tui`, `botster-workspaces`, and `project-pipelines`.

Both use the public `PluginLifecycleStatus` daemon request. Core remains the
authoritative producer for plugin queue/executor counters. Hub adds only the
sanitized aggregate `active_timer_resources` observation.

## Committed invariants

- queue capacity is `256` and executor concurrency is `2`;
- four loaded owners materialize exactly eight executor workers, never 1,024;
- the Hub process has at most 64 OS threads;
- queued jobs, in-flight jobs, entity subscriptions, terminal attaches, and
  timer resources return to zero;
- reload returns to the four-owner baseline;
- public disable retires executors and workers stepwise to zero; and
- orderly down leaves no campaign-owned Hub, session worker, zombie, or socket.

Run the deterministic regression locally:

```sh
./test.sh --test hub_daemon_lifecycle_test \
  focused_plugin_resources_are_bounded_across_reconnect_reload_idle_and_unload \
  -- --exact --nocapture
```

The loaded workflow exposes `focused-plugin-resource-bounds`. It requires
`stress_profile=none` and sets `BOTSTER_ASSERT_IDLE_CPU_BOUND=1`. On Linux that
selector records Hub process CPU ticks for five seconds after deterministic
counter convergence and enforces at most 250 ms growth. Ordinary full-suite
runs record the same observation but do not use it as a pass/fail gate.

## Exact four-package campaign

Run `script/test-production-package-runtime` with the exact source revisions
and a new evidence directory as documented in the README. The fresh leg invokes
`script/probe-hub-resources` against the caller-owned Hub before churn, during
an eight-reconnect churn phase, once for each of eight additional reconnect
generations, after public package reload, after idle settle, and after each
public package disable. Every probe waits for authoritative counters to
converge, asserts reconnect/cleanup deltas and stable idle delivery counters,
and emits one bounded JSON object per phase. The campaign compares the eight
generation snapshots so retained live resources cannot grow. Evidence
redaction and PII verification remain owned by the existing production evidence
helper.

`botster-tui-kit` remains an exact build/test source input. It is not installed
and does not count as a fifth package.

## Live macOS and Linux census

Resolve the Hub PID and socket from the campaign-owned
`.botster-hub-runtime-daemon.json`, then run:

```sh
script/probe-hub-resources \
  --socket "$hub_socket" --hub-pid "$hub_pid" \
  --phase operator-idle --expected-owners 4
```

For corroborating OS evidence, use universal process fields:

```sh
ps -axo pid=,ppid=,pgid=,sid=,stat=,time=,command=
lsof -nP -p "$hub_pid"
```

On Linux, `/proc/$hub_pid/task` supplies the thread census and
`/proc/$hub_pid/stat` fields 14 and 15 supply process CPU ticks. On macOS,
`ps -M -p "$hub_pid"` supplies per-thread rows and two `ps -o time=` samples
five seconds apart provide the equivalent operator observation. Accept idle
only after counters converge and average CPU remains at or below 5% of one
core. CPU corroborates the deterministic resource gates; it never replaces
them.

After `down`, the shared census checks live processes by the exact Hub and
session-worker executable realpaths. It separately compares zombies with a
pre-campaign baseline and a bounded settle window because zombies have no argv
to match. Linux retains the Botster-role `comm` filter; Darwin compares every
new zombie because both `comm` and `args` become `<defunct>`. The self-test
forks and execs a worker-named binary and proves the platform-specific zombie
oracle rejects it before cleanup. A SID-only scan is insufficient because PTY
workers may call `setsid()`.
