# botsterq

A machine-wide queue for heavy work, so that many agents' builds and test suites
take turns instead of overloading the laptop. It replaces message-based slot
coordination between writers.

## Install

```sh
brew install task-spooler
mkdir -p ~/.local/share/botsterq
cp tools/botsterq/botsterq tools/botsterq/botsterq-job tools/botsterq/README.md ~/.local/share/botsterq/   # from a botster-hub checkout
ln -sf ~/.local/share/botsterq/botsterq ~/.local/bin/botsterq
```

Install a copy, not a link into a worktree, so removing a worktree cannot break it.

`~/.local/bin` must be on `PATH`, and `python3` (3.9 or later) must be available for the job
supervisor. State (socket, slot count, pid files) lives in
`~/.botsterq` (override with `BOTSTERQ_HOME`).

## Use

```sh
botsterq run --label "hub lib" -- cargo test --locked -p botster-hub --lib
botsterq run --label "hub lifecycle" --exclusive -- ./test.sh --locked --test hub_daemon_lifecycle_test
botsterq list            # queued and running jobs: id, state, enqueue time, label @ owner dir
botsterq cancel <id>     # remove a queued job, or SIGTERM a running job's process group
botsterq slots           # show how many jobs run at once (default 2)
botsterq slots 3         # change it (kept across server restarts)
```

- `run` blocks until its job has run, streams the output live, and exits with the
  command's exit code.
- The job runs in the caller's directory with the caller's environment (so
  `RUSTUP_TOOLCHAIN`, `BOTSTER_HUB_BIN` and the other test variables pass through),
  under `nice -n 10`, in its own process group.
- Cancelling (Ctrl-C, SIGTERM or SIGHUP to `run`, or `botsterq cancel <id>`): a
  queued job never starts. A running job's group gets SIGTERM, and SIGKILL for any
  member still there when the grace period (10 s, `BOTSTERQ_KILL_GRACE`) ends,
  including a child that ignores SIGTERM after its leader exits. The queue slot is
  released only when the whole group is gone, so the next job never starts beside
  the remains of a cancelled one. `run` exits 130 after a cancel.
- A job that exits while leaving other processes in its group gets them stopped the
  same way (grace, SIGTERM, SIGKILL), with a warning naming them; `run` keeps the
  command's own status.
- A job whose `run` process is gone when its turn comes does not start. A caller
  killed with SIGKILL while its job runs cannot be noticed; use `botsterq cancel <id>`.
- `slots N` refuses while any job is queued or running.
- Inside a job, `botsterq run` runs its command directly, so nesting cannot deadlock.

## What goes through it

Heavy work: `cargo build`, `cargo test` (lib, lifecycle, any integration target),
`cargo clippy`, `script/build-dev-artifacts`, `./test.sh`, and the live client
suites. Light commands (git, `cargo fmt --check`, reading files) run directly.

## Lifecycle exclusivity

`--exclusive` makes a job require every slot, so it runs alone: nothing else heavy
competes with the load-sensitive lifecycle target, and no second lifecycle run can
overlap it. Ordinary jobs admitted after an exclusive job is queued wait for
it to finish before they enter the queue (the check and the enqueue are one step
under the admission lock). Without that, task-spooler would keep filling single
free slots from behind it, and the exclusive job could starve. Use
`--exclusive` for `hub_daemon_lifecycle_test`, for the full `./test.sh`, and for
any run whose result is sensitive to host load.

## How it works

task-spooler (`ts`) on the socket `~/.botsterq/queue.sock`, with `TS_SLOTS` slots.

- Admission is serialized by one lock (`~/.botsterq/admission.lock`, a perl
  `flock`; macOS has no `flock(1)`). Under it, `run` checks for a queued exclusive
  job, enqueues with `ts -f -n -N <slots> -L "<label> @ <cwd> #<token>"`, waits for
  `TS_ENV` to signal the enqueue through a FIFO (an event), and reads the job's id.
  `slots N` takes the same lock, which is why an exclusive job's reservation always
  equals the slot count.
- `-f` keeps the job a child of the caller's `ts` client (caller's environment and
  directory); `-n` streams output instead of storing it; `ts -w <id>` returns the
  job's exit code.
- Each job runs under `botsterq-job`, a small Python supervisor that leads the job's
  process group. It publishes its pid (the group id) and checks for a cancel marker
  under a per-job lock; a cancel creates the marker and reads the pid under the same
  lock, so either the job published first (the cancel signals the supervisor) or the
  cancel came first (the job deletes the marker and does not start). A job whose
  `run` is gone does not start. The supervisor runs the command as a child, and on
  SIGTERM signals the group, then SIGKILLs what outlives the grace, the command
  itself included. A cancel before the command starts means it never starts
  (cancel signals are blocked around the spawn). It exits only
  when the group is empty, waiting on exit events (kqueue NOTE_EXIT on macOS, pidfd
  on Linux) and re-enumerating members after each round so a process forked
  meanwhile is included. Its exit, observed with `ts -w`, is the event that the job
  and its group are gone.
- A cancel that arrives while `run` is still inside admission resolves the job by its
  token, so a job that already started is stopped too.
- Waiting uses `wait`, `ts -w` and process-exit events, never polls. The only timer is
  the kill grace, a deadline.
- `tools/botsterq/test-botsterq` is the regression suite (a private queue): it
  covers the exit code and environment, slot refusal, exclusive reservations and
  fairness, cancel of queued and running jobs, a SIGTERM-ignoring child with an
  untouched control process, a SIGTERM-ignoring command, both sides of the
  start/cancel race, a cancel before the command starts and a cancel during
  admission (pinned with test hooks), slot release only after the cancelled group is
  empty, leftover processes (including a command that exits before the supervisor
  first looks), nesting, an orphaned queued job, and `cancel <id>`.

## Stage 2 (design only): a remote Linux backend

- A second task-spooler queue on the remote test host (the HyperFlex testq
  host), reached over ssh like `bin/remote-test` in HyperFlex. The host runs
  jobs in a Docker image with Rust 1.97.0 and Zig 0.16.0 (for Ghostty).
- `botsterq run --any -- ...` marks a job as able to run on either backend;
  `--macos` (the default) keeps it local. Lifecycle suites, live client suites
  and candidate builds for local use stay `--macos`.
- Routing: an `--any` job goes to whichever backend has a free slot now (local
  first on a tie). Otherwise it queues on the backend with the shorter queue.
- Remote jobs sync the checkout first (rsync, uncommitted changes included, one
  stable directory per checkout, as testq does), then stream output back over
  ssh. Ctrl-C cancels on the host with the same queued/running split.
- Needs from the user: the host address, and whether the Rust target cache
  should persist on the host between jobs (it should, per checkout, to keep
  incremental cost low).
