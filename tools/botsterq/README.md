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

`~/.local/bin` must be on `PATH`, and `python3` (3.9 or later; `botsterq run` refuses an older one before admission) must be available for the job
supervisor. State (socket, slot count, pid files) lives in
`~/.botsterq` (override with `BOTSTERQ_HOME`).

## Use

```sh
botsterq run --label "hub lib" -- cargo test --locked -p botster-hub --lib
botsterq run --label "hub lifecycle" --exclusive -- ./test.sh --locked --test hub_daemon_lifecycle_test
botsterq run --label "hub tests" --exclusive --deadline 20m -- ./test.sh --locked   # shorter hang guard
botsterq list            # queued and running jobs: id, state, enqueue time, label @ owner dir; a run still waiting for admission shows as `waiting` (id `-`)
botsterq wait <id>        # block until job <id> ends; exit with its status (130 if it was cancelled, 1 if unknown)
botsterq audit           # jobs that overlapped an exclusive job (from the events log)
botsterq cancel <id>     # remove a queued job, or SIGTERM a running job's process group
botsterq slots           # show how many jobs run at once (default 2)
botsterq slots 3         # change it (kept across server restarts)
```

- `run` blocks until its job has run, streams the output live, and exits with the
  command's exit code.
- The job runs in the caller's directory with the caller's environment (so
  `RUSTUP_TOOLCHAIN`, `BOTSTER_HUB_BIN` and the other test variables pass through),
  under `nice -n 10`, in a process group of its own.
- Cancelling (Ctrl-C, SIGTERM or SIGHUP to `run`, or `botsterq cancel <id>`): a
  queued job never starts. A running job's group gets SIGTERM, and SIGKILL for any
  member still there when the grace period (10 s, `BOTSTERQ_KILL_GRACE`) ends,
  including a child that ignores SIGTERM after its leader exits. The queue slot is
  released only when the whole group is gone, so the next job never starts beside
  the remains of a cancelled one, even if a process survives SIGKILL. `run` exits
  130 after a cancel.
- A job that exits while leaving other processes in its group gets them stopped the
  same way (grace, SIGTERM, SIGKILL), with a warning naming them; `run` keeps the
  command's own status.
- A job whose `run` process is gone when its turn comes does not start. A caller
  killed with SIGKILL while its job runs cannot be noticed; use `botsterq cancel <id>`.
- Deadline: `--deadline <duration>` (seconds, or a number with `s`, `m` or `h`)
  bounds a job's run time from its start (queue wait is not counted). At expiry the
  supervisor prints `deadline expired`, stops the group like a cancel (SIGTERM,
  SIGKILL when the grace ends, the slot held until the group is gone), and `run`
  exits 124; a cancel (130) wins. An `--exclusive` job has a 45 minute deadline
  unless it passes its own (any value replaces the default; the environment variable
  `BOTSTERQ_EXCLUSIVE_DEADLINE`, in seconds, changes the default for tests). An
  ordinary job has none. It is a hang guard, not a budget: it exists so one hung job
  cannot hold the queue for hours. It does not apply after an unexpected supervisor
  error, when the supervisor only holds the slot until the group is gone.
- An ordinary job that meets a queued exclusive job waits for it while holding the
  admission lock, so a job that arrives later cannot be queued ahead of it. What is and
  is not promised: (1) a job that holds the lock, or is queued, keeps its place; (2)
  the order among jobs that are all waiting at the lock is the kernel's `flock` wakeup
  order, first come first served on macOS in practice and checked by a test with
  three waiters, but not promised by the platform. `botsterq slots N` refuses at once
  while that wait holds the lock. Cancelling a `run` that waits, at the lock or for the
  exclusive job, ends it at once with exit 130: its command never starts and it leaves
  no admission process behind. How a cancel reaches the admission: `run` holds a cancel
  fifo (`~/.botsterq/run/<token>.admitcancel`) open for writing and sends a cancel as one
  byte; it stores and signals no pid. The admission process (a small perl event loop)
  waits in `select` for the lock, which its own child takes, or for a cancel, and then for
  its admission child or a cancel; it signals only those two children, which it has not
  reaped, so a reused pid cannot be hit. If `run` dies without a chance to clean up (a
  SIGKILL), the fifo reports end of file, and the admission cancels itself and removes its
  own fifo and output file. A cancel that lands in the
  instant an admission finishes can leave a queued job for a run that is gone; the job
  is skipped when its turn comes, because its `run` process no longer exists, and never
  starts its command. The admission files (`*.admit*`) of a cancelled run do not
  remain, by the tests.
- After an install, a `run` that started before it keeps working: the supervisor
  accepts the old argument shape (no deadline) and the legacy `__admit` entry point
  admits with the old protocol.
- `botsterq audit` reads `~/.botsterq/events.log` (an append-only file: the supervisor
  writes a `start` line, with the host load average, and an `end` line, when the
  command's group is gone, for every job) and prints each job that ran at the same time
  as an exclusive job; it exits 1 if there is one. Use it to answer whether an exclusive
  job really ran alone. The log has no rotation. It is evidence only for the jobs it
  recorded: a write failure makes the supervisor print a warning on stderr and the
  job disappears from the audit, so an empty result is not proof that a job ran alone.
- Target cap: before a job's command starts, botsterq measures the `./target` of the job's
  directory when that directory has a `Cargo.toml`. If it is above the cap, it runs
  `cargo clean --target-dir target` there, in that job's slot, prints one line
  (`botsterq: <dir>/target is N GB, above the C GB cap; running cargo clean ...`) and adds a
  `clean` line to the events log. The cap is one setting, `BOTSTERQ_TARGET_CAP_GB`, default 15
  (the value is PROVISIONAL, awaiting the user; a fresh full Hub build is about 7 to 8 GB; `0`
  turns it off; the default lives in one line at the top of the `botsterq` script). Only the
  job directory's own `./target` is ever cleaned: nothing happens without a `Cargo.toml`, and
  nothing when `CARGO_TARGET_DIR` is set (the command builds elsewhere). The price is one cold
  rebuild each time the cap is hit. It exists because unpruned worktree targets (15 to 40 GB each)
  filled the disk three times in 24 hours (free space 4.9 GB and 9.6 GB).
  The clean is skipped, with one line and a `clean_skipped` events entry, when another running
  botsterq job has the same directory or one inside it, because that job is using this target;
  the same happens if the running-job list cannot be read. What it does not see: a job that
  builds into this target from outside the directory tree (through a different `CARGO_TARGET_DIR`
  setup or a symlink), and a job of another queue. A phased gate whose later phase starts over the
  cap loses the compiled artifacts of the earlier phase and recompiles them; its candidate
  directory (`target/candidate` by default, or a directory outside `target/`) survives: the clean
  moves `target/candidate` aside and back.
  Why not prune by age: `cargo sweep` (0.8.0) was measured on a Hub worktree with a build, a
  clippy run, `cargo sweep --file`, and the same build again. It cleaned 1.1 GiB of 8.7 GB
  (13 percent), and the next build recompiled 293 crates instead of none, because a build that
  finds an artifact fresh reads it without rewriting it, so on this Mac a reused artifact carries
  no last-use time and a time-based sweep deletes exactly the shapes the cycle just used. Time
  and size based sweeping (`--time`, `--maxsize`) use the same file times.
- Removed queued jobs: task-spooler never ends a `ts -w` waiter for a job that was removed from
  the queue, so `botsterq` records those waiters (`run/<token>.waitfor.<id>`, one per admission or
  run that waits for job `<id>`) and, when it removes a queued job (`botsterq cancel <id>`, or the
  run being interrupted), marks it (`run/<token>.removed`, its run then exits 130) and releases
  the recorded waiters, each only if its command line is exactly `ts -w <id>`. A cancel in the
  millisecond between starting a waiter and writing its record misses that waiter, and a run
  killed with SIGKILL leaves its records behind (small files). A job removed by a plain `ts -r`
  is not released.
- Recovery step, the ONLY case where killing a process is allowed (and only by pid): if the queue
  looks stuck (no job runs, `run`s wait) or a `botsterq run` hangs after its job was removed, list
  `ps -axo pid,command | grep 'ts -w'`; a `ts -w <id>` whose job `<id>` no longer appears in
  `botsterq list` is one of botsterq's own internal waiters that will never end (this happens only
  for waiters recorded by an older botsterq, since this version releases them itself). Check with
  `lsof ~/.botsterq/admission.lock` that its process does not hold the admission lock, then stop
  exactly that pid. Never stop a `ts -w <id>` of a job that still exists, and never kill by name
  or pattern. Documented and approved by the orchestrator (2026-09-29) after such waiters (pids 55589
  and 56728 for removed jobs 609 and 610, about 8 hours old) and a jam at 10:36 were cleared this way.
- Mr Boxington (`mbx`, https://mr-boxington.jdx.dev), a shared build cache. Enabled for every
  job by one setting at the top of the `botsterq` script (`mbx_default=1`; `0` turns it off for
  everyone; `BOTSTERQ_MBX=0` turns it off for one job). When `mbx` is installed, `botsterq run`
  puts a shim directory (`~/.botsterq/mbx-shim`, one symlink named `cargo` to the `mbx` binary)
  first on the job's PATH, so every plain `cargo` in the job goes through mbx, and sets
  `MBX_CACHE_DIR` to `~/.botsterq/mbx-cache` (unless you set it) and `MBX_TARGET_VIEWS=0`.
  Managed target views stay off because mbx would otherwise replace `./target` with a symlink into
  its cache, which the target cap and `target/candidate` do not expect. Nothing global changes:
  no `mbx setup`, no mise or cargo config. The target cap's `cargo clean` uses the real cargo,
  never the shim (mbx has its own `clean`). Measured (Hub, `./test.sh --phase build`, fresh
  worktrees, load 9 to 14): 234 s without mbx, 267 s with an empty cache (the fill), 196 s with
  the warm cache (16 percent faster than without), 3 GB of new disk per fresh worktree against
  8 to 9 GB (reflinks); 484 cache hits. `mbx exec` alone caches only C and C++ compiles, which is
  why the shim is used. If a build looks wrong (a stale or odd artifact, a Zig or build-script
  problem, a candidate provenance mismatch), run the job with `BOTSTERQ_MBX=0`, and turn it off
  for everyone with `mbx_default=0` before investigating. The cache's files are read-only: use
  `chmod -R u+w ~/.botsterq/mbx-cache` before deleting it.
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
- Each job runs under `botsterq-job`, a small Python supervisor. It publishes its
  pid and checks for a cancel marker under a per-job lock; a cancel creates the
  marker and reads the pid under the same lock, so either the job published first
  (the cancel signals the supervisor) or the cancel came first (the job deletes the
  marker and does not start). A job whose `run` is gone does not start.
- The supervisor runs the command as a child in a process group of its own and
  publishes that group as the pid file's second line. A cancel sends the group
  SIGTERM, then SIGKILL when the grace ends, the command itself included. A cancel
  before the command starts means it never starts (cancel signals are blocked
  around the spawn).
- The supervisor exits, which releases the slot, only when the kernel reports the
  command's group gone: `killpg(group, 0)` fails with ESRCH (zombies count as
  present; macOS answers EPERM for a group of zombies only). A process that
  survives SIGKILL, a failed `ps` census or a failed exit watch never ends the job:
  the slot stays held, with a warning, until the group is gone. Its exit, observed
  with `ts -w`, is the event that the job and its group are gone.
- A cancel that arrives while `run` is still inside admission resolves the job by its
  token, so a job that already started is stopped too.
- Waiting uses `wait`, `ts -w` and process-exit events (kqueue NOTE_EXIT on macOS,
  pidfd on Linux) on the members a `ps` census names. The kill grace is a deadline.
  The one exception to waiting on events is a degraded mode: when no exit event can
  be watched (the census or a watch failed at any step, or the group holds only
  zombies awaiting their reaper), the supervisor polls for absence every 0.1 s. It is
  a progress poll, not a give-up deadline: it never ends the job. An unexpected
  error in the supervisor's loop also never releases the slot early: the command
  runs on and is reaped, a cancel still escalates, and the slot is held until the
  group is gone.
- `tools/botsterq/test-botsterq` is the regression suite (a private queue): it
  covers the exit code and environment, slot refusal, exclusive reservations and
  fairness, cancel of queued and running jobs, a SIGTERM-ignoring child with an
  untouched control process, a SIGTERM-ignoring command, both sides of the
  start/cancel race, a cancel before the command starts and a cancel during
  admission (pinned with test hooks), slot release only after the cancelled group is
  empty, leftover processes (including a command that exits before the supervisor
  first looks), nesting, an orphaned queued job, `cancel <id>`, the slot held
  while absence is unproved (a SIGKILL survivor alone, and with a failed census,
  kqueue creation, registration, wait, fallback select, or supervisor loop, each
  injected at the real boundary through test hooks), and the Python version check.
  Run it once more with `PATH=/usr/bin:$PATH` to cover the oldest supported Python.

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
