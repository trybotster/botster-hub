# botsterq

A machine-wide queue for heavy work, so that many agents' builds and test suites
take turns instead of overloading the laptop. It replaces message-based slot
coordination between writers.

## Install

```sh
brew install task-spooler
mkdir -p ~/.local/share/botsterq
cp tools/botsterq/botsterq tools/botsterq/README.md ~/.local/share/botsterq/   # from a botster-hub checkout
ln -sf ~/.local/share/botsterq/botsterq ~/.local/bin/botsterq
```

Install a copy, not a link into a worktree, so removing a worktree cannot break it.

`~/.local/bin` must be on `PATH`. State (socket, slot count, pid files) lives in
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
- Ctrl-C, SIGTERM or SIGHUP to `run` cancels the job. A queued job leaves the
  queue. A running job's process group gets SIGTERM, then SIGKILL after 10 s
  (`BOTSTERQ_KILL_GRACE`). Only that group is signalled, never anything by name.
- A caller killed with SIGKILL cannot be noticed, so its job runs to the end. Use
  `botsterq cancel <id>`.
- Inside a job, `botsterq run` runs its command directly, so nesting cannot deadlock.

## What goes through it

Heavy work: `cargo build`, `cargo test` (lib, lifecycle, any integration target),
`cargo clippy`, `script/build-dev-artifacts`, `./test.sh`, and the live client
suites. Light commands (git, `cargo fmt --check`, reading files) run directly.

## Lifecycle exclusivity

`--exclusive` makes a job require every slot, so it runs alone: nothing else heavy
competes with the load-sensitive lifecycle target, and no second lifecycle run can
overlap it. Ordinary jobs queued after a queued exclusive job wait for it to
finish before they enter the queue. Without that, task-spooler would keep filling
single free slots from behind it, and the exclusive job could starve. Use
`--exclusive` for `hub_daemon_lifecycle_test`, for the full `./test.sh`, and for
any run whose result is sensitive to host load.

## How it works

task-spooler (`ts`) on the socket `~/.botsterq/queue.sock`, with `TS_SLOTS`
slots. `run` calls `ts -f -n -N <slots> -L "<label> @ <cwd> #<token>" ...`: `-f`
keeps the job a child of the caller's `ts` client (caller's environment and
directory), `-n` streams output instead of storing it, and `-N` is the slot
count the job takes. The job writes its pid (its own process group) to a pid
file before it execs the command; cancel uses it to signal exactly that group.
Waiting uses `wait` and `ts -w`, which are completion events, not polls.

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
