# Provision an isolated loaded daemon lifecycle runner

## Context loaded

- Pipeline run `run_1784132692_694978`, Plan step `hotwire_plan`, and gate
  `hotwire_plan_gate`. The run has no prior artifacts, reviews, findings,
  questions, or answers.
- Ticket `ticket_1784132686_796177` and its blocked consumer,
  `ticket_1784087788_242994`. The consumer's approved diagnostic-first plan and
  review establish that commit `20871abafdd227a1c145e07035731f916938ff28`
  contains the diagnostics baseline and that this ticket owns infrastructure,
  not the residual behavior fix.
- `[[planner-playbook]]` and `[[botster-planner-playbook]]`. This is the Rust
  Botster hub, not a Hotwire/Rails application, so
  `[[hotwire-app-planner-playbook]]` does not apply.
- Botster stack packet: `[[botster-architecture]]`, `[[cli-patterns]]`,
  `[[spa-patterns]]`, `[[project pipeline orchestration belongs in a device-level botster plugin]]`,
  `[[project pipelines needs an operator workbench not more primitives]]`,
  `[[project pipelines ui contract belongs in the plugin readme]]`,
  `[[botster orchestration should spawn agents with explicit target ids]]`,
  `[[botster orchestration prompts must bind agents to explicit worktrees]]`,
  `[[botster pipeline needs continuous product owner between agent steps]]`, and
  `[[plan agents must author vault context as wikilinks not home paths]]`.
- Testing and delivery constraints: `[[test script required for rust tests not cargo test]]`,
  `[[botster test sh forwards arguments to cargo not custom unit flags]]`,
  `[[a poisoned test lock is a symptom not a waiver]]`,
  `[[suite wide acceptance criteria make every observed test failure in scope]]`,
  `[[full suite hangs need source and behavior proof before unrelated waivers]]`,
  `[[PTY integration tests poll for readiness not fixed sleeps]]`,
  `[[a regression test must be shown to go red with the fix reverted]]`,
  `[[prefer universal filesystem tools over framework-specific artifact abstractions]]`,
  `[[prefer framework and library components over custom solutions]]`,
  `[[plan steps need reviewable plan artifacts]]`, and
  `[[project pipelines checklist worker timeouts require artifact evidence fallback]]`.
- Repository evidence: GitHub Actions is enabled for the public
  `trybotster/botster-hub` repository, but the repository currently has no
  workflows and no registered self-hosted runners. `test.sh` is the required
  `BOTSTER_ENV=test cargo test "$@"` entry point. The target test is in
  `tests/hub_daemon_lifecycle_test.rs` and exercises real `botster-hub` daemon,
  Unix-socket, session-worker, attach, and independent command subprocesses.
- Current dependency pins checked during planning: Rust `1.97.0` is the current
  stable release; `actions/checkout` `v7.0.0` resolves to
  `9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0`; and
  `actions/upload-artifact` `v7.0.1` resolves to
  `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a`. Implementation should use the
  immutable action SHAs with version comments and the exact Rust version.

## Decision and runtime path

Add one manual `workflow_dispatch` workflow on a standard `ubuntu-24.04`
GitHub-hosted runner. GitHub provisions a fresh VM for the job, so it provides
the required isolated, non-developer-host execution without a permanent machine,
self-hosted runner registration, secret, or recurring idle cost. Repository
write access controls who may dispatch it; the job itself receives only
`contents: read`.

The dispatch path is:

1. An authorized repository operator selects a full 40-character subject commit,
   one of the named test targets, a validated repetition count, and a named stress
   profile.
2. The workflow checks out its runner harness from the workflow commit into one
   directory and the exact subject commit into a separate directory. This is
   necessary because the default diagnostics subject predates the runner script.
   It verifies the subject's resolved `HEAD` equals the requested SHA.
3. The workflow installs exact Rust `1.97.0` with `rustup`, records tool versions
   and host metadata, and invokes the harness script with the subject checkout as
   its working directory.
4. The harness maps the workflow's bounded test choice to an argument array that
   calls the subject checkout's `./test.sh`; it never evaluates a free-form shell
   command. The default is
   `./test.sh --test hub_daemon_lifecycle_test -- --nocapture`. A focused
   `stalled_attach_stdout_does_not_block_other_daemon_commands` choice is allowed
   for capture triage, but cannot satisfy the 20+ suite acceptance campaign.
5. The harness starts the selected job-local CPU stress process group and a
   resource sampler, then runs each default-parallel test repetition in its own
   owned process group. It stops at the first red result so the exact target
   failure is retained without spending the remaining campaign budget.
6. Signal and exit traps terminate, bounded-wait, escalate if necessary, and reap
   the active test, daemon/session-worker descendants, sampler, and load generator.
   The workflow also has an `always()` cleanup step as a second boundary. The
   fresh VM is the final isolation boundary on forced runner termination.
7. Complete stdout/stderr is streamed to the GitHub job log while also being
   written under one artifact directory. An `always()` upload step publishes the
   accumulated directory on green, red, inner timeout, and normal cancellation.
   Streaming ensures evidence up to a hard cancellation remains in the durable
   workflow log even if cancellation prevents the upload step from finishing.

This changes the real operator path: after the workflow lands on the default
branch, an operator can dispatch it from GitHub Actions or `gh workflow run`, and
the workflow executes the real repository test wrapper at the selected subject
commit. It is not scaffold-only.

## Scope

### In scope

- Add `.github/workflows/loaded-daemon-lifecycle.yml` with manual dispatch only,
  explicit `contents: read`, `ubuntu-24.04`, an exact Rust toolchain, immutable
  action SHAs, a workflow-level concurrency group, and a job timeout that leaves
  an upload/cleanup cushion before GitHub's runner limit.
- Expose bounded dispatch inputs:
  - `subject_sha`: full commit SHA, defaulting to the diagnostics commit above;
  - `test_target`: choice of full lifecycle suite or the single residual test;
  - `repetitions`: integer validated to a documented finite range, default `20`;
  - `stress_profile`: choice of `residual-tail`, `moderate`, or `none`, defaulting
    to the documented residual-tail profile.
- Add `script/run-loaded-daemon-lifecycle` as a Bash harness using standard Linux
  process, filesystem, and signal primitives. Keep all generated state in the
  workflow-provided artifact directory and the subject checkout's existing test
  paths.
- Define the default residual-tail profile in terms of detected CPUs and a fixed,
  bounded runnable-worker multiplier. Record actual `/proc/loadavg`, CPU, memory,
  process, and elapsed-stage samples rather than claiming a nominal profile
  reached a particular load.
- Give the campaign an inner deadline and each repetition a deadline below the
  workflow job timeout. Preserve exit status `0` only when every requested run is
  green; red tests, invalid inputs, cleanup failures, and timeouts remain nonzero.
- Preserve metadata, per-run combined stdout/stderr, exact panic/assertion,
  elapsed stage/run data, campaign exit statuses, resource samples, requested and
  resolved refs, toolchain versions, and cleanup TERM/KILL/wait evidence.
- Add `docs/loaded-daemon-lifecycle-runner.md` with GitHub UI and `gh` dispatch,
  artifact retrieval, stress-profile interpretation, early-red behavior,
  cancellation, timeout, teardown, and rerun instructions.

### Non-scope

- No change to `tests/hub_daemon_lifecycle_test.rs`, its assertions, readiness
  budgets, default parallelism, or production daemon behavior.
- No diagnosis or fix for the residual flake; that remains solely owned by
  `ticket_1784087788_242994`.
- No `--test-threads=1`, retries that hide a red run, timeout inflation in the
  test, or claim that 20 greens alone prove elimination.
- No self-hosted runner, cloud account/provisioner, long-lived VM, production
  host, shared developer host, secret-bearing workflow, scheduled trigger, or
  always-on resource.
- No generic remote-command runner, arbitrary workflow shell input, reusable CI
  framework, dependency cache, matrix, broad CI rollout, or adjacent cleanup.
- No Rails, Hotwire, SPA, TUI, Lua plugin, daemon protocol, Cargo dependency, or
  lockfile changes.

## Assumptions and unknowns

### Assumptions

- GitHub-hosted fresh-VM isolation satisfies "isolated, non-shared" at the job
  boundary intended by the ticket; no dedicated physical host is required.
- Repository collaborators authorized to run Actions are the authorized
  operators. No separate GitHub environment approval gate is requested.
- The public repository's standard hosted-runner allowance is acceptable and
  avoids recurring idle cost.
- `ubuntu-24.04` provides loopback, Unix sockets, `/proc`, Bash, `setsid`, and the
  process permissions needed by the existing real-daemon suite.
- The default subject remains the merged diagnostics commit even though the
  workflow/harness comes from the later default-branch workflow commit.
- A 20-run default campaign plus cleanup/upload cushion fits within the bounded
  job budget. If measured duration disproves this, the implementer must ask a
  human before reducing the default campaign or moving to paid larger runners.

### Unknowns

- The exact runnable-worker multiplier needed to reproduce the prior load-50 tail
  on GitHub's standard runner. The profile must be fixed and recorded, but actual
  samples—not the profile label—decide whether evidence is comparable.
- Whether GitHub's standard runner exhibits the residual within one bounded
  campaign. Non-reproduction is a valid infrastructure result, not permission to
  weaken the downstream capture gate.
- Whether a hard GitHub cancellation permits the final artifact-upload step to
  complete. The plan therefore streams every diagnostic to the durable job log,
  uses signal-aware cleanup, and treats the disposable VM boundary as cleanup
  enforcement; the cancellation acceptance exercise must confirm the observable
  behavior before advancement.
- Exact practical per-run and campaign deadlines. Implementation may choose the
  smallest values that fit measured one-run evidence while retaining a clear
  cleanup/upload cushion, without changing the test's internal timeouts.

No human question is blocking this plan. Ask before substituting a paid larger
runner, dedicated hardware, a shared host, a secret, or weaker acceptance proof.

## Affected surfaces/files

- `.github/workflows/loaded-daemon-lifecycle.yml` — authorized manual entry point,
  bounded input mapping, isolated checkouts, pinned environment, deadlines,
  cleanup, and durable upload.
- `script/run-loaded-daemon-lifecycle` — job-local stress, sampling, repeated test
  execution, first-red preservation, process-group ownership, and reaping.
- `docs/loaded-daemon-lifecycle-runner.md` — operator usage, evidence, cancellation,
  timeout, and teardown documentation.
- `docs/plans/provision-isolated-loaded-daemon-lifecycle-runner.md` — this
  reviewable Plan-stage artifact.
- `test.sh` and `tests/hub_daemon_lifecycle_test.rs` — invoked runtime surfaces;
  expected to remain unchanged.

Botster layer: Rust hub test/CI infrastructure and operator documentation only.
Pipeline target/worktree: the assigned target and ticket worktree are used for
implementation; the production dispatch explicitly binds the subject by exact
commit SHA and never relies on an ambient checkout.

## Risks

- **Harness is absent from the diagnostics commit.** Separate harness and subject
  checkouts keep the trusted runner implementation available while testing exact
  old code.
- **Free-form workflow input becomes command execution.** Use typed choices and
  array-based mapping; accept only a full SHA and validated integer count.
- **Load label overstates actual contention.** Record frequent load/resource
  samples and publish worker count plus CPU count; downstream diagnosis must cite
  samples, not the label.
- **Stress starves cleanup or artifact upload.** Stop load before packaging, use an
  inner campaign deadline, and reserve job time for TERM/KILL/wait and upload.
- **A test daemon or worker escapes.** Put each run and the load generator in
  distinct owned process groups, kill groups by recorded PGID, wait every direct
  child, and record post-cleanup process evidence. Never use broad host-wide
  `pgrep` as ownership proof.
- **A red run is hidden by loop or pipeline status.** Preserve the test command's
  status through `tee`, stop immediately, write a campaign status row, upload, and
  return nonzero after cleanup.
- **Cancellation truncates uploaded files.** Stream output continuously to the
  GitHub log, trap signals in the harness, run workflow cleanup/upload under
  cancellation-aware conditions, and verify this path with an actual cancelled
  dispatch.
- **Third-party action or toolchain drift changes evidence.** Pin exact action
  SHAs, Rust `1.97.0`, Ubuntu release label, subject SHA, and `Cargo.lock`; record
  all resolved versions.
- **Workflow could expose secrets from an arbitrary ref.** Grant read-only
  contents, pass no secrets, persist no checkout credentials, and restrict the
  input to commits resolvable in this repository.
- **Twenty greens are statistically weak for a roughly 1/15 flake.** This runner
  supplies capture and corroboration. The downstream ticket still requires the
  exact captured root and red-when-reverted proof.

## Acceptance checks/tests

### Static and local harness checks

- Parse the workflow with `actionlint` when available; otherwise use GitHub's
  workflow acceptance on the branch and record that no local validator exists.
- `bash -n script/run-loaded-daemon-lifecycle` passes.
- Exercise invalid SHA, repetition below/above bounds, and unknown profile/target;
  each must fail before starting load or tests and still emit metadata/cleanup
  evidence.
- Run a local signal smoke with a deliberately long child process tree: send
  `TERM` to the harness, require a nonzero status, and verify every recorded
  test/load/sampler PID or PGID is gone and was waited. This tests the harness,
  not the daemon behavior.

### Real workflow checks

- Push the workflow branch and dispatch a one-repetition focused run against
  `1c4af771d6ed9c09b4b6e0e6f1f8b0c906c79895`. Confirm the workflow log shows the
  exact resolved SHA, Rust `1.97.0`, default Cargo parallelism, real
  `botster-hub`/socket lifecycle test execution, resource samples, exit status,
  and cleanup evidence.
- Dispatch a bounded run and cancel while load and a test process group are
  active. Confirm the durable workflow log contains signal/cleanup evidence, no
  owned process survives the job boundary, and the accumulated artifact uploads
  when GitHub permits cancellation cleanup.
- Exercise the inner timeout with a deadline safely below the job timeout. Confirm
  the run is red, load and test groups are terminated and waited, cleanup evidence
  is logged, and artifacts upload. Do not wait for GitHub's hard job timeout as
  the primary timeout test because it can preclude post-job upload.
- Inspect the uploaded bundle on a red/timeout path: it must contain requested and
  resolved refs, toolchain/runner metadata, exact test command, repetition index,
  full stdout/stderr and panic, elapsed stages, resource samples, all exit
  statuses, and cleanup TERM/KILL/wait/post-clean evidence.

### Binding ticket acceptance

- From the default branch, an authorized operator can dispatch the documented
  workflow against the exact diagnostics commit without a developer or production
  host.
- The default `lifecycle-suite` + `repetitions=20` + `residual-tail` dispatch runs
  `./test.sh --test hub_daemon_lifecycle_test -- --nocapture` with default Cargo
  parallelism until all 20+ runs complete or the first exact target failure is
  captured. Record actual sustained-load samples for comparability.
- Green, red, cancellation, and inner-timeout paths all demonstrate owned-process
  teardown and retain evidence in GitHub logs; green/red/inner-timeout paths also
  retain the complete uploaded artifact bundle.
- Usage, artifact retrieval, rerun, cancellation, and teardown behavior are
  documented and match the actual GitHub UI/CLI path.
- `git diff --check` passes, and the final diff contains only the four planned
  surfaces unless a directly necessary correction is explained in the
  implementation artifact.

## Vault gaps worth capturing

- No durable runner-specific convention currently says how Botster loaded CI
  campaigns should combine exact subject checkout, harness checkout, process-group
  ownership, streamed logs, and upload cushions. If implementation and real
  cancellation evidence validate this pattern, capture one atomic Botster CI
  note through the vault inbox and connect it to `[[botster-architecture]]` and
  `[[cli-patterns]]`.
- Capture a separate gotcha only if GitHub cancellation empirically prevents an
  `always()` upload or cleanup step; do not record the current uncertainty as a
  fact.
- No vault write is warranted during Plan because those behaviors are not yet
  verified. The Implement/Verify stages should record the capture path or an
  explicit "no durable knowledge discovered" disposition.

## Checklist evidence fallback

Both the standard vault checklist and the custom Plan workflow checklist calls
timed out at the Project Pipelines plugin-worker boundary. Following
`[[project pipelines checklist worker timeouts require artifact evidence fallback]]`,
this document records:

- notes/playbooks read in **Context loaded**;
- convention conflicts: none—the selected fresh hosted VM, universal Bash/process
  primitives, exact worktree/ref binding, repo wrapper, and non-behavioral scope
  conform to the loaded guidance;
- planned verification evidence in **Acceptance checks/tests**; and
- durable knowledge disposition in **Vault gaps worth capturing**.
