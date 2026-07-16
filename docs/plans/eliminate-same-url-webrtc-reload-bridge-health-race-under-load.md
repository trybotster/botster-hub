# Eliminate the same-URL WebRTC reload bridge-health race under load

## Context loaded

- Project Pipelines run `run_1784234860_483060`, Plan step `botster_plan`, run
  step `run_step_1784234861_209939`, and gate `botster_plan_gate`. The run has no
  prior artifacts, reviews, findings, questions, answers, or blocking
  dependencies.
- Ticket `ticket_1784169409_416830` and its two cited residual-tail failures:
  Actions runs `29466442155` at subject
  `e6ed8fa780b2d3a5fb4dbee3db842ecaa92f3f44` and `29530923386` at subject
  `b17ad232a9e95bdbfa463a6b7c45c9d89ea680a2`. The latter ran 48 CPU workers on
  4 CPUs, reached load averages above 50, and failed the target test with
  `botster-web bridge health did not become ready`. Its process samples retained
  both the target daemon and its supervised Node bridge after the test panic,
  which rules out an immediate spawn/exit failure but does not prove when the
  HTTP listener became ready.
- `[[planner-playbook]]` and `[[botster-planner-playbook]]`, plus their Botster
  packet: `[[botster-architecture]]`, `[[cli-patterns]]`, `[[spa-patterns]]`,
  `[[project pipeline orchestration belongs in a device-level botster plugin]]`,
  `[[project pipelines needs an operator workbench not more primitives]]`,
  `[[project pipelines ui contract belongs in the plugin readme]]`,
  `[[botster orchestration should spawn agents with explicit target ids]]`,
  `[[botster orchestration prompts must bind agents to explicit worktrees]]`,
  `[[botster pipeline needs continuous product owner between agent steps]]`, and
  `[[plan agents must author vault context as wikilinks not home paths]]`.
- Targeted testing constraints: `[[packaged botster web reloads need fresh webrtc grants]]`,
  `[[PTY integration tests poll for readiness not fixed sleeps]]`,
  `[[a regression test must be shown to go red with the fix reverted]]`,
  `[[test script required for rust tests not cargo test]]`,
  `[[botster test sh forwards arguments to cargo not custom unit flags]]`,
  `[[suite wide acceptance criteria make every observed test failure in scope]]`,
  `[[loaded lifecycle ci precompiles the exact test target before synthetic cpu stress]]`,
  and `[[pre existing failure waivers must isolate the first non cascade failure on base]]`.
- Repository trace: `write_botster_web_package` declares
  `readiness.result_fields = [local_url]`; its Node bridge writes the structured
  launch result inside the HTTP server's `listen` callback; supervisor snapshots
  refresh that launch result later; and `ListApps` projects it as
  `launch_target.local_url`. The verified publication order is listener ready
  (`/health` answerable), then launch-result file written, then supervisor
  refresh, then `ListApps.local_url`. The target test already observes the
  earliest correct readiness signal, but incorrectly treats 100 iterations at
  20 ms as a readiness budget under 12x CPU oversubscription.
- Plan Review `review_1784235737_998949` returned seven findings. Blocking human
  decision `question_1784235821_620334` approved a condition-driven `/health`
  wait with immediate supervised-terminal failure, a 60-second ordinary-run
  liveness backstop, and a separately bounded loaded-run backstop below the
  existing 900-second outer deadline. The answer explicitly rejected using
  `local_url` as readiness and approved it as a post-readiness contract check.
- The assigned target/worktree is explicit: target
  `tgt_7e208a0c76a44980a83b63af976b1f22`, this ticket worktree, base `main`, and
  clean subject `0aca7be327bfc57979a8cb8444b49d7a2f1e5ad8`.

Botster layer: Rust real-daemon integration-test fixture and lifecycle harness
only. No Lua plugin, SPA, TUI, Rails relay, MCP, public daemon protocol, or
production WebRTC transport behavior changes.

## Decision and runtime proof

Treat the residual as a test readiness-budgeting and observability defect. The
production same-URL path already has the intended invariant: every HTML request
asks the live daemon for a new bootstrap grant, and the existing test proves the
two grant IDs and secrets differ and both encrypted peers work. The failure
occurs earlier: a direct and correct `/health` readiness probe is coupled to an
arbitrary two-second fixed wall-clock ceiling while the supervised Node child is
competing with 48 busy workers on four CPUs.

Replace the fixed-iteration helper with a condition-driven wait whose success
predicate is only a successful exact `/health` response. Each observation also
requests `ListApps` for the exact `botster-web/web-client` row so the helper can
fail immediately when the supervised process reaches a terminal/failed state
and retain its process state, launch result, and diagnostics. Ordinary local/CI
`./test.sh` uses a 60-second liveness backstop. The loaded workflow explicitly
supplies an 840-second liveness backstop, leaving 60 seconds inside its unchanged
900-second outer run deadline for failure reporting and teardown. Neither
backstop is a readiness predicate or evidence of success; expiry is a
live-yet-stuck diagnostic failure.

After `/health` succeeds, require `ListApps` to publish the exact structured URL
`http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub`. A non-`None` different URL
fails immediately with expected and actual values. This later signal strengthens
the launch contract and diagnostics; it is not the race fix.

The fixture gains one test-only startup-delay environment override, set above
the old two-second ceiling for this test. The delayed listener is the
deterministic negative control: old fixed-iteration code is red on every machine
while the condition-driven helper is green. This deliberate fixture fault is
not readiness evidence, a product sleep, or a retry. Residual-tail execution
remains a separate load-acceptance campaign.

This is intentionally test-only. The production entrypoint is asynchronous by
contract: `StartPackageEntrypoint` starts a supervised process, while
`ListApps.launch_target.local_url` exposes the later structured launch result. Making the start
request block, changing supervisor publication semantics, or altering bootstrap
issuance would broaden the ticket without evidence of a product defect.

## Scope

### In scope

- In `tests/hub_daemon_lifecycle_test.rs`, replace
  `wait_for_botster_web_health` with a condition-driven helper that succeeds only
  on exact `/health`, fails immediately on supervised terminal state, and uses a
  60-second default liveness backstop configurable only for the owned loaded
  harness.
- Preserve the exact health shape so the test proves the listener, existing-hub
  ownership, socket source, socket presence, and non-mixed ownership.
- Add a fixture-only startup-delay environment override and set it above two
  seconds in this test, before `listen`, as deterministic fault injection.
- After health readiness, assert the exact app-row URL
  `http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub`; fail fast on any
  different published URL rather than waiting for expiry.
- Make a failed wait identify the phase and include elapsed time, expected URL,
  last `DaemonApp` lifecycle/launch target/diagnostics, daemon status or request
  error, and bridge HTTP result when one exists. Avoid generic unwrap panics that
  discard the last observed state.
- Timestamp daemon start, package enable, `StartPackageEntrypoint` return,
  listener health success, and `local_url` publication so a future red run
  attributes the ticket's unexplained 4m40s pre-readiness window.
- In `.github/workflows/loaded-daemon-lifecycle.yml`, supply the explicit
  840-second test liveness backstop only to the loaded lifecycle command. Do not
  change the existing 900-second outer deadline or stress profile.
- Keep the existing fresh-grant, wrong-origin, wrong-secret, first-peer,
  second-peer, session continuity, and cleanup assertions byte-for-byte unless a
  directly necessary signature adjustment is documented.
- Commit this Plan-stage artifact and later implementation/verification evidence
  traceable to the ticket.

### Non-scope

- No changes to `src/local_webrtc.rs`, `src/daemon_transport.rs`,
  `src/entrypoint_supervisor.rs`, public DTOs, grant TTL/redeem behavior,
  encryption, peer setup, daemon request routing, or production package launch
  semantics.
- No changes to `script/run-loaded-daemon-lifecycle`, its residual-tail worker
  profile, workflow inputs, default parallelism, per-run/campaign deadlines, or
  cleanup policy. The workflow-only test backstop environment is the sole loaded
  harness change authorized by `question_1784235821_620334`.
- No fixed product/test sleep used as readiness evidence, readiness-budget
  inflation, retry loop that reruns a red test,
  `--test-threads=1` acceptance run, ignored test, reduced load, assertion
  weakening, or broad test-suite cleanup.
- No replacement of the fixture with a direct Node child that bypasses the real
  hub-owned entrypoint supervisor, and no test-only production API.
- No adjacent repair for other lifecycle roots. Any failure observed by the
  required default-parallel campaign remains blocking unless exact evidence and
  a human disposition establish it is unrelated.
- No dependency, Cargo lockfile, plugin README, user documentation, SPA, TUI,
  Lua, Rails, or MCP change.

## Assumptions and unknowns

### Assumptions

- `/health` is the authoritative and earliest readiness condition because the
  listener is accepting requests when Node enters the successful `listen`
  callback.
- Structured `local_url` is later contract evidence: the fixture writes it
  inside that callback and the supervisor exposes it only on a later refresh.
- The two cited failures occur before any reload/bootstrap assertion. Therefore
  they do not support a production grant or peer-lifecycle change.
- `question_1784235821_620334` authorizes a 60-second ordinary liveness backstop
  and an 840-second loaded-test override below the unchanged 900-second outer
  deadline. These bounds prevent hangs and never decide readiness.
- The implementation uses the assigned worktree and repo `./test.sh`; no ambient
  checkout or raw `cargo test` result is acceptable.

### Unknowns

- The cited 4m40s occurred before the two-second health failure, but existing
  logs do not attribute it among worker fixture build, daemon start, package
  enable, entrypoint start, and listener scheduling. Phase timestamps are
  required before claiming where that interval went.
- The failed artifacts did not record the last `/health` body, app projection,
  launch-result state, or bridge stderr. The new diagnostics must distinguish
  not-listening, unhealthy response, terminal child, wrong URL, and liveness
  expiry.
- The 840-second loaded override is deliberately below the 900-second command
  deadline. If real evidence shows it leaves insufficient reporting/cleanup
  margin, ask a human rather than changing either value silently.

The ambiguity is resolved by human answer `question_1784235821_620334`. Ask
again before changing production launch semantics, the 60/840-second liveness
contract, the loaded stress profile or 900-second outer deadline, or required
negative-control/default-parallel acceptance evidence.

## Affected surfaces/files

- `tests/hub_daemon_lifecycle_test.rs` — semantic bridge readiness, state-rich
  diagnostics, fixture startup-delay injection, 60-second ordinary liveness
  backstop, exact post-ready `local_url`, and preserved WebRTC assertions.
- `.github/workflows/loaded-daemon-lifecycle.yml` — explicit 840-second test-only
  liveness environment for the loaded lifecycle command; no outer deadline,
  input, load, concurrency, or cleanup change.
- `docs/plans/eliminate-same-url-webrtc-reload-bridge-health-race-under-load.md`
  — this reviewable plan artifact.
- `script/run-loaded-daemon-lifecycle`, `src/entrypoint_supervisor.rs`,
  `src/daemon_transport.rs`, and `src/local_webrtc.rs` — runtime and verification
  surfaces inspected or invoked but expected to remain unchanged.

Required docs/plugin README updates: none. This changes internal regression
synchronization and diagnostics, not a user-facing or plugin UI contract.

## Risks

- **Later metadata is mistaken for listener health.** Only exact `/health`
  succeeds readiness; `local_url` is asserted afterward.
- **The new wait hides a dead or stuck bridge.** Fail immediately on terminal
  state and expire at the human-approved 60/840-second liveness backstop with
  app/daemon/process/HTTP diagnostics.
- **A stale or wrong app row short-circuits readiness.** Match package,
  entrypoint, and exact expected URL. A published non-matching URL fails fast
  with both strings; it never waits until the backstop.
- **Generic helper changes destabilize unrelated tests.** Prefer a surgical
  same-URL helper; touch the existing generic helper only if its contract and all
  callers stay unchanged or improve for the same reason.
- **Health failure remains opaque.** Capture response status/body or connection
  error together with the last daemon projection and supervised process state.
- **The test passes without exercising fresh grants.** Preserve all post-ready
  wrong-origin, distinct-grant, wrong-secret, encrypted request, second-peer, and
  session-continuity assertions.
- **A green focused run is mistaken for loaded proof.** Require the existing
  residual-tail default-parallel lifecycle campaign; isolated and
  single-threaded runs are diagnostic only.
- **A different first root is waived as pre-existing.** Identify the first
  non-cascade panic and compare the exact test on branch/base before requesting a
  human waiver; poisoned-lock cascades do not excuse it.
- **The implementation drifts into production readiness policy.** Plan Review
  should reject supervisor/daemon/transport edits unless new captured evidence
  disproves the test-only diagnosis and a human expands scope.
- **Fault injection leaks into product behavior.** Keep the startup delay in the
  generated test fixture, require an explicit test environment override, and
  prove normal packages and production code have no delay path.
- **The unexplained 4m40s is misattributed.** Timestamp every upstream phase and
  report raw elapsed values before drawing a new root-cause conclusion.

## Acceptance checks/tests

1. Static scope checks:
   - `git diff --check` passes.
   - `git diff --stat main...HEAD` contains only the plan and the surgical test
     file plus the loaded workflow's test-liveness environment unless an
     implementation artifact explains a directly necessary addition.
   - `rg` confirms the only timing changes are the 60-second default liveness
     backstop, explicit 840-second loaded override, and fixture-only injected
     delay; no workflow outer deadline, stress setting, Cargo concurrency flag,
     WebRTC assertion, or production transport path changed.
2. Focused local behavior:
   - Run
     `./test.sh --test hub_daemon_lifecycle_test botster_web_same_url_reload_issues_fresh_local_webrtc_bootstrap -- --exact --nocapture`.
   - The output must execute exactly one test and pass through the real daemon,
     supervised delayed package entrypoint, HTTP health, exact structured app projection,
     fresh bootstrap, and two encrypted WebRTC peers.
   - Run the focused test repeatedly enough to exercise clean process teardown;
     do not add retries to product code or treat repetition as a substitute for
     the loaded gate.
3. Deterministic negative control:
   - Keep the fixture startup delay above the old two-second ceiling, revert only
     the condition-driven helper to the old 100x20ms behavior, and run the focused
     exact test without synthetic load. It must fail at bridge readiness on every
     control run.
   - Restore the helper and rerun the byte-identical focused command. It must pass
     after the injected delay through health, exact `local_url`, and both peers.
   - Attach delay value, raw commands, exact SHA, exit codes, phase timestamps,
     and first panic. There is no stochastic escape clause: missing red-on-revert
     evidence blocks acceptance.
4. Repo gates:
   - `./test.sh --test hub_daemon_lifecycle_test -- --nocapture` passes at default
     Cargo parallelism.
   - `cargo fmt --check` and the repository's enforced strict Clippy command pass
     if Rust code formatting or lint-visible helper code changes.
5. Binding loaded acceptance:
   - Dispatch `.github/workflows/loaded-daemon-lifecycle.yml` against the exact
     implementation SHA with `test_target=lifecycle-suite`, at least `20`
     repetitions, and `stress_profile=residual-tail`.
   - Require the workflow to precompile the exact lifecycle target before load,
     execute `./test.sh --test hub_daemon_lifecycle_test -- --nocapture` at
     default parallelism, and stop at the first red run.
   - Confirm the test receives the explicit 840-second liveness backstop while
     the runner retains its existing 900-second outer deadline. Readiness must
     still be reported only by exact `/health`, never by elapsed time.
   - Attach workflow URL, artifact metadata, exact SHA, completed repetition
     count, raw run logs, load samples, cleanup evidence, and all statuses. Every
     requested repetition must pass; any other first-root failure remains red
     pending exact unrelatedness proof and human disposition.
6. Runtime-path proof:
   - The passing log or focused diagnostic must show the real
     `StartPackageEntrypoint` path, delayed listener, successful exact `/health`,
     app `local_url` equal to
     `http://127.0.0.1:{web_bridge_port}/?dogfood=real-hub`, two distinct
     bootstrap grants, and successful encrypted requests over both peers. It must
     also timestamp worker build, daemon start, package enable, entrypoint return,
     health, and URL publication. Code presence alone is not acceptance.

## Pipeline gates and artifacts

- Plan: attach this document with explicit assumptions and checklist evidence,
  submit `botster_plan_gate`, then request advancement.
- Plan Review: reject readiness-budget inflation, liveness values other than the
  human-approved 60/840 contract, production launch/transport changes,
  metadata-only readiness, missing phase/process/daemon/HTTP diagnostics,
  weakened WebRTC assertions, or acceptance that omits deterministic
  negative-control and loaded proof.
- Implement: attach the exact diff summary, commands, raw focused result, explicit
  assumptions that remained valid or changed, and any newly discovered root.
- Review: inspect test correctness, hidden unbounded behavior, exact app-row
  matching, diagnostics, teardown, scope, dead helpers, and negative-control
  evidence.
- Verify: independently rerun local gates and require the exact-SHA 20+
  residual-tail default-parallel artifact before approval.

## Vault gaps worth capturing

- The residual exposes a candidate durable testing gotcha: supervised web
  entrypoint tests should use direct health as readiness, terminal process state
  as immediate failure, and declared structured launch results as later contract
  evidence; liveness backstops prevent hangs but never prove readiness. Capture
  this through the vault inbox only after implementation and deterministic
  negative control prove it; connect it to `[[packaged botster web reloads need fresh webrtc grants]]`,
  `[[PTY integration tests poll for readiness not fixed sleeps]]`,
  `[[botster runnable entrypoints are hub owned launch contracts]]`,
  `[[botster-architecture]]`, and `[[cli-patterns]]`.
- Capture a separate production readiness-contract note only if new evidence
  proves `StartPackageEntrypoint` or supervisor semantics are defective. The
  current evidence does not justify recording that inference as fact.
- No Plan-stage vault write is warranted. Implement/Verify must record the inbox
  capture path or explicitly state why the candidate was not confirmed.

## Checklist evidence

- Project Pipelines checklist instructions were loaded. Standard vault and
  custom Plan checklist creation calls returned `plugin worker invoke timeout`,
  but Plan Review proved the writes had persisted. Current context was re-read;
  both vault checklists (`checklist_1784234966_947279` and
  `checklist_1784235080_820558`) are now complete, and the custom workflow
  checklist (`checklist_1784234971_172243`) records the runtime trace, revised
  scope, human decision, and gate progress.
- Vault notes and project constraints read are listed under **Context loaded**.
- Convention conflicts: none after human clarification. The plan uses direct
  health readiness, later app projection, immediate terminal failure, explicit
  liveness-only backstops, the repo wrapper and default parallelism, and adds no
  dependency or production abstraction.
- Verification evidence required is enumerated under **Acceptance checks/tests**;
  current evidence is planning inspection plus raw cited-run diagnostics, not a
  claim that implementation is already green.
- Durable capture disposition is recorded under **Vault gaps worth capturing**;
  no unverified Plan-stage note was written.
- Checklist persistence is available despite caller-side timeouts. Future agents
  must re-read current context after a timeout before claiming a failed write.
