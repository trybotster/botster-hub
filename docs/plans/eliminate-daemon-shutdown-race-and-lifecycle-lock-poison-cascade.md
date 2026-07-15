# Eliminate Daemon Shutdown Race And Lifecycle-Lock Poison Cascade

## Context loaded

- Pipeline context: ticket `ticket_1784067574_620812`, run `run_1784073954_259950`, active Plan step `hotwire_plan`, gate `hotwire_plan_gate`, and no prior artifacts, findings, reviews, questions, answers, or blocking dependencies.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]], including the Botster architecture, CLI, and SPA maps and the pipeline/worktree notes required by that overlay. This is a Rust hub/test-support ticket, not a Hotwire-focused Rails app, so [[hotwire-app-planner-playbook]] does not govern the implementation.
- Controlling vault context: [[poisoned rust mutex test locks cascade one failure across parallel suite]], [[a poisoned test lock is a symptom not a waiver]], [[pre existing failure waivers must isolate the first non cascade failure on base]], [[daemon probe order changes require lifecycle integration tests]], and [[botster test sh forwards arguments to cargo not custom unit flags]].
- Repository baseline: the ticket branch is clean at `origin/main` commit `7f05059`. `test.sh` is `BOTSTER_ENV=test cargo test "$@"`. The public isolated-hub harness is in `crates/botster-hub-test-support/src/lib.rs`; its `shutdown_inner` returns `ShutdownFailed` immediately when the `botster-hub shutdown` client loses the response, before checking whether the owned daemon completed a clean exit.
- Production/runtime path inspected: `IsolatedHub::shutdown` and `Drop` invoke `shutdown_inner`; that launches the strict production `botster-hub shutdown` CLI, which sends `DaemonRequest::DaemonShutdown` through `daemon_transport_request`. The daemon marks the response as close-after-response and stops after returning the shutdown frame. The narrow race is therefore in test-harness cleanup observation, not shutdown command semantics.
- Shared test topology inspected: `tests/hub_daemon_lifecycle_test.rs` uses process-wide `REAL_DAEMON_TEST_LOCK` across the real-daemon tests. Every acquisition currently calls `.expect(...)`, so one cleanup panic poisons the lock and makes later tests fail with `PoisonError`.
- Originating and adjacent runtime proofs inspected: `external_hub_test_support_drives_isolated_daemon_socket_protocol` explicitly shuts down two public-harness daemons; the same public cleanup is used by contract-matrix, first-party dogfood, and many-PTY conformance tests. `external_daemon_same_session_reattach_replays_opaque_history_before_live_output` is the adjacent reconnect regression from the parent ticket and must remain unchanged in meaning.
- Workflow discipline: run checklist `Plan vault discipline` (`checklist_1784074099_172547`) records vault context, convention fit, planned verification, and capture disposition. Initial checklist creation timed out but committed; listing before retry avoided a duplicate.

## Scope

1. Make `IsolatedHub::shutdown_inner` wait for and classify the owned daemon process even when the shutdown CLI exits nonzero because its connection closed at the shutdown boundary.
2. Treat the shutdown CLI's exact `client disconnected` outcome as successful cleanup only when the owned daemon process itself exits successfully. Continue returning `ShutdownFailed` for other shutdown-command failures and `DaemonExited` for unsuccessful daemon exits.
3. Add deterministic test-support coverage for the shutdown result matrix so the race-tolerant branch is exercised without depending on a timing flake: disconnect plus clean daemon exit succeeds, an unrelated shutdown-command failure remains an error, and a failed daemon process remains an error.
4. Centralize acquisition of `REAL_DAEMON_TEST_LOCK` in a poison-recovering test helper and route every real-daemon lifecycle test through it. Recover the guard with the standard `PoisonError::into_inner` behavior so one root panic remains visible without releasing serialization or cascading into unrelated failures.
5. Preserve the public isolated-hub API, the production shutdown CLI/protocol, all daemon lifecycle test bodies, and the reconnect/history assertions.

## Non-scope

- No change to `src/main.rs`, `src/daemon_transport.rs`, `DaemonRequest::DaemonShutdown`, response ordering, daemon stop timing, or production CLI error policy.
- No retry loop, sleep, timeout increase, or acceptance of arbitrary shutdown errors.
- No change to terminal history DTOs, reconnect behavior, Core dependencies, generated protocol artifacts, package metadata, or the binary-safe history work from the parent ticket.
- No replacement of the standard-library mutex, new synchronization dependency, suite-wide single-thread requirement, or relaxation of real-daemon test serialization.
- No poison-handling retrofit for the separate MCP daemon lock unless implementation evidence shows this ticket's originating cascade uses it; current evidence identifies `REAL_DAEMON_TEST_LOCK` only.
- No adjacent refactor, documentation rewrite, or cleanup beyond the test-support lifecycle and shared lifecycle-test lock sites required by this ticket.

## Botster layers touched

- Rust hub test-support crate: isolated subprocess shutdown/cleanup classification.
- Rust hub integration-test harness: process-wide real-daemon serialization guard.
- Repository plan artifact and pipeline evidence.
- No plugin, Lua core, session/client worker behavior, TUI, React SPA, Rails relay, MCP contract, or production daemon transport layer changes.

## Assumptions and unknowns

- Determined: `botster-hub shutdown error: client disconnected` can mean the daemon accepted shutdown and exited before the CLI observed its final frame. A clean owned-child exit is the required corroborating evidence; the stderr string alone is insufficient.
- Determined: returning immediately on the shutdown-command failure leaves `Drop` to kill a daemon that may already be exiting and turns successful cleanup into a test panic when `shutdown()` is explicit.
- Assumption: the accepted stderr match should stay narrow to the production CLI's current shutdown prefix plus `client disconnected`, preferably after trimming trailing whitespace. If implementation discovers structured status or a typed error is available without changing production APIs, it may use that equivalent evidence, but must not broaden accepted failures.
- Assumption: the existing `ShutdownFailed` and `DaemonExited` variants are sufficient; no public error variant or compatibility layer is needed.
- Assumption: a small private lock-acquisition helper is preferable to repeating poison recovery at every test because it makes the invariant reviewable while retaining `std::sync::Mutex` and the existing process-wide lock.
- Unknown until implementation: the cleanest deterministic unit-test setup may use a private result-classification seam or controlled child processes inside the crate's existing Unix-only tests. It must prove the behavioral matrix, not merely a string predicate.
- Worktree/target assumption: all work remains in the pipeline-provided ticket worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`; no ambient checkout or second repository is involved.

## Affected surfaces and files

- `crates/botster-hub-test-support/src/lib.rs`: reorder/complete shutdown observation, narrowly classify disconnect-after-success, and add deterministic crate-local regression coverage.
- `tests/hub_daemon_lifecycle_test.rs`: add the poison-recovering guard helper and route all `REAL_DAEMON_TEST_LOCK` acquisitions through it without altering test semantics.
- `docs/plans/eliminate-daemon-shutdown-race-and-lifecycle-lock-poison-cascade.md`: this reviewable Plan artifact.
- No production Hub source, client DTO, generated artifact, lockfile, package, or external repository should change.

## Implementation sequence

1. Refactor isolated-hub cleanup so it records the shutdown command result, always takes and waits for the owned child once a shutdown command was launched, then classifies the combined command/process outcome. Preserve error detail and ensure `self.child` is cleared exactly once.
2. Add deterministic crate-local tests for accepted disconnect-after-clean-exit and rejected non-disconnect/failed-child outcomes. Show the accepting regression test fails with the tolerance removed or otherwise capture an equivalent controlled negative proof.
3. Add one private lifecycle-test guard function that locks `REAL_DAEMON_TEST_LOCK` and recovers a poisoned guard with `into_inner()`. Replace every direct real-daemon lock acquisition, including the two one-line variants, with that helper.
4. Run focused test-support, originating cleanup, reconnect, entire daemon lifecycle, and full workspace checks at the repository's supported default concurrency. Use `--test-threads=1` only to isolate the first root failure if a run goes red.

## Risks

- **False-success cleanup:** accepting any nonzero shutdown command could hide a missing daemon, incompatible protocol, or genuine operator failure. Mitigation: accept only the exact disconnect signature and only alongside a successful owned-child exit.
- **Error-priority ambiguity:** both the shutdown command and daemon process can fail. Mitigation: preserve the daemon's nonzero exit as `DaemonExited`, because it proves cleanup did not complete successfully; retain command stderr for the clean-child/non-disconnect `ShutdownFailed` case.
- **Child ownership regression:** taking the child too early or twice could prevent `Drop` from killing/waiting after an actual wait error. Mitigation: keep `Option<Child>` ownership transitions explicit and add failure-path coverage where practical.
- **Incomplete poison fix:** leaving one `.lock().expect(...)` site preserves a cascade path. Mitigation: scan the file for direct `daemon_test_lock`/`REAL_DAEMON_TEST_LOCK` acquisitions after the mechanical change.
- **Serialization accidentally removed:** poison resistance must not permit real daemons to run concurrently. Mitigation: return and hold the same mutex guard for each complete test body; only the poisoned-state handling changes.
- **Diagnostic command becomes a waiver:** a green `--test-threads=1` run would not prove the supported suite. Mitigation: require the lifecycle file and full `./test.sh` at default Cargo concurrency; isolate and report the first non-`PoisonError` failure if either fails.
- **Parent-ticket misattribution:** touching history/reconnect logic could blur the independent failure. Mitigation: keep those code paths unchanged and rerun their named regression as adjacent proof.

## Acceptance checks and tests

- Test-support behavior:
  - `./test.sh -p botster-hub-test-support` passes, including deterministic proof that shutdown-command `client disconnected` plus clean owned-daemon exit is accepted.
  - The same test surface proves an unrelated shutdown CLI failure still returns `IsolatedHubError::ShutdownFailed` and a nonzero daemon exit still returns `IsolatedHubError::DaemonExited`.
  - Controlled negative evidence shows the disconnect/clean-exit regression fails when the new tolerance is removed or bypassed.
- Originating production-path harness:
  - `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol -- --nocapture` passes and exercises two explicit `IsolatedHub::shutdown` calls through the real daemon/CLI path.
  - Run the originating test repeatedly if needed to observe stability, but do not substitute repetition for the deterministic test-support regression.
- Parent-ticket non-regression:
  - `./test.sh --test hub_daemon_lifecycle_test external_daemon_same_session_reattach_replays_opaque_history_before_live_output -- --nocapture` passes with its binary-safe history and detach/reattach assertions unchanged.
- Lock/cascade proof:
  - Source scan finds no direct poison-panicking acquisition of `REAL_DAEMON_TEST_LOCK`; all real-daemon lifecycle tests use the poison-recovering helper and still retain a guard for the test duration.
  - A focused crate-local/helper test or controlled test-only proof demonstrates that acquiring the serialization lock after a holder panic succeeds instead of returning a cascading `PoisonError`, while mutual exclusion remains intact.
- Supported suite gates:
  - `./test.sh --test hub_daemon_lifecycle_test` passes at default Cargo test concurrency. This is the serialized real-daemon suite because its tests retain `REAL_DAEMON_TEST_LOCK`; do not add `--test-threads=1` to final evidence.
  - `./test.sh` passes at default concurrency, reproducing the original full-suite execution path. If it fails, identify the first non-cascade panic and compare exact branch/base evidence before considering any waiver.
  - `cargo fmt --all -- --check` passes.
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes using raw exit status/diagnostics.
  - `git diff --check` passes, and the final diff contains only the plan plus the two expected Rust test surfaces unless implementation discovers and documents a required deviation.

## Runtime-path proof

`external_hub_test_support_drives_isolated_daemon_socket_protocol` is the in-repo downstream-shaped entry point: it constructs `IsolatedHub` with explicit binaries, runs real client and plugin conformance, and calls public `shutdown()` twice. The fix is wired when those calls flow through the revised `shutdown_inner`, invoke the real production CLI/protocol, wait for the owned daemon, and accept only a corroborated clean exit. The lock change is wired when the entire `hub_daemon_lifecycle_test` file acquires its shared guard through the new recovery helper. Static presence of either helper is not sufficient.

## Pipeline gates and artifacts

- Plan artifact: this file.
- Plan gate evidence must include all seven required fields, identify the Rust hub/test-support layer, name the target/worktree assumption, and link checklist `checklist_1784074099_172547`.
- Plan Review should reject production shutdown weakening, arbitrary string-error suppression, retries/sleeps, a final single-thread-only verification command, partial lock-site conversion, or any change to the parent ticket's history DTO/reconnect assertions.
- Implementation evidence must include the deterministic cleanup decision-matrix test, the poison-recovery proof, exact default-concurrency lifecycle/full-suite commands and exit status, and the source scan for remaining poison-panicking lock sites.
- Review and Verify must attribute any red suite to the first non-`PoisonError` panic; cascade similarity on base is not a waiver.

## Vault gaps worth capturing

- No new vault note is required at Plan time. [[poisoned rust mutex test locks cascade one failure across parallel suite]] and [[a poisoned test lock is a symptom not a waiver]] already capture the lock recovery and verification discipline, while [[daemon probe order changes require lifecycle integration tests]] captures adjacent shutdown-boundary races.
- Capture a new atomic note after implementation only if the combined shutdown-command/owned-child classification proves reusable beyond this harness: a disconnect at a destructive daemon command boundary is acceptable only when independently observed process state proves the requested transition completed.
- If implementation reveals a distinct child-ownership/Drop invariant not covered by existing Botster subprocess notes, capture that separately after behavior is verified.

## Convention check

No convention conflict or waiver is required. The plan uses standard-library process and mutex primitives, keeps production policy strict, makes the smallest test-support change that addresses the observed race, preserves serialized real-daemon tests, keeps `--test-threads=1` diagnostic-only, and leaves the parent ticket's binary-safe history work untouched.
