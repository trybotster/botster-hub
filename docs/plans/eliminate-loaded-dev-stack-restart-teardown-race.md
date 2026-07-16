# Eliminate the loaded dev-stack restart teardown race

## Context loaded

- Project Pipelines ticket `ticket_1784168174_908058`, run `run_1784222792_904056`, returned Plan run step `run_step_1784224285_491552`, gate `botster_plan_gate`, prior plan artifact `artifact_1784223010_464883`, Plan Review `review_1784223490_309578`, all five review findings, and human answer `question_1784224314_881528`.
- Required role context: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]]. Applicable layer: Rust hub/client lifecycle; no Rails, Hotwire, TUI, Lua, SPA UI, or plugin-policy change.
- Lifecycle constraints: [[daemon shutdown disconnects count as success only after clean owned process exit]], [[daemon probe order changes require lifecycle integration tests]], and [[test script required for rust tests not cargo test]].
- Human decision: fix the production probe. Normalize `ConnectionReset`, `BrokenPipe`, and `UnexpectedEof` at the hub-client transport boundary as daemon-disconnected outcomes; preserve unrelated I/O, JSON, protocol, compatibility, and authorization failures as fatal. Do not add a test-only PID/socket wait.
- Production path: `shutdown` and `down` return after the daemon acknowledges shutdown. An immediate `dev-stack bootstrap` or `up` calls `prepare_local_runtime` -> `ensure_dev_stack_daemon` -> `daemon_transport_request(Status)`. That policy already spawns a replacement for `NotRunning` and `ClientDisconnected`, but the hub-client boundary currently leaves reset/broken-pipe/unexpected-EOF errors as `Io`, causing the catch-all fatal branch.
- Campaign reconciliation: retained GitHub Actions run `29466442155`, artifact `loaded-daemon-lifecycle-29466442155-1`, tested exact subject `e6ed8fa780b2d3a5fb4dbee3db842ecaa92f3f44` with `lifecycle-suite`, 20 requested repetitions, and `residual-tail`. Repetition 1 panicked in the named test at that subject's line 4232 with `Connection reset by peer (os error 104)`. The ticket's line 4228 is a four-line summary drift; it does not identify a different test. At the current checkout the named test starts at line 4105 because later commits shifted the file.

Botster layer: public Rust hub-client transport classification plus real CLI lifecycle integration coverage. Target `tgt_7e208a0c76a44980a83b63af976b1f22` and the assigned ticket worktree remain binding.

## Scope

- Add one private hub-client I/O normalization function that maps only `ConnectionReset`, `BrokenPipe`, and `UnexpectedEof` to the existing `DaemonTransportError::ClientDisconnected`; all other `io::Error` values remain `DaemonTransportError::Io` with their original diagnostics.
- Route client-boundary socket I/O mappings used by connect/hello, frame writes, cloned-stream setup, and frame reads through that normalization where those three disconnect kinds can surface. Reuse the existing error variant and the existing `ensure_dev_stack_daemon` replacement policy; add no new public enum or startup policy branch.
- Add focused hub-client unit coverage for all three normalized kinds and at least one unrelated kind remaining fatal.
- Preserve daemon-side cleanup for connections that now surface one of the normalized disconnect kinds, and let the startup socket probe treat a normalized disconnect during its hello write like its existing failed-ack path: the old daemon is not live, so startup may unlink and bind.
- Preserve the existing immediate, unbarriered `shutdown` -> `dev-stack bootstrap` sequence in `cli_dev_stack_bootstrap_reuses_live_daemon_and_preserves_state_after_restart`.
- Extend `cli_local_runtime_up_starts_reuses_and_down_stops_runtime` to perform an immediate unbarriered `down` -> `up`, assert a fresh daemon starts and the runtime is ready, then clean it up. This proves the second reachable user path selected by the human.
- Verify red when the normalization is reverted and green under the same loaded default-parallel campaign.

## Non-scope

- No test-only wait for daemon PID exit, socket disappearance, metadata cleanup, sleep, retry, timeout increase, serialization, failure waiver, or manual cleanup between shutdown and restart.
- No change to daemon shutdown response ordering, `daemon.stop()`, socket cleanup, metadata format, readiness probe order, `ensure_dev_stack_daemon` policy, or stale-daemon recovery.
- No broad transport-error rewrite: permission, resource exhaustion, invalid data, JSON, protocol, compatibility, authorization, and unknown I/O failures remain fatal.
- No stalled-attach shutdown-response work, loaded-runner/CI changes, dependency changes, refactors, or adjacent cleanup.

## Assumptions and unknowns

### Assumptions

- The human answer is binding: teardown-shaped reset/broken-pipe/unexpected-EOF signals mean the probed daemon connection is no longer usable, so the existing `ClientDisconnected` semantic is correct at the shared client boundary.
- Normalization belongs beside socket I/O because every caller should receive the same transport meaning; `src/main.rs` should consume the existing semantic rather than inspect OS error kinds itself.
- The immediate restart tests must continue probing during teardown. Waiting for completion in test code would mask the production race and is explicitly rejected.
- The named failing test and exact subject are authoritative; the ticket's line mismatch is documented source drift, not permission to retarget.
- Replacement startup during teardown is safe because `serve_daemon` unlinks the old socket before returning and dropping its listener. An observed reset therefore follows unlink, so the replacement binds a fresh path. This ordering is load-bearing because `rebind_missing_socket_path` is currently a no-op and must not be reordered as part of this ticket.

### Unknowns

- The exact operation that produced the retained `ConnectionReset` (connect, hello write, hello read, request write, or response read) is not recorded. Applying the narrow classifier consistently at the client I/O boundary avoids guessing while focused tests prove the allowed set.
- `BrokenPipe` and `UnexpectedEof` were selected by the human as equivalent teardown outcomes but were not the retained campaign's observed error. Unit tests must prove their classification; integration/load evidence primarily proves the observed reset path.
- The loaded suite may expose independent failures. A non-target red must be preserved and investigated; it cannot substitute for reverted-normalization proof or be waived to claim green.

No further human question blocks this revision. Any proposal to widen the normalized error set, add waiting/retries, change shutdown semantics, or waive loaded failures requires a new question.

## Affected surfaces/files

- `crates/botster-hub-client/src/lib.rs` — primary production change and focused classification tests.
- `tests/hub_daemon_lifecycle_test.rs` — preserve shutdown/bootstrap coverage and add immediate down/up coverage without a wait.
- `docs/plans/eliminate-loaded-dev-stack-restart-teardown-race.md` — revised Plan artifact.
- `src/main.rs` — read-only wiring evidence: `ensure_dev_stack_daemon` already replaces `ClientDisconnected`; `local_runtime_down`, `dev_stack_bootstrap`, and `local_runtime_up` are the production entry points.
- `src/daemon_transport.rs` — deliberate behavioral consumer: normalized request-read disconnects take the existing subscription-detach path, and a normalized hello-write disconnect in `prepare_socket_path` falls through to the existing stale-socket cleanup rather than aborting startup. The focused daemon test characterizes the pre-existing graceful-EOF detach path; the loaded reset campaign proves normalization only through the client-side replacement path.
- `script/run-loaded-daemon-lifecycle`, `.github/workflows/loaded-daemon-lifecycle.yml`, and `docs/loaded-daemon-lifecycle-runner.md` — unchanged acceptance path.

Every changed line must implement the three-kind normalization, prove the fatal negative boundary, or preserve one of the two immediate user restart paths.

## Implementation plan

1. In `crates/botster-hub-client/src/lib.rs`, introduce the smallest private conversion from `io::Error` to `DaemonTransportError`. Return `ClientDisconnected` for exactly `ConnectionReset`, `BrokenPipe`, and `UnexpectedEof`; otherwise retain `Io(error)`.
2. Replace direct `map_err(DaemonTransportError::Io)` at client socket I/O boundaries with the helper. Keep connect-time `NotFound`/`ConnectionRefused` mapped to `NotRunning` before applying the new disconnect classifier to the remaining kinds.
3. Add focused unit tests that construct each selected `io::ErrorKind`, assert `ClientDisconnected`, and assert an unrelated kind remains `Io` with its kind preserved. Do not test by string matching OS messages.
4. Characterize the daemon's existing graceful-EOF request-read branch detaching active subscriptions, and make `prepare_socket_path` deliberately fall through when its hello write returns the normalized disconnect semantic. Keep all other probe write errors fatal. Do not claim the graceful-EOF test directly exercises reset normalization; the concrete `BufReader<UnixStream>` path has no portable test-only reset injection seam.
5. Leave `ensure_dev_stack_daemon`'s `NotRunning | ClientDisconnected` replacement arm unchanged. Its existing production call chain is the wiring proof that normalized errors spawn a fresh daemon.
6. Keep `cli_dev_stack_bootstrap_reuses_live_daemon_and_preserves_state_after_restart` immediate and unbarriered. Extend the local runtime lifecycle test with immediate `down` -> `up`, fresh-start/readiness assertions, and normal final shutdown.
7. Produce deterministic red proof by reverting only the normalization while retaining its focused unit tests. Produce integration red proof under the unchanged default-parallel residual-tail campaign, tied to an exact reverted subject SHA and the named immediate restart failure. Restore the normalization and run the identical campaign green.

## Risks

- **Over-normalization:** mapping generic I/O failures to disconnect would hide actionable errors. Match exactly the three human-approved `ErrorKind` values and prove an unrelated kind stays `Io`.
- **Partial boundary wiring:** fixing only the observed read or connect site may leave the same semantic failure at hello/request writes. Route all relevant hub-client socket I/O conversions through one private classifier.
- **Policy duplication:** matching OS kinds again in `ensure_dev_stack_daemon` would make callers diverge. Normalize once in the client crate and reuse `ClientDisconnected`.
- **Regression test masking:** any wait, retry, or cleanup before restart removes the failing window. Preserve both immediate sequences.
- **False loaded proof:** a green campaign with a test wait or a red campaign caused only by another test proves nothing. Compare exact subjects/diffs and retain the target failure.
- **Public API drift:** adding a new error variant would affect downstream exhaustive matches. Reuse the existing variant.
- **Shared-client behavior change:** callers outside startup will now see the three connection-loss kinds as `ClientDisconnected`. That is intentional but requires the hub-client workspace tests and strict clippy to catch incorrect assumptions.
- **Daemon-side behavior change:** normalized request-read disconnects now detach subscriptions cleanly, and normalized hello-write disconnects no longer abort startup. The graceful-EOF detach branch is characterized directly, while reset-to-detach remains inferred from the shared classifier and match arm because a portable AF_UNIX reset trigger is unavailable in the current concrete reader; the loaded campaign directly proves only the client-side reset replacement path.

## Acceptance checks/tests

- Focused classifier: `./test.sh -p botster-hub-client tests::teardown_io_kinds_normalize_to_client_disconnected -- --exact --nocapture` executes the planned classifier matrix, proving all three approved kinds normalize and an unrelated kind retains `Io`.
- Daemon graceful-EOF cleanup characterization: `./test.sh --lib daemon_transport::tests::client_eof_detaches_connection_subscriptions -- --exact --nocapture` proves the pre-existing `ClientDisconnected` branch detaches active subscriptions. It is not regression proof for reset normalization and is expected to remain green when that normalization is reverted.
- Immediate shutdown/bootstrap: `./test.sh --test hub_daemon_lifecycle_test cli_dev_stack_bootstrap_reuses_live_daemon_and_preserves_state_after_restart -- --exact --nocapture` passes without any PID/socket wait, sleep, retry, or serialization.
- Immediate down/up: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime -- --exact --nocapture` passes and proves the post-`down` `up` reports `daemon=started` and `runtime=ready`.
- Relevant default-parallel target: `./test.sh --test hub_daemon_lifecycle_test -- --nocapture` passes with no `--test-threads=1` acceptance substitution.
- Workspace checks: `./test.sh --workspace`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `git diff --check` pass.
- Deterministic red when reverted: with only the production normalization reverted and focused tests retained, the classifier test fails for the approved kinds.
- Loaded red when reverted: exact reverted subject under `lifecycle-suite`, 20 repetitions, `residual-tail`, and default Cargo parallelism retains the named immediate restart failure with reset/disconnect evidence. Stop at the first red campaign run; do not retry past it.
- Loaded green: restored implementation completes all 20 repetitions under identical inputs. Record subject SHA, workflow/run and artifact IDs, commands, resource samples, campaign result, and cleanup status. No unrelated red is waived.
- Runtime wiring review: confirm `ensure_dev_stack_daemon` still consumes `ClientDisconnected` to call `spawn_dev_stack_daemon`, and confirm the integration diff contains no pre-restart completion barrier.

## Pipeline gates and artifacts

- The revised Plan artifact supersedes `artifact_1784223010_464883` and resolves every finding from `review_1784223490_309578` without waiver.
- Implement evidence must include changed files, exact SHA, classifier matrix, direct production call-chain wiring, both immediate lifecycle tests, and reverted-vs-restored diffs.
- Verify evidence must compare identical loaded inputs and reject test waits, retries, serialization, timeout changes, non-target red substitution, or code-existence-only claims.

## Vault gaps worth capturing

- The rejected first plan exposed a durable rule not yet captured narrowly: restart tests must preserve the real immediate lifecycle sequence; a test-local completion wait can mask a production probe-classification defect.
- The implementation may also validate a reusable client-boundary rule: reset, broken pipe, and unexpected EOF are connection-loss semantics, while unrelated I/O remains diagnostic-bearing and fatal.
- Capture only after implementation and loaded proof through the inbox-first vault workflow. Connect any validated note to [[daemon probe order changes require lifecycle integration tests]], [[daemon shutdown disconnects count as success only after clean owned process exit]], [[botster-architecture]], and [[cli-patterns]]. Until then, `capture_path: nil`.
