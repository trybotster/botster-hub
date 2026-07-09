# Prove Many PTYs Plus Client Attach

## Context loaded

- Pipeline: ticket `ticket_1783552998_811867`, run `run_1783636084_849761`, Plan step `botster_plan`, gate `botster_plan_gate`. All three dependencies are closed. There are no prior artifacts, findings, reviews, questions, or answers to reconcile.
- Ticket intent: add one joint adversarial product-path proof with many worker-backed PTYs, a noisy session, quiet completion, late client attach/drain, input, history/readback, cleanup, stable failure labels, and a documented CI/local command.
- Role and repo overlays: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]].
- Binding architecture notes: [[botster local client api lives over hubruntime not raw core routers]], [[coredaemon must expose terminal truth used by the production hub path]], [[botster data plane bypasses the hub through session and client actors]], [[external client hub tests use subprocess spawned hub test support]], [[test script required for rust tests not cargo test]], and [[subprocess harnesses must kill child on failed readiness]].
- Pipeline discipline: [[plan steps need reviewable plan artifacts]], [[plan agents must author vault context as wikilinks not home paths]], and [[project pipelines checklist worker timeouts require artifact evidence fallback]]. The initial vault-checklist creation call timed out, but inspection confirmed that it durably created checklist `checklist_1783636165_930944`; gate evidence and this artifact remain the fallback record.
- Repo evidence: `botster-hub-test-support` already owns `IsolatedHubBuilder` and public client conformance helpers; `tests/hub_daemon_lifecycle_test.rs` already drives the harness against real `botster-hub` and `botster-session-worker` subprocesses; `docs/client-protocol.md` documents the downstream-shaped live-hub workflow. The daemon socket adapter routes public requests through `HubClientApi::handle_request`, `HubRuntime`, and the production `CoreDaemon`/session-worker path.

## Scope

- Extend the existing hub test-support crate with one focused adversarial helper and a stable report/error classification for the required stages: `spawn`, `attach`, `drain`, `input`, `history`, and `cleanup`.
- In one isolated live hub, spawn a fixed set of uniquely named worker-backed sessions through public `botster-hub-client` requests. One session emits enough labeled output to exercise history/egress while remaining interactive; the other sessions emit distinct completion markers and exit without waiting on the noisy session.
- Prove quiet-session completion while the noisy PTY remains live by bounded polling of public `ListSessions` until every quiet row has `lifecycle == "exited"`. Do not attach to quiet sessions: `ProcessExit` is subscription-scoped, and attaching them would erase the intended distinction between unblocked quiet completion and the one late noisy-session attach. Then late-attach a client to the noisy session, read/drain prior history, send labeled input, and observe later live output in order.
- Exercise `ReadScreen` and `CaptureSnapshot` through the same public client boundary so the recently landed production CoreDaemon readback path is part of the joint proof, not merely a DTO assertion.
- Attempt explicit per-session teardown on every return path and surface cleanup failures with the `cleanup` label; keep `IsolatedHub` shutdown/drop as the outer process-level fallback.
- Add a CI-safe integration test with a small fixed count and a clearly ignored larger local case that invokes the same helper with a larger fixed count. Avoid a new command-line/configuration abstraction.
- Document the exact default and ignored commands and what product path they prove.

Botster layers touched: Rust hub test-support, daemon/client integration tests, and client-protocol documentation. The exercised production layers are the daemon socket adapter, `HubClientApi`, `HubRuntime`, `CoreDaemon`, `SessionIo`/`ClientWorker`, and worker-backed PTYs; production implementation changes are not expected.

## Non-scope

- Performance targets, throughput claims, soak testing, benchmarking, or tuning queue/backpressure constants.
- Cloud, Rails relay, browser UI, WebRTC, TUI rendering, plugin workflow policy, spawn-target/worktree changes, or package lifecycle changes.
- New daemon protocol requests, new core primitives, alternate PTY/session implementations, or raw core-router test shortcuts.
- Broad refactors of the existing client conformance helper, daemon lifecycle suite, runtime ownership, or cleanup machinery.
- Publishing/versioning the Rust or npm test-support packages unless implementation unexpectedly changes a published wire/fixture contract; the planned report/helper is test API only and does not change daemon DTOs.

## Assumptions and unknowns

### Assumptions

- The assigned worktree and explicit target `tgt_7e208a0c76a44980a83b63af976b1f22` are authoritative; no agent spawn or alternate checkout is required.
- “Many” is a concurrency/adversarial proof, not a performance threshold. Use small deterministic constants (proposed: 8 total sessions for CI and 32 for the ignored local case) and adjust downward only if measured CI evidence requires it; do not weaken the scenario shape.
- “Client attach/drain” is satisfied by `botster_hub_client::DaemonConnection` issuing public `Attach` and `Drain` requests. Direct core/session-worker frames do not satisfy the ticket.
- “History” means renderable prior terminal output is visible through late attach/drain and production readback (`ReadScreen`/`CaptureSnapshot`) before later input-driven output. The test should assert history-before-live ordering where the public event stream exposes it.
- Failure labels are stable machine-readable stage values, with a session identifier/details carried separately in a narrow additive test-support error type. Do not overload `ConformanceError.operation`, whose vocabulary describes individual calls and cannot carry a dynamic session identifier.
- A quiet session that is successfully created but does not reach `exited` before the bounded `ListSessions` deadline is reported as `spawn` for that session. This preserves the ticket's exact six-label set: the spawn stage owns bringing every requested quiet workload to its expected terminal state.
- The test is Unix-only like the existing real-PTY lifecycle coverage.

### Unknowns to resolve during implementation

- The smallest noisy-output volume that reliably exercises retained history without turning the CI case into a throughput benchmark. Measure the focused test and keep the marker-based assertion, not an exact chunk count.
- Whether the late attach produces `Snapshot` or `Scrollback` for this scenario. Accept either authoritative history variant, require non-empty renderable `data` with `bytes == data.len()`, and do not encode one backend-specific variant.

No human question blocks this plan. If implementation discovers that one of the six required stages cannot be exercised through the public hub/client path, it must ask rather than waive or substitute a raw core proof.

## Affected surfaces/files

- `crates/botster-hub-test-support/src/lib.rs`
  - Add the reusable many-PTY adversarial helper, compact report, stage labels, bounded polling, marker assertions, and best-effort cleanup aggregation.
  - Reuse `IsolatedHub`, `DaemonConnection`, existing request/response checks, standard-library threads/timing, and current `ConformanceError` patterns where they remain clear.
- `tests/hub_daemon_lifecycle_test.rs`
  - Add one serialized CI-safe real-daemon test and one `#[ignore]` larger local test, both building the real worker path and calling the shared helper.
  - Assert report counts/stages and explicit hub shutdown, without duplicating the scenario inline.
- `docs/client-protocol.md`
  - Document the helper, scenario, fixed CI/local sizes, exact `./test.sh` commands, expected runtime boundary, and the fact that this is correctness/adversarial evidence rather than a benchmark.
- `docs/plans/prove-many-ptys-plus-client-attach.md`
  - This reviewable Plan artifact.

Expected unchanged: `src/runtime.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `crates/botster-hub-client`, `Cargo.toml`, `Cargo.lock`, and core dependencies. Touch them only if the public path is genuinely unwired; that would require plan-review visibility before expanding scope.

## Implementation sequence

1. Define the minimal stable stage/report shape beside the existing client conformance report. Use a narrow additive error type with one of the six static stage labels plus a dynamic synthetic `session_id`; keep session counts and observed marker/history facts, not raw timing or full output buffers.
2. Implement the shared helper using only public client calls: confirm an empty/known baseline, spawn the noisy PTY plus quiet PTYs, bounded-poll `ListSessions` until every unattached quiet session is `exited`, wait for a noisy pre-attach marker, late attach only the noisy session, verify screen/snapshot/history, send input, drain until the live marker appears after history, and list/clean every created session. Polling must have a fixed deadline and may sleep between requests, but sleep alone is never a success condition. A quiet-completion timeout is a `spawn` failure for the affected session.
3. Structure cleanup so every created session is attempted even after an earlier stage error. Preserve the primary failure unless cleanup is the only failure; make cleanup evidence available without leaking local paths.
4. Add the small default and larger ignored daemon lifecycle tests around `IsolatedHubBuilder`, using the existing global daemon-test lock and explicit shutdown.
5. Add the exact run instructions and proof boundary to `docs/client-protocol.md`.

## Risks

- False product-path proof: linking hub internals or sending raw core/session-worker frames would bypass the production client boundary. The test must enter through `botster-hub-client` and the real daemon socket.
- Scheduler/timing flakiness: PTY output chunks and exit timing are nondeterministic. Use unique markers, accumulated observations, bounded deadlines, and no sleep-only success condition.
- Noisy-session starvation or oversized CI work: an excessive output volume/count turns correctness coverage into an unstable stress benchmark. Keep the default bounded and the larger case ignored.
- History ambiguity: snapshot versus scrollback is backend-dependent. Assert renderable content and ordering, not a single variant, while separately exercising `ReadScreen` and `CaptureSnapshot`.
- Cleanup masking: early returns can orphan worker processes, while a cleanup error can hide the actual attach/input/history failure. Track created sessions, attempt all shutdowns, and report primary and cleanup evidence deliberately.
- Global daemon-test serialization makes focused tests slow. Keep one shared scenario helper and avoid multiplying fresh hubs inside the default case.
- Public test-support API sprawl: add only the helper/report/stage surface required by downstream-shaped proof; do not generalize into a configurable load-testing framework.
- PII/path leakage: stage errors and reports must use synthetic IDs and sanitized diagnostics, not data-dir, socket, binary, or home paths.

## Acceptance checks/tests

Runtime assertions:

- The isolated hub starts with the explicit real hub and session-worker binaries and the public status/list path is compatible.
- The requested total number of distinct sessions is spawned through `DaemonRequest::Spawn`; failures identify `spawn` and the affected synthetic session.
- Quiet sessions reach their completion observation while the noisy session remains interactive.
- Quiet sessions are never attached. Bounded public `ListSessions` polling observes every quiet session at `lifecycle == "exited"` while the noisy session remains interactive; deadline expiry is a `spawn`-labeled failure for the affected quiet session, and no sleep-only success check is permitted.
- A late public client attach to the noisy session succeeds. Its `DaemonEvent::Snapshot` or `DaemonEvent::Scrollback` carries non-empty renderable `data` with `bytes == data.len()` and is ordered before later `TerminalOutput` for the same subscription. A positive history byte count with empty `data` is a `history`-labeled failure, not a pass.
- `DaemonReadScreen.text` contains the noisy pre-attach marker.
- `DaemonCaptureSnapshot` has `payload_bytes > 0` and `payload_format == Some("plain-opaque-v1")`; it is opaque and has no renderable `data` assertion.
- Public `SendInput` reaches the noisy PTY and its labeled response is observed through public drain.
- Every created session receives cleanup, the hub remains responsive, and explicit isolated-hub shutdown succeeds.
- All surfaced failures use exactly one of `spawn`, `attach`, `drain`, `input`, `history`, or `cleanup`; reports/errors contain no host-specific paths.

Commands:

- `cargo fmt --check`
- `./test.sh -p botster-hub-test-support`
- `./test.sh --test hub_daemon_lifecycle_test external_hub_client_many_pty_adversarial_conformance_ci`
- `./test.sh --test hub_daemon_lifecycle_test external_hub_client_many_pty_adversarial_conformance_local -- --ignored --exact`
- Re-run the existing adjacent regression proofs: `./test.sh --test hub_daemon_lifecycle_test external_hub_test_support_drives_isolated_daemon_socket_protocol` and `./test.sh --test hub_daemon_lifecycle_test external_daemon_attach_replays_prior_history_with_renderable_byte_count`.
- Run `cargo clippy --all-targets --all-features -- -D warnings` after focused tests. If the full repository gate is practical, run `./test.sh`; otherwise record the exact skipped breadth and why.
- Run the two commands exactly as documented in `docs/client-protocol.md`, including the ignored local case, and record elapsed time/session counts as verification context only—not a performance claim.

Production-path evidence required in implementation/review: show that the test calls `botster-hub-client` over the daemon socket, that `src/daemon_transport.rs` routes the exercised requests into `HubClientApi::handle_request`, and that the hub runtime uses the worker-backed CoreDaemon/session path. Code existence or report construction alone is insufficient.

## Pipeline gates and artifacts

- Plan gate artifact: this document plus gate fields for context, scope, assumptions/unknowns, files, risks, checks, and vault gaps.
- Plan Review should reject any proposal that replaces the joint runtime scenario with separate unit tests, bypasses the public hub boundary, omits a required failure label, or leaves the larger/default run policy undocumented.
- Implementation evidence must include the focused CI test, the ignored local test, adjacent conformance/history regressions, formatting/lint results, exact docs command output, and runtime entry-point trace.
- Review/Verify should inspect cleanup on all error paths, stable label coverage, history-before-live semantics, path leakage, dead/unwired helper code, and whether the ignored test truly uses the larger case.

## Vault gaps worth capturing

- If the implementation settles a reusable rule for composing many-PTY fairness, late attach, history, and cleanup in one public-client harness, capture it as a Botster testing pattern after verification.
- If stable stage-labeled conformance errors become a repeated contract across test-support helpers, capture the convention; one isolated enum does not yet justify a note.
- If measured CI behavior establishes a durable safe default/ignored session-size boundary, capture the rationale rather than the incidental number.
- The checklist worker timeout is already covered by [[project pipelines checklist worker timeouts require artifact evidence fallback]]; no duplicate vault capture is needed.
- No convention conflict was found. The plan reinforces the existing public-client, HubRuntime/CoreDaemon, subprocess-harness, cleanup, and `./test.sh` conventions.
