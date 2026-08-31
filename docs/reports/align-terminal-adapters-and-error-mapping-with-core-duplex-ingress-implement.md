# Hub terminal adapter alignment implementation report

Ticket: `ticket_1788137128_417142`

Run: `run_1788138132_150545`

## Target

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Run worktree branch: `project-pipelines/ticket_1788137128_417142`.
- Base: `main` at `c674a62`.
- Core candidate: `a781556258789dea4a50ffcb17351e7294c8ff26`.

The ticket target selected the repository. The ambient directory did not select the repository.

## Applied guidance

The implementation used these playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[project-pipelines-playbook]]

The implementation used these targeted notes:

- [[botster-architecture]]
- [[cli-patterns]]
- [[botster runtime teardown lenses]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[botster hub is a first party host profile over core]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[botster terminal egress is session backed only]]
- [[Core reports terminal mechanism capabilities and Hub admits their use]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[colon worktree paths break cargo dyld library paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[each acceptance condition names its authoritative production oracle]]
- [[dependency closure must requeue the blocked parent step]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[core terminal progress is wake driven and targeted]]

## Implementation

The change rolls all Hub Core pins to the exact candidate revision. The dependency graph contains one terminal protocol source.

`AdapterSlot` now owns a bounded ingress queue. It reports states in this order: `Closed`, `Lost`, `Frame`, and `Empty`.

The Unix and WebRTC adapters implement `TerminalAdapter::try_read`. Their harness drivers implement all required ingress operations.

The runtime maps both new `ControlPlaneFailed` variants to distinct error classes. The live bind paths use these mappings.

The route test uses the sanctioned test seam. It proves that ingress loss retires the exact route and preserves a sibling.

The session cleanup fixture accepts `SessionCleanup` only for the expected session and the `already_exited` outcome. It waits for authoritative exit and then calls `RemoveSession`. Live shutdown checks still require `Events`.

## Files changed

- `Cargo.lock`
- `Cargo.toml`
- `crates/botster-hub-client/Cargo.toml`
- `crates/botster-hub-test-support/Cargo.toml`
- `crates/botster-hub-test-support/build.rs`
- `crates/botster-hub-test-support/src/conformance_data.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `docs/plans/align-terminal-adapters-and-error-mapping-with-core-duplex-ingress.md`
- `docs/reports/align-terminal-adapters-and-error-mapping-with-core-duplex-ingress-implement.md`
- `src/runtime.rs`
- `src/subscription/attach_routes.rs`
- `src/transport/shared/adapter_slot.rs`
- `src/transport/unix/adapter.rs`
- `src/transport/webrtc/adapter.rs`
- `tests/hub_daemon_lifecycle/event_plane_saturation.rs`
- `tests/hub_daemon_lifecycle/package_event_plane.rs`
- `tests/hub_daemon_lifecycle/session_fixtures.rs`
- `tests/hub_daemon_lifecycle/sessions.rs`
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`
- `tests/session_projection_owner_loop.rs`

## Ownership boundaries

Hub owns the concrete adapters, the ingress storage, the error classes, and the Hub fixtures. Core still owns the adapter contract and teardown policy.

The adapters store opaque bytes. The adapters do not parse terminal input.

The implementation does not add a Unix producer, a WebRTC producer, a wake pump, or a transport cutover.

The implementation does not change Core or another repository.

## Cross-repository work

The Core ticket `ticket_1788128130_441301` depends on this Hub ticket through `dependency_1788137138_804385`.

Hub must merge first with the exact Core candidate pin. Core must then merge `a781556258789dea4a50ffcb17351e7294c8ff26` unchanged.

If Core changes the candidate, both runs must stop. Hub must update the pin and repeat the proof.

The cold-cut ticket `ticket_1787894427_525056` owns production ingress wiring. Its checklist `checklist_1788139722_173987` records the production proof obligation.

## Plan deviations

The implementation did not make an unapproved deviation.

Human answer `question_1788142641_571879` replaced the stale red/green proof with green/green compatibility proof. The committed plan now records this change.

Advisor answer `question_1788144097_297699` approved the narrow cleanup fixture change. The committed plan now records this change and acceptance check `13c`.

## Review repair

Review `review_1788150881_521801` returned four findings.

- `finding_1788150881_471943`: A producer could enqueue ingress after `close` failed to acquire the occupied queue lock. The producer now clears the queue after a post-insert close check. Loss marking also clears a flag that races with close. A deterministic test covers both close and push orders.
- `finding_1788150881_203541`: Typed cleanup skipped the promised registry removal. The helper now waits for authoritative exit, calls `RemoveSession`, proves target absence, and preserves the exact sibling row.
- `finding_1788150881_173921`: The plan contained a personal absolute path. The plan now uses a path-neutral repository reference. The committed-artifact path scan is clean.
- `finding_1788150881_687191`: The report had an extra blank line at end of file. The blank line is removed. The raw `git diff --check main...HEAD` gate runs after the repair commit.

## Verification

The toolchain was `rustc 1.97.0 (2d8144b78 2026-07-07)`.

The following checks passed:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --locked -- -D warnings`
- `cargo tree -e normal -i botster-terminal-protocol --locked`
- `cargo build --locked -p botster-core-daemon --bin botster-session-worker`
- `cargo build --locked --bin botster-hub`
- `./test.sh --locked --test session_projection_owner_loop git_visible_hub_members_share_one_exact_core_revision -- --exact`
- Unix adapter Core conformance, by its exact test path.
- WebRTC adapter Core conformance, by its exact test path.
- The adapter ingress state and capacity tests.
- `runtime::tests::bind_terminal_adapter_mapping_is_total_over_published_variants`
- `subscription::attach_routes::tests::ingress_loss_hard_stops_exact_bound_route_and_preserves_sibling`
- `final_cleanup_accepts_already_exited_without_altering_sibling`
- `external_hub_shutdown_session_failure_keeps_daemon_and_sibling_usable`
- `webrtc_peer_post_handshake_data_channel_reaches_production_reject`
- `env -u CARGO_TARGET_DIR RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked`
- `git diff --check`

The repaired official locked suite passed with exit code 0. It included 504 library tests and 322 lifecycle test cases before later workspace suites and doc tests.

The first sandbox run failed because the sandbox denied socket and WebRTC operations. That result was an environment failure.

Two earlier full runs exposed timing failures. Each exact test passed alone. The final official run passed both tests in the full suite.

## Red ablations

Four red ablations failed as required:

1. Returning `Empty` after close failed the Unix conformance check.
2. Returning a frame before a pending loss failed the Unix conformance check.
3. Using capacity `N - 1` failed the capacity floor check.
4. Using one error class for both variants failed the exact error mapping check.

The review repair added a fifth red ablation. Removing the post-insert close check made the deterministic close-race test retain `racing-close` bytes and fail.

The implementation restored each source change after its ablation.

## Downstream proof

The exact test `unix_adapter_unbound_scoped_drain_delivers_terminal_output` passed twice at Core `3672c667` after a full pin roll and rebuild.

The same exact test passed at Core candidate `a781556258789dea4a50ffcb17351e7294c8ff26`.

This green/green pair proves Hub compatibility. Core targeted-input tests and Core red ablations prove the Core behavior change.

The Core branch tip still resolved to `a781556258789dea4a50ffcb17351e7294c8ff26`. The contract diff from `3672c667` was empty for the approved contract paths.

## Runtime teardown lenses

- Isolation: Each route and generation owns one `AdapterSlot`. A loss cannot change a sibling slot.
- Bounds: The queue uses Core's minimum frame constant. `try_read` does bounded non-blocking work.
- Late messages: A closed slot discards buffered and new ingress. A retired generation cannot feed a replacement generation.
- Ownership identity: Core owns the bound route. The Hub handle and Core adapter share one `Arc`.
- Fail-closed policy: A loss hard-stops only the exact route. A closed adapter always reports `Closed`.
- Production path: The error mapping is live. The ingress buffer is scaffold-only in Hub production.

Hub production does not call `pump_woken` or `drain`. The route test uses a test-only drain seam. It does not prove production reachability.

## Residual risk and unverified behavior

The ingress buffer has no production producer or wake pump in this ticket. The cold-cut ticket owns that work and its production proof.

GitHub CI was not verified before this report. Local repository gates passed.

The Core candidate is not yet on Core `main`. The merge order and exact candidate requirement remain active.

## Missing vault guidance

The implementation found six guidance gaps:

1. Hub adapter read precedence is `Closed`, `Lost`, `Frame`, and `Empty`.
2. Hub can pin an unmerged Core candidate when the required merge order keeps Hub green.
3. Hub Core pin rolls currently have fourteen literal sites and six lock sources.
4. Hub cannot reach `TerminalAdapter::try_read` in production before the wake pump lands.
5. A frozen Core candidate can move during one Plan step.
6. Final cleanup can receive typed `SessionCleanup { outcome: "already_exited" }` after authoritative exit.

The implementation did not write these notes to the vault. The approved plan and this report preserve the findings for a separate vault capture step.

## Assumptions

The implementation assumes that Core will merge the exact candidate unchanged.

The implementation assumes that the cold-cut ticket will keep its registered production proof obligation.
