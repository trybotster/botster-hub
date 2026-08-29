# Repair the WebRTC byte-exact baseline flake under full-suite load

Ticket: ticket_1787999248_674913 — "Hub baseline flake: webrtc_terminal_output_is_byte_exact fails under full-suite load".
Run: run_1787999630_574649. Pipeline: Botster Stack Delivery. Base ref: main at 38d140c.

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- target_id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- The target came from the run record, then from the spawn-target list. The process working directory did not select it.

## Repository playbook loaded

- [[botster-hub-playbook]]

## Other role and surface playbooks and atomic notes loaded

Role playbooks:
- [[planner-playbook]]
- [[botster-planner-playbook]]

Class overlay:
- [[botster runtime teardown lenses]] (loaded; see the class verdict below)

Atomic notes:
- [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]]
- [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]
- [[host exhaustion markers identify each failed test]]
- [[a suite-load oracle must not demand more than the host contract another test in the same file already codifies]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[test names do not prove their bodies can fail on the named claim]]
- [[exact Rust test ablations require a one test baseline]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]

## Context loaded

Repository code read at 38d140c:
- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs:826-909` — the failing test.
- `tests/hub_daemon_lifecycle/session_fixtures.rs:429-560, 579-660` — `PRODUCER_READY_MARKER`, `wait_for_producer_ready`, `wait_for_authoritative_session_exit`, `production_cleanup_after_authoritative_exit`, `live_output_decoded_bytes`.
- `tests/hub_daemon_lifecycle/package_fixtures.rs:1251-1265` — `write_python_wait_then_write_script`, which prints `producer-ready` before it waits for the release file.
- `tests/hub_daemon_lifecycle/harness.rs:199-330` — `format_harness_budget_expired`, `classify_budget_expiry`, `probe_fd_limit`, `probe_pty_allocation`.
- `tests/hub_daemon_lifecycle/common.rs:290-330, 1007-1050` — existing call sites that panic with a named harness marker on budget expiry.
- `tests/hub_daemon_lifecycle/sessions.rs:1510, 1548` — live callers of `wait_for_producer_ready`.
- `tests/hub_daemon_lifecycle/webrtc_proofs.rs:267-390` — the sibling byte-exactness proof `external_hub_webrtc_live_output_preserves_exact_bytes`, which uses a counted-turn loop.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs:553-600` — prior art for a blocking `botster_hub_client::request` call inside `block_on`.

## Mechanism

The test spawns a Python producer through a real Hub session, binds a WebRTC terminal subscription, writes the release file at once, and then polls for four expected bytes inside `Instant::now() + Duration::from_secs(8)` (line 852).

That single wall-clock window must cover Python interpreter start, Core PTY plumbing, adapter framing, and WebRTC delivery. Under full-suite concurrency the window can expire before the first frame arrives, so the assertion at line 895 reports `got []`. A focused rerun passes on the same source.

The window is therefore an ambient-load-sensitive wall-clock bound over a daemon child, which [[wall-clock ready-operation bounds through a daemon child are ambient-load-sensitive]] forbids as a pass-or-fail gate.

## Scope

Change one test body in `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs`:

1. Add an observed-state precondition. Call `wait_for_producer_ready(&endpoint, session_id)` after `spawn_and_bind_webrtc` and before the release-file write. This removes producer start latency from the delivery window and replaces a hidden race with an observed state.
2. Replace the fixed eight-second delivery deadline with a completion oracle that does not gate on elapsed time:
   - Success: the expected byte window appears in `concatenated`.
   - Completion: the session reaches lifecycle `exited` under `wait_for_authoritative_session_exit`-style production observation, and a counted number of further drain turns adds no new bytes. The producer has then written everything it will ever write, so the byte-exactness assertion decides the result.
   - Starvation: an outer backstop expires while `concatenated` is still empty. The test then panics with `format_harness_budget_expired`, which carries `test=webrtc_terminal_output_is_byte_exact`, the resource class, and the probe verdict.
3. Keep the byte-exactness assertion at line 895 unchanged in text and strength. When frames arrived but never matched, the test falls through to that assertion and fails on the byte claim, not on a harness marker.
4. Keep the test name `webrtc_terminal_output_is_byte_exact` unchanged.
5. Add or reuse a small file-local helper only if the loop body needs it. Do not add a shared harness abstraction for one call site.

The backstop remains a wall-clock value, but it no longer decides the product claim. It only separates "the host never delivered one byte" from "the adapter delivered wrong bytes", which is exactly the marker lane that [[host exhaustion markers identify each failed test]] requires.

## Non-scope

- Any production change to WebRTC framing, delivery, sealing, chunking, or the adapter.
- Any change to Core, `botster-hub-client`, or `hub-test-support`.
- The Hub decomposition sequence, including decomposition 4a (`ticket_1787894421_128594`).
- The 30-plus sibling `Duration::from_secs(8)` wait sites across `tests/hub_daemon_lifecycle/`. They are recorded below as a follow-up candidate, not as work in this run.
- The sibling proof `external_hub_webrtc_live_output_preserves_exact_bytes` in `webrtc_proofs.rs`. Its counted-turn loop has a different failure profile and no measured failure in this ticket.
- The suite wrapper and its host-exhaustion classification logic.

## Repository ownership boundaries and cross-repository dependencies

- Hub owns this test file, the lifecycle harness, and the host-exhaustion marker format. The change stays inside Hub test code.
- Core owns terminal bytes, PTY plumbing, and session lifecycle. This plan reads Core behavior through Hub daemon requests and changes nothing in Core.
- `botster-hub-client` owns the DTOs the test decodes. The plan adds no DTO field and changes no protocol value.
- Cross-repository dependencies: none. No dependency ticket is required.
- Blocked work: ticket_1787894421_128594 (Hub decomposition 4a) currently uses a differential base-versus-HEAD suite protocol because of this failure. This run does not change that ticket.

## Runtime-teardown class verdict

`teardown_class_applies`: **no**, with a stated reason. The ticket touches a WebRTC-carried test, but it changes no peer lifecycle, no SessionIo or ClientWorker teardown, no multi-peer ownership, and no forget or close path. The change is confined to the wait oracle of one test body. The note itself says the lenses do not apply to work outside those surfaces, and it asks Plan to state the verdict when the ticket sits near the class boundary.

The existing teardown-shaped steps in the test stay unchanged and keep their current proofs:
- `peer.peer.close().await` still closes the offer peer.
- `production_cleanup_after_authoritative_exit` still proves `SessionCleanup { already_exited }` and `SessionRemoved`.
- `hub.shutdown()` still proves isolated Hub shutdown.

If Plan Review judges that the class applies, the correct action is to force the class on this ticket and return the plan. Do not open a second pipeline.

## Assumptions and unknowns

Assumptions:
1. The Python producer prints `producer-ready` to the same PTY before it waits for the release file. `write_python_wait_then_write_script` confirms this at `package_fixtures.rs:1256`.
2. `wait_for_producer_ready` reaches the session through the Unix endpoint while a WebRTC subscription is bound. `sessions.rs:1510` and `1548` use the helper against live sessions.
3. A blocking `botster_hub_client::request` call inside the multi-thread `block_on` block is safe. `webrtc_terminal_adapter.rs:582` already does this inside an async block.
4. The producer exits shortly after it writes the four bytes, so lifecycle `exited` is a reachable completion signal. `production_cleanup_after_authoritative_exit` at line 903 already depends on that exit.

Unknowns for Implement to resolve with measurement, not assertion:
1. Whether the `producer-ready` marker text can appear in `concatenated` and shift the byte window search. The search uses `windows(4)`, so extra leading bytes are harmless, but Implement must confirm no assertion depends on `concatenated` starting at the expected bytes.
2. Whether observed exit can precede the last terminal frame on the WebRTC route. Implement must size the counted quiet-turn drain after exit from observed behavior, not from a guess.
3. Whether `AUTHORITATIVE_SESSION_EXIT_WAIT` (10 s) inside `wait_for_producer_ready` is itself load-fragile at this call site. If Implement observes that budget expiring under suite load, it must report the observation instead of silently raising a shared constant.

## Affected surfaces and files

- `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` — the body of `webrtc_terminal_output_is_byte_exact` (lines 826 to 909), plus the imports that the new helper calls require.
- `docs/plans/repair-webrtc-byte-exact-baseline-flake-under-suite-load.md` — this plan.
- No production source file changes.

## Risks

1. **Masking a real product failure.** A marker lane can hide a genuine byte failure. Mitigation: emit the marker only when zero bytes arrived. Any non-empty `concatenated` falls through to the unchanged byte-exactness assertion.
2. **A new hang.** An oracle that waits for exit can hang when the producer never exits. Mitigation: keep an outer backstop that always terminates the loop and always produces either the marker or the byte assertion.
3. **Weakening the proof.** Waiting for producer-ready could look like relaxing the claim. Mitigation: the claim is byte identity, not delivery latency. The assertion text and the expected byte sequence stay identical.
4. **Shared-constant drift.** Raising `AUTHORITATIVE_SESSION_EXIT_WAIT` or any production budget to pass the test is forbidden by [[wall-clock MAX_OWNER_TURN_MS assertions flake under default-concurrency lib load]]. Mitigation: Implement changes no production constant and no shared harness constant.
5. **Ablation blindness.** `cargo test --exact` with a bare leaf name filters out every test and reports `ok`. Mitigation: run the ablation with the full module path and prove a one-test baseline first, per [[exact Rust test ablations require a one test baseline]].
6. **Residual load failure.** The repaired oracle can still fail on a genuinely exhausted host. That outcome is acceptable only when the failure carries the named marker.

## Acceptance checks and tests

Toolchain and worktree rules for every command:
- `RUSTUP_TOOLCHAIN=1.97.0`; record `rustc --version` from the same shell.
- `CARGO_TARGET_DIR` unset for the official gate; use a colon-free worktree path.
- Prebuild before the locked wrapper: `cargo build --locked -p botster-core-daemon --bin botster-session-worker`, then `cargo build --locked --bin botster-hub`.

Checks:
1. Focused exact rerun, full module path, one-test baseline first:
   `cargo test --locked --test hub_daemon_lifecycle_test -- --exact hub_daemon_lifecycle::subscription_ownership_baseline::webrtc_terminal_output_is_byte_exact --nocapture`
   The baseline output must report exactly one test run.
2. Full official suite on a normally loaded host: `./test.sh --locked`. The named test must pass, or its failure must print a `harness_budget_expired` marker that contains `test=webrtc_terminal_output_is_byte_exact`.
3. **Red-on-revert, product arm.** Break byte exactness deliberately, for example by changing the producer to write one different byte, and prove the test fails on the byte-exactness assertion at line 895 with a non-empty `got [...]`. The failure must not be a harness marker. Restore the source afterward.
4. **Red-on-revert, precondition arm.** Remove the producer-ready wait and confirm the test still passes in isolation. This shows the wait is a precondition, not the proof.
5. **Marker arm.** Force the starvation lane, for example by never releasing the producer, and confirm the panic text contains `harness_budget_expired`, `test=webrtc_terminal_output_is_byte_exact`, a `resource=` class, and a `probe=` verdict. Restore the source afterward.
6. Strict Rust gates: `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --locked -- -D warnings`. Rerun Clippy after each repair.
7. Repeat check 2 or the focused test under deliberate ambient load, and record the observed elapsed time as an observation only, never as a pass-or-fail gate.

Downstream proof: none required. The change is Hub-internal test code and crosses no repository seam.

Production-path note: this ticket is intentionally test-only. The production WebRTC delivery path is unchanged, and the plan claims no production behavior change.

## Vault gaps worth capturing

1. A byte-delivery proof through a real producer needs a producer-ready precondition plus a completion oracle. A single wall-clock window over interpreter start plus transport is not a valid gate. The existing wall-clock notes cover ordering and owner-turn budgets, not live byte delivery.
2. A starvation marker lane must stay mutually exclusive with the product assertion. Emit the marker only when nothing arrived, so a wrong-byte failure can never read as host exhaustion.
3. `tests/hub_daemon_lifecycle/` holds more than thirty `Duration::from_secs(8)` wait sites. That repeated constant is a suite-wide flake surface and deserves one durable note, plus a separate ticket if the owner wants the sweep.
