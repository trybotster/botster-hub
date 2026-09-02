# Implement report: Core paste frames in Hub

## Target

| Field | Value |
| --- | --- |
| Repository | `trybotster/botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1788313897_932611` |
| Run | `run_1788326546_496759` |
| Candidate commit | `648e444d761e5158222a467efa5b872fc38f552f` |
| Base | `db2c43c51513c02dd32ecd7ba85a9112f769c3e8` |
| Core pin | `48a437032791e678010254708259568ce4ad02bf` |
| Merge policy | Direct merge. No pull request is required. |

The pipeline context and the spawn target map both route the target to `trybotster/botster-hub`.
The approved plan uses the same route.

## Guidance

The implementation applied these playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[project-pipelines-playbook]]

The implementation applied the targeted notes that the approved plan lists.
The main constraints came from these notes:

- [[core owns bounded atomic terminal input transactions across clients]]
- [[core owns duplex terminal transport while Hub stays content blind]]
- [[concrete terminal transports stay in hub until a second host needs them]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[Hub Core pin rolls update eleven literal sites and six lock sources]]
- [[Git-consumed Hub members pin Core protocol by exact revision]]
- [[Hub test support copies Core protocol fixtures from the pinned crate source]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[test script required for rust tests not cargo test]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[fixed source guard lists need one ablation per added file]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[region bounded source guards need a required symbol anchor]]
- [[release file gated producers flush readiness before release]]
- [[webrtc starvation markers must drop pre release producer ready bytes]]
- [[live byte delivery proofs need producer readiness and a completion oracle]]
- [[live acceptance tests must not depend on a loop tick window]]
- [[suite wide acceptance criteria make every observed test failure in scope]]

The Rails conventions do not apply because this repository contains Rust code.
The runtime teardown class does not apply because this change does not create or change peer ownership.

## Files changed

- `Cargo.toml`, `Cargo.lock`, and the two in-repository crate manifests pin one Core revision.
- `crates/botster-hub-test-support/build.rs`, `src/conformance_data.rs`, and `src/lib.rs` update exact Core provenance.
- Five lifecycle proof files and `tests/session_projection_owner_loop.rs` update exact Core provenance.
- `src/transport/webrtc/delivery.rs` adds one bounded opaque inbound envelope assembly.
- `src/transport/webrtc/subscription_channel.rs` connects the assembly to the production terminal channel.
- `src/transport/shared/ingress.rs` reports test-only stored and Lost admission outcomes.
- `src/transport/shared/adapter_slot.rs`, `src/transport/unix/adapter.rs`, and
  `src/transport/unix/connection.rs` publish route-specific outcomes to an append-only test journal.
- `src/local_webrtc_smoke.rs` sends every Hub smoke terminal input through version 2 chunks.
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` sends version 2 terminal delivery chunks.
- `tests/hub_daemon_lifecycle/common.rs` adds the test-only Core paste frame encoder.
- `tests/hub_daemon_lifecycle/paste_transaction.rs` adds six lifecycle proofs and two source guards.
- Three WebRTC proof files separate producer readiness, route binding, and product-byte release.
- The WebRTC live-byte proofs share one byte-or-authoritative-exit completion path.
- The second-channel and peer-loss proofs hold their producers until the reserved route is bound.
- The Unix detach and connection-death proof waits for route-specific terminal markers before cleanup.
- `src/transport/webrtc/adapter.rs` adds persistent test-only pressure for one named route.
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` replaces two unbounded output producers with deterministic pressure proofs.
- `tests/hub_daemon_lifecycle/harness.rs` reconciles a missing worker ancestor with the existing registry reread budget.
- `tests/hub_daemon_lifecycle/harness_isolation.rs` proves that a concurrent registry exit does not taint the harness.
- `tests/hub_daemon_lifecycle_test.rs` includes the new lifecycle proof file.
- `README.md` and `docs/client-protocol.md` document the ownership and framing rules.
- The approved plan records the human-approved WebRTC scope change.

## Ownership

Hub reassembles only one opaque encrypted WebRTC envelope per subscription route.
Hub enforces the existing WebRTC frame and delivery bounds.
Hub decrypts the complete envelope and calls the existing shared ingress boundary once.

Core owns paste frame kinds, paste assembly, ordering, timeout, write admission, and `input_result`.
Hub does not inspect the Core frame body.
Hub does not keep paste operation state.

The implementation changes no file in another repository.

## Plan deviations

The approved plan first assumed that one encrypted maximum Core frame fit one WebRTC application frame.
The live WebRTC proof disproved that assumption.

Human answer `question_1788330311_325612` approved bounded Hub reassembly of opaque ciphertext.
The committed plan records this material change.

The first official gate proved that the current Botster Web sender still used the old raw envelope path.
Human answer `question_1788332467_932381` rejected a dual Hub reader.
The answer requires one cold-cut version 2 chunk path for all WebRTC terminal input.

The project orchestrator fulfilled the sender dependency through existing Web ticket `ticket_1787600676_914408`.
Dependency `dependency_1788360616_179633` is closed.
Web `origin/main` contains the cold-cut sender at `6dc32b32d9842070742272577483275aceb71ea3`.

The next gate found the same raw path in the Hub-owned Rust smoke client.
The final change moved `src/local_webrtc_smoke.rs` to version 2 terminal chunks.
This change stays inside Hub transport ownership and removes the final raw terminal sender.

Review `review_1788374640_664771` found no observation of actual buffer admission.
The Unix route now writes one append-only JSON row after each `IngressBuffer` store attempt.
Each row includes the session, subscription, and `stored` or `lost` outcome.
The 19-frame test waits for exactly 19 stored outcomes.
The 65-frame control waits for exactly 64 stored outcomes and one Lost outcome.

The same review found three WebRTC producer tests that failed under full-suite load.
The release-file producers now prove Core readiness before product-byte release.
The byte-exact WebRTC test also starts the readiness marker only after route binding.
The public protocol guide now requires version 2 chunks for every encrypted envelope.
The plan uses a path-neutral worktree description.

Review `review_1788377731_757118` found three first-failure roots in required full runs.
Two WebRTC tests depended on high-volume `yes` output to cause DataChannel pressure.
The WebRTC mux now applies persistent test-only pressure to one named route.
DataChannel low-water events cannot clear this test pressure.
The two tests now use one output frame and preserve their Core close assertions.

The third root was `cli_short_lived_session_shutdown_returns_structured_cleanup`.
The harness saw a live command but could not resolve its worker ancestor.
The new test records the session exit concurrently in the registry.
The harness now uses its existing eight-attempt registry reread before it reports unresolved ownership.
The harness still reports an error for a live, nonterminal command with no verified worker.

Review `review_1788379959_143996` found that a helper thread armed WebRTC test pressure after route registration.
Core could drain the one-frame producer before the helper thread armed pressure.
The mux now arms route pressure synchronously before it inserts and wakes the route.
The write-budget test sets a 250 ms helper delay as a scheduling control.
The control stays green because the fixed path does not use the helper thread.

The same review found that the committed candidate failed Rustfmt in the new low-water unit test.
Rustfmt applied the required line wrap.
The exact format gate now passes against the committed candidate.

Review `review_1788381602_871891` found three tests that failed under aggregate load.
The shutdown-after-exit proof had not drained the producer-ready bytes before release.
It now uses the shared readiness drain and byte-or-authoritative-exit completion path.
The second-channel proof now holds its producer until the reserved route is bound.
The peer-loss proof now receives a route-specific terminal marker before it closes the peer.

The first full run after those repairs passed all three Review failures.
That run exposed `connection_death_and_detach_do_not_emit_terminal_subscription_closed`.
The test had not proved that either Unix route was live before Detach or connection death.
Both Unix producers now stay held until their route receives a distinct terminal marker.

## Verification

The following focused tests pass:

- `unix_paste_transaction_delivers_one_result_and_byte_exact_pty_content`
- `webrtc_paste_transaction_delivers_one_result_and_byte_exact_pty_content`
- `paused_ingress_holds_nineteen_paste_frames_without_lost`
- `paused_ingress_sixty_fifth_frame_latches_lost_and_closes_only_that_route`
- `hub_transport_source_stays_paste_blind`
- `paste_blind_guard_fails_on_seeded_eof_token`
- `terminal_input_assembly_reassembles_one_large_opaque_envelope`
- `terminal_input_assembly_fails_closed_on_malformed_or_unbounded_chunks`

The live tests send a 1,048,576-byte paste as 19 Core frames.
Both transport tests receive one admitted `input_result` with `bytes_written=1048576`.
Both PTY sinks receive the exact payload bytes.

The old Core pin red control failed with `core_adapter_closed` and no `input_result`.
The old single-message WebRTC red control failed with `rejection=operation_incomplete`.
The permanent source guard red control detects a forbidden token after the final test module.

The concurrent registry exit test failed with the harness fix reverted.
It reported `unresolved worktree session-worker ancestor for command ... session concurrent-exit`.
The WebRTC write-budget test failed with the route pressure fix reverted.
It reported `keep-reading observer must see core_adapter_closed` after 22.93 seconds.
The direct low-water unit test proves that a low-water event cannot clear test pressure.
The prior helper-thread implementation also failed the 250 ms scheduling control after 22.61 seconds.
It reported `keep-reading observer must see core_adapter_closed`.

Review commit `af06529bf0f1f496c5a0013ecbfdb4d26590bea3` failed these aggregate-load tests:

- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`
- `webrtc_peer_rejects_a_second_data_channel`
- `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`

The official run from `dcbfcc82d60c896f8fa1c756941b3e01efd1091d` passed those three tests.
The same run failed `connection_death_and_detach_do_not_emit_terminal_subscription_closed`.
It received `TerminalSubscriptionClosed` with reason `core_adapter_closed` during explicit Detach.
This result is the red-on-revert evidence for the Unix route-readiness repair.

The five WebRTC focused tests pass:

- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup`
- `webrtc_peer_rejects_a_second_data_channel`
- `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`
- `external_hub_webrtc_live_output_preserves_exact_bytes`
- `webrtc_terminal_output_is_byte_exact`

The exact Unix test `connection_death_and_detach_do_not_emit_terminal_subscription_closed` also passes.

Fresh Git checks show that Hub `origin/main` remains `db2c43c51513c02dd32ecd7ba85a9112f769c3e8`.
Core `origin/main` remains `48a437032791e678010254708259568ce4ad02bf`.
The Core branch containment check includes `origin/main`.
The active source and lock inventory contains no old Core revision.
`Cargo.lock` contains six exact new Core sources.

The final official gate used Rust 1.97.0 and no `CARGO_TARGET_DIR` override.
Both required binary builds, formatting, and Clippy with warnings denied passed.
`env -u CARGO_TARGET_DIR RUSTUP_TOOLCHAIN=1.97.0 ./test.sh --locked` passed at default concurrency.
The lifecycle suite passed 341 tests and ignored 2 documented tests.
The library suite passed 544 tests.
All other workspace tests and doctests passed.

`RUSTUP_TOOLCHAIN=1.97.0 cargo fmt --all -- --check` passed.
`RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --locked --all-targets --all-features -- -D warnings` passed.
The exact 250 ms scheduling control passed in 2.83 seconds.

The first returned full run failed these WebRTC tests:

- `webrtc_terminal_adapter_failed_remove_session_does_not_suppress_later_core_close`
- `webrtc_terminal_adapter_write_budget_emits_core_adapter_closed_while_peer_stays_readable`

Independent Review then found one harness taint root in `cli_short_lived_session_shutdown_returns_structured_cleanup`.
The first full run after the initial pressure change passed 340 lifecycle tests and failed the write-budget test.
That run proved that low-water events could clear the initial injected pressure.
The final unchanged full run passed all three prior roots and the complete official gate.

The two prior smoke failures pass inside the final full suite.
This result proves the Hub smoke client and merged Web sender use the same cold-cut chunk contract.

## Provenance and residual risk

The tested Hub commit is `648e444d761e5158222a467efa5b872fc38f552f`.
The locked Core commit is `48a437032791e678010254708259568ce4ad02bf`.
The merged Web commit is `6dc32b32d9842070742272577483275aceb71ea3`.
The test used the worktree `target/debug/botster-hub` and `target/debug/botster-session-worker` binaries.
The tracked code changes matched the tested candidate before the official gate.

No known ticket behavior remains unverified.
Review must confirm the bounded receiver rules and the content-blind ownership boundary.

## Missing vault guidance

The Core pin inventory note is stale.
The current Hub tree has 18 active source literals and six lock sources.
A zero-old-pin invariant is safer than a fixed source count.

No vault note records this WebRTC size boundary.
A maximum Core frame fits Core but its encrypted JSON envelope exceeds `LOCAL_WEBRTC_MAX_FRAME_BYTES`.
The durable rule must state that Hub reassembles opaque ciphertext without owning Core transaction meaning.
