# Implement report: Core paste frames in Hub

## Target

| Field | Value |
| --- | --- |
| Repository | `trybotster/botster-hub` |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1788313897_932611` |
| Run | `run_1788326546_496759` |
| Candidate commit | `0417317412804ee172c6abe29cf03b80e195554a` |
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

The Rails conventions do not apply because this repository contains Rust code.
The runtime teardown class does not apply because this change does not create or change peer ownership.

## Files changed

- `Cargo.toml`, `Cargo.lock`, and the two in-repository crate manifests pin one Core revision.
- `crates/botster-hub-test-support/build.rs`, `src/conformance_data.rs`, and `src/lib.rs` update exact Core provenance.
- Five lifecycle proof files and `tests/session_projection_owner_loop.rs` update exact Core provenance.
- `src/transport/webrtc/delivery.rs` adds one bounded opaque inbound envelope assembly.
- `src/transport/webrtc/subscription_channel.rs` connects the assembly to the production terminal channel.
- `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` sends version 2 terminal delivery chunks.
- `tests/hub_daemon_lifecycle/common.rs` adds the test-only Core paste frame encoder.
- `tests/hub_daemon_lifecycle/paste_transaction.rs` adds six lifecycle proofs and two source guards.
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

The official gate then proved that the current Botster Web sender still uses the old raw envelope path.
Human answer `question_1788332467_932381` rejected a dual Hub reader.
The answer requires one cold-cut version 2 chunk path for all WebRTC terminal input.

The pipeline now has blocking dependency `dependency_1788332665_513105` on Web ticket `ticket_1788332653_879489`.
That ticket targets `trybotster/botster-web` through `tgt_40abcf71ccf049f4ac0c99953a799869`.
The Web ticket must not depend on this Hub ticket because that dependency would create a cycle.

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

Fresh Git checks show that Hub `origin/main` remains `db2c43c51513c02dd32ecd7ba85a9112f769c3e8`.
Core `origin/main` remains `48a437032791e678010254708259568ce4ad02bf`.
The Core branch containment check includes `origin/main`.
The active source and lock inventory contains no old Core revision.
`Cargo.lock` contains six exact new Core sources.

The official gate used Rust 1.97.0 and no `CARGO_TARGET_DIR` override.
Both required binary builds, formatting, and Clippy with warnings denied passed.
The library suite passed 543 tests.
The lifecycle suite passed 338 tests, failed 2 tests, and ignored 2 tests.

The two failures are:

- `cli_daily_commands_share_canonical_default_data_directory`
- `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc`

Both failures report `local WebRTC terminal marker not observed`.
The current Web sender sends raw encrypted terminal envelopes.
The Hub candidate accepts only version 2 delivery chunks.

The official suite must run again after Web ticket `ticket_1788332653_879489` merges.
No downstream proof is complete until that gate passes.

## Provenance and residual risk

The tested Hub commit is `0417317412804ee172c6abe29cf03b80e195554a`.
The locked Core commit is `48a437032791e678010254708259568ce4ad02bf`.
The test used the worktree `target/debug/botster-hub` and `target/debug/botster-session-worker` binaries.
The tracked worktree was clean before the official gate.

The only unverified behavior is the merged Web sender with the Hub candidate.
The blocking Web ticket owns that sender change.
The Hub official gate must prove the combined cold-cut path before Review.

## Missing vault guidance

The Core pin inventory note is stale.
The current Hub tree has 18 active source literals and six lock sources.
A zero-old-pin invariant is safer than a fixed source count.

No vault note records this WebRTC size boundary.
A maximum Core frame fits Core but its encrypted JSON envelope exceeds `LOCAL_WEBRTC_MAX_FRAME_BYTES`.
The durable rule must state that Hub reassembles opaque ciphertext without owning Core transaction meaning.
