# Implementation report: upgrade WebRTC for post-handshake DataChannel creation

| Field | Value |
| --- | --- |
| Ticket | `ticket_1787654915_646236` |
| Run | `run_1787654940_337274` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Plan | `docs/plans/upgrade-webrtc-for-post-handshake-datachannel-creation.md` (rev5, commit `d9ff12c`) |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| Base | `f66d459` |

## Target repository and target_id

- Target repository: `botster-hub` (`trybotster/botster-hub`)
- `target_id`: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Resolved from `list_spawn_targets`, not from the ambient working directory.
- Worktree: `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787654915_646236`
- Branch: `project-pipelines/ticket_1787654915_646236`

## Repository playbook and other playbooks/notes applied

Role and overlay:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]

Architecture and runtime:

- [[botster-architecture]]
- [[cli-patterns]]
- [[botster runtime teardown lenses]]

Targeted atomic notes:

- [[the pinned Rust WebRTC peer cannot open a DataChannel created after the SCTP handshake]]
- [[botster subscriptions use dedicated ordered DataChannels]]
- [[rejected channel isolation needs a surviving channel positive control]]
- [[WebRTC DataChannel local close uses the peer close bound before cleanup]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[implement gate must verify committed work and pr link before review]]
- [[test script required for rust tests not cargo test]]

**Not loaded:** [[project-pipelines-playbook]] — no Project Pipelines package or plugin path is in scope.

## Files changed

| Path | Change |
| --- | --- |
| `Cargo.toml` | `webrtc = "0.21.0-beta.2"` |
| `Cargo.lock` | `webrtc`, `rtc`, and every `rtc-*` member resolve to `0.21.0-beta.2`; adds `rtc-crypto` |
| `src/local_webrtc.rs` | `webrtc_runtime()` helper; `timeout` call sites take `&dyn Runtime`; fail-safe `_ => "unknown"` on `RTCPeerConnectionState`; A4/A6/A9 library tests |
| `src/daemon_transport.rs` | `#[cfg(test)]` `has_webrtc_admission_row` and `has_host_compatibility_row` (`contains_key`). No production change |
| `src/local_webrtc_smoke.rs` | tokio `block_on` replacement; runtime-bound `sleep`/`timeout` |
| `tests/hub_daemon_lifecycle/webrtc_fixtures.rs` | tokio `block_on`; runtime-bound `timeout`; `create_extra_data_channel` requires `OnOpen` |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | A4-live `webrtc_peer_post_handshake_data_channel_reaches_production_reject` |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | A7: extra channel must open before isolation is measured |
| `tests/hub_daemon_lifecycle_test.rs` | compile shims for include! files: local `block_on`, `timeout`, `sleep` |
| `tests/hub_daemon_lifecycle/{cli,common,session_fixtures,process,package_fixtures,operator_console_fixtures}.rs` | drop removed `webrtc::runtime::{block_on, sleep, timeout}` imports |
| `docs/reports/upgrade-webrtc-for-post-handshake-datachannel-creation-implement.md` | this report |

## Ownership boundaries preserved

- Work stays in the Hub trusted host kernel and local WebRTC transport.
- No subscription reservation, route labels, generations, or channel routing.
- Single-claim rejection in `LocalWebrtcHandler::on_data_channel` is unchanged.
- `tokio::time::timeout` close bounds are unchanged.
- No Core, hub-client, ui-contract, or hub-test-support version change.
- No Project Pipelines package/plugin edits.

## Cross-repo dependencies or separately routed work

- None implemented here.
- Downstream consumers remain `ticket_1787600674_500120` and `ticket_1787600682_233928`.
- No downstream repository proof is required.

## Runtime-teardown lenses

| Lens | Status |
| --- | --- |
| Isolation | Unchanged. One failed peer still tears down its own state. Close-bound, sibling-preserve, and ultimate-close tests stay green. |
| Bounds | Production close still uses `tokio::time::timeout(LOCAL_WEBRTC_PEER_CLOSE_BOUND, ...)`. Those sites were not converted. |
| Late-message matrix | No owner tag, rejection rule, or sweep changed. A9 proves Hello admission rows are removed on `PeerClosed`. |
| Production path | A4-live drives a live Hub: post-handshake `create_extra_data_channel` → production `on_data_channel` → `lost_claim`/`close_ok`/`label=botster-extra` + close marker. |
| Ownership identity | Unchanged. `grant_id` remains the owner. A9 uses `contains_key`, not `webrtc_is_admitted`. |
| Sibling fail-closed | Unchanged. Ultimate-close and sibling-preserve tests stay green. |

## Exact versions

| Crate | Resolved version |
| --- | --- |
| `webrtc` | `0.21.0-beta.2` |
| `rtc` | `0.21.0-beta.2` |
| `rtc-crypto` (new) | `0.21.0-beta.2` |
| every other `rtc-*` member | `0.21.0-beta.2` |

Assumption A1 holds: no newer `0.21.0-rc` was published. Cargo resolved exactly `0.21.0-beta.2`.

`rtc-crypto` is additive. `cargo check --workspace --all-targets --locked` passed, so the `rustls` / `ring` graph used by `ureq` did not conflict (U3).

## Provenance

| Identity | Value |
| --- | --- |
| Hub worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787654915_646236` |
| Hub binary realpath | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787654915_646236/target/debug/botster-hub` |
| Session worker realpath | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787654915_646236/target/debug/botster-session-worker` |
| Locked Core revision | `7eafa470a18025895995bbedc20d34b58106a03b` |

## Deviations from plan

1. **Compile-required files outside the planned table.** `cargo check --workspace --all-targets` failed on include! parents and fixture modules that imported the removed free functions `block_on`, `sleep`, and `timeout`. The plan said to record that rather than widen silently. Shims live in `tests/hub_daemon_lifecycle_test.rs`. Fixture modules only drop the missing imports.
2. **`webrtc_runtime()` helper.** `send_response_frames` has no `Runtime` in scope. The helper returns `default_runtime()` so timeout sites can pass `&dyn Runtime` without threading a runtime through the send loop.
3. **A4 creates a pre-handshake `botster-client` channel.** `create_offer` without a channel produced `ErrSessionDescriptionMissingIceUfrag`. The late channel is still created only after both peers reach `Connected` and the setup channel is open.
4. **`create_extra_data_channel` requires `OnOpen`.** The helper used to discard the open wait. A7 and A4-live need that open to be load-bearing.
5. **A5 ran on a detached `d9ff12c` worktree**, not by pinning this branch to `0.20`. This branch's 3-arg `timeout` API cannot compile against `webrtc 0.20.0-rc.1`. The A4 body was copied onto `d9ff12c` with 0.20 2-arg `timeout`. That run failed: remote `on_data_channel` stayed on `botster-client` and never delivered `botster-late`. A2 holds. Escalation was not required.
6. **A4-live `0.20` result is not runnable on this branch** for the same API reason. Recorded as diagnostic, not a gate, per A5/R6.
7. **A9 uses `WebrtcTerminalAdmission::Rejected`.** A `Rejected` row still inserts both maps. `webrtc_is_admitted` stays false, so the positive control cannot pass through the false-negative helper (R10).

## Tests and downstream proof

Prebuild, in order:

```bash
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo build --locked --bin botster-hub
```

Both completed. Then:

| Check | Command | Result |
| --- | --- | --- |
| A1 | `cargo check --workspace --all-targets --locked` | pass |
| A6 | `BOTSTER_ENV=test cargo test --locked --lib local_webrtc::tests::runtime_spawn_detach_on_drop_runs_to_completion -- --exact` | running 1 test, 1 passed |
| A9 | `BOTSTER_ENV=test cargo test --locked --lib local_webrtc::tests::peer_closed_removes_webrtc_admission_and_host_compatibility -- --exact` | running 1 test, 1 passed |
| A4 | `BOTSTER_ENV=test cargo test --locked --lib local_webrtc::tests::post_handshake_data_channel_opens_and_delivers_bytes -- --exact` | running 1 test, 1 passed (0.22s) |
| A3 close hang | lib exact `local_webrtc_close_hang_fail_closed_returns_handler_within_deadline` | running 1 test, 1 passed |
| A3 ultimate close | lib exact `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` | running 1 test, 1 passed |
| A3 sibling preserve | lib exact `local_webrtc_single_peer_failed_cleanup_preserves_sibling_peer_and_runtime` | running 1 test, 1 passed |
| A3 sibling live | `./test.sh --locked --test hub_daemon_lifecycle_test peer_close_leaves_sibling_peers_working -- --exact` | running 1 test, 1 passed |
| A7 | `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_terminal_adapter_second_data_channel_does_not_receive_terminal_frames -- --exact` | running 1 test, 1 passed |
| A4-live | `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_peer_post_handshake_data_channel_reaches_production_reject -- --exact` | running 1 test, 1 passed |
| A3 reject prefix | `./test.sh --locked --test hub_daemon_lifecycle_test webrtc_peer_rejects_a_second_data_channel` | running 2 tests, 2 passed |
| A5 | A4 body on `/tmp/hub-a5-020` at `d9ff12c` / `webrtc 0.20.0-rc.1` | **red**: `left: "botster-client" right: "botster-late"` |
| A2 | `./test.sh --locked` after prebuild | exit 0; every crate 0 failed. `hub_daemon_lifecycle_test`: 318 passed, 2 ignored |

U4: `smoke_local_webrtc_round_trip` is called from `src/main.rs` smoke CLI on a plain thread, not from inside an existing tokio runtime. Nested-runtime hazard is preserved, not new.

R8: no `Receiver` clone sites were found. Hub stays single-consumer.

No downstream repository proof.

## Unverified behavior or residual risk

- A4-live on `webrtc 0.20` was not executed on this branch. R6 already treats that result as diagnostic.
- Live browser-created *admitted* subscription channels remain the deliverable of `ticket_1787600674_500120`.
- `0.21.0-beta.2` is a beta. `Cargo.lock` pins the exact graph.
- Isolated focused tests do not replace the locked suite. The locked suite is complete and green.

## Missing vault guidance discovered

Plan gaps G1–G3 remain valid. The plan says capture after Verify, not during Implement. No new conflicting convention was found. No capture was written in this step.

## Suite section

`./test.sh --locked` after the two prebuild commands: **exit 0** in 393.6s.

`hub_daemon_lifecycle_test`: running 320 tests, **318 passed, 0 failed, 2 ignored**. The two new tests are in that count. The unrelated Unix lifecycle failure seen on base during Plan Review did not reproduce.

Lib suite: 493 passed, 0 failed. No crate reported a failure.
