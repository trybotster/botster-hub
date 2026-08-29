# Implement follow-up: Request owner and strict Clippy

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788039898_642630`) |
| Approved plan | `docs/plans/hub-decomposition-4b-move-daemon-ownership.md` revision 5, plus Review-required Request owner |
| Merge policy | direct into `main`; do not create a PR |
| Open findings closed | `finding_1788039873_936660` (high), `finding_1788039873_333814` (high), `finding_1788039874_502015` (medium) |
| Runtime-teardown class | applies; every lens remains a survive-the-move invariant |

Routing is unchanged: ticket and run `target_id` still map to `trybotster/botster-hub`. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[strict clippy can hide later crate diagnostics behind the first compile failure]]
- [[hub moves must extend source scanning guard file lists]]
- [[fixed source guard lists need one ablation per added file]]
- [[code moves need paired absence and presence source guards]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[implementation deviations must resync committed plan acceptance checks]]

[[project-pipelines-playbook]] was not loaded.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/control/request.rs` | Dedicated `ControlMessage::Request` owner: live-peer gate, family dispatch, post-processing |
| `src/daemon/control.rs` | Request and PeerClosed arms are one-call delegates |
| `src/daemon/control/webrtc.rs` | `handle_peer_closed` takes existing `ControlMessage` instead of eight arguments |
| `src/daemon/control/spawn_targets.rs` | create/update take existing `SpawnTargetCreate` / `SpawnTargetUpdate` |
| `src/lib.rs` | Add `request.rs` to the production-source guard list |
| `tests/daemon_control_ownership.rs` | Single-delegation Request-arm check plus red control |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | Guard list includes `request.rs` |
| `tests/session_projection_owner_loop.rs` | Guard list includes `request.rs` |
| `docs/plans/hub-decomposition-4b-move-daemon-ownership.md` | Resync Request owner and gate-4a/4b file names |

## Ownership boundaries preserved

- Hub still owns control-plane topology.
- `handle_control_message` Request arm delegates once to `request::handle`.
- The universal WebRTC live-peer gate still runs before family delegation, now in `request.rs`.
- Post-processing order is unchanged.
- Public DTOs, protocol, Core pin, and grant non-removal are unchanged.
- Clippy argument-count repairs use existing request/message types, not new wrappers.

## Cross-repo routing

None.

## Deviations from plan

Review required a dedicated Request owner. The original plan left Request post-processing in `control.rs`. That arm is now `src/daemon/control/request.rs`, and acceptance checks 4a, 4b, and 5 plus the affected-files list were resynced in the committed plan.

## Tests and downstream proof

Always `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset:

- `env -u CARGO_TARGET_DIR RUSTUP_TOOLCHAIN=1.97.0 cargo clippy --workspace --all-targets --locked -- -D warnings` — passed
- `cargo test --locked --test daemon_control_ownership` — 8 passed
- GHOSTSNP in `request.rs` reddens `production_sources_reject_terminal_drain_and_snapshot_phase_decode` naming that file
- Inserting `overlay_live_attach_occupancy` into `control.rs` reddens `control_rs_request_arm_rejects_inlined_post_processing`
- `./test.sh --locked` — passed, including `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach`

## Unverified behavior or residual risk

Downstream TUI/Web consumers were not rebuilt; public DTOs did not change.

## Missing vault guidance

None. Review asked for existing request/state types for Clippy and a dedicated Request owner; both were available without a new convention.
