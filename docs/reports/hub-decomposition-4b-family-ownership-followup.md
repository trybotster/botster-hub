# Implement follow-up: Hub decomposition 4b family ownership

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788036685_599372`) |
| Approved plan | `docs/plans/hub-decomposition-4b-move-daemon-ownership.md` revision 5 |
| Merge policy | direct into `main`; do not create a PR |
| Open findings closed | `finding_1788036661_971980` (high), `finding_1788036661_263219` (medium) |
| Runtime-teardown class | applies; every lens remains a survive-the-move invariant |

This visit is the return-to-Implement after Review `changes_required`. Routing is unchanged: ticket and run `target_id` still map to `trybotster/botster-hub`. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

[[project-pipelines-playbook]] was not loaded. This visit still changes no Project Pipelines package, plugin, or workflow-policy path.

### Targeted atomic notes

- [[daemon transport extraction moves ownership before deleting the facade]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[code moves need paired absence and presence source guards]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[hub moves must extend source scanning guard file lists]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[test script required for rust tests not cargo test]]

### Constraints applied before edits

- Keep Hub host-policy ownership. Do not change Core pin, DTO shapes, serde names, protocol version, or existing proof names.
- Move each request body into its named family module. Keep `src/daemon/control.rs` as delegation plus the existing Request post-processing (occupancy overlay, pump marks, drain owner snapshot).
- Keep `sessions.rs` limited to the session family.
- Fill `host.rs` and `plugins.rs` instead of leaving marker comments.
- Preserve match order, runtime borrow order, teardown gates, and skip-mode by keeping new guards in `tests/daemon_control_ownership.rs`.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/control.rs` | Dispatchers only: one-call family delegates; live-peer Request gate and Request post-processing stay here |
| `src/daemon/control/host.rs` | Check/Start/Get Hub update, HubUpdateCheckCompleted, DaemonShutdown |
| `src/daemon/control/webrtc.rs` | LocalWebrtcSignal, PeerClosed sweep, bootstrap already here |
| `src/daemon/control/spawn_targets.rs` | Spawn-target and worktree request match plus create/update/delete/validate bodies |
| `src/daemon/control/packages.rs` | Package/app/route/navigation/entrypoint request match plus entrypoint supervision |
| `src/daemon/control/plugins.rs` | Plugin MCP/surface runtime family and PluginLifecycleStatus |
| `src/daemon/control/messaging.rs` | Whoami, PostMessage, ReceiveMessages, AckMessage, NotifySession |
| `src/daemon/control/session_types.rs` | Session-type request match plus existing generation helpers |
| `src/daemon/control/sessions.rs` | Session family only, including ReadSessionContext |
| `src/daemon/control/entities.rs` | JSON-path reject for Subscribe/UnsubscribeEntities |
| `src/daemon/control/events.rs` | JSON-path reject for Subscribe/UnsubscribeEvents |
| `tests/daemon_control_ownership.rs` | Complete DaemonRequest/ControlMessage ownership matrix with wrong-owner red control |

## Ownership boundaries preserved

- Hub still owns control-plane topology, admission, package policy, and concrete transports.
- Control-plane families now have one module owner each. `control.rs` names variants only in delegating arms plus Request post-processing.
- `webrtc.rs` may construct `DaemonRequest::Detach` for the PeerClosed occupancy sweep; sessions remain the Detach owner.
- Public crate-root DTOs, `serve_daemon`, and `daemon_transport_request` are unchanged.
- Four WebRTC `has_live_peer` gates remain four distinct sites.
- Grant registry is still untouched; daemon modules still do not remove grant rows.
- Runtime-teardown lenses are unchanged: isolation, bounds, late-message matrix, production path, ownership identity, sibling fail-closed policy.

## Cross-repo routing

None. No Core pin change. No Hub-client DTO change. No wake-driven data plane. No dedicated DataChannels.

## Deviations from plan

- `ReadSessionContext` lives in `sessions.rs` rather than `session_types.rs`. The plan listed session-type catalog mutations under `session_types.rs` and did not name this variant; it is a session-context read, not a session-type mutation.
- `PluginLifecycleStatus` stays on the pre-runtime-borrow path (as before) and is owned by `plugins.rs`, not `packages.rs`.
- Request post-processing (occupancy overlay, pump marks, drain owner snapshot, shutdown-busy Hub update reply) remains in `control.rs` after family delegation, matching the plan's cross-family dispatcher note.

No plan-acceptance-check rewrite was required: check 3 is now implemented rather than deferred.

## Tests and downstream proof

Commands, always with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset:

- `cargo test --locked --test daemon_control_ownership` — 7 passed
- Red-on-revert: duplicated `DaemonRequest::SendInput` into `plugins.rs`; `each_daemon_request_has_exactly_one_family_owner` failed with `plugins.rs must not own DaemonRequest::SendInput`. Restored.
- Isolated `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach` — passed
- `./test.sh --locked --test hub_daemon_lifecycle_test` — 319 passed, 0 failed, 2 ignored
- `./test.sh --locked` — passed on a quiet host after two load-sensitive fails of `webrtc_terminal_adapter_bound_peer_loss_closes_adapter_without_hub_detach` while other binaries ran in parallel. Isolated and lifecycle-binary runs of that test stayed green. No production PeerClosed body change was required; the handler remains the moved original.

## Unverified behavior or residual risk

- The bound-peer-loss lifecycle test is ambient-load-sensitive at default workspace concurrency. Two full-suite runs failed it; the quiet-host rerun and the lifecycle-only binary passed. Residual risk is harness load, not a family-ownership defect.
- Downstream TUI/Web consumers were not rebuilt this visit; public DTOs did not change.

## Missing vault guidance

None discovered this visit. The Review findings matched the plan's family partition; the missing piece was completing that partition rather than a vault gap.
