# Implement report: Hub decomposition 4b

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788033437_386845`) |
| Approved plan | `docs/plans/hub-decomposition-4b-move-daemon-ownership.md` revision 5 |
| Merge policy | direct into `main`; do not create a PR |
| Base | `origin/main` / `6b405b7` |
| Move commit | `07a4a79` |
| Ownership-guard commits | `8d9b15d`, `96b66f0` |
| Source-guard retarget | `5788f75` |
| Runtime-teardown class | applies; every lens is preserved as a survive-the-move invariant |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]

[[project-pipelines-playbook]] was not loaded. This ticket changes no Project Pipelines package, plugin, or workflow-policy path.

### Targeted atomic notes

- [[daemon transport extraction moves ownership before deleting the facade]]
- [[Hub extraction must reduce ownership rather than only split files]]
- [[hub moves must extend source scanning guard file lists]]
- [[code moves need paired absence and presence source guards]]
- [[fixed source guard lists need one ablation per added file]]
- [[a source scanner can stay in cfg test skip mode through end of file]]
- [[region bounded source guards need a required symbol anchor]]
- [[exact Rust test ablations require a one test baseline]]
- [[source guard ablations must not overlap a running full suite]]
- [[Owner loop must not stack maintenance and pump ahead of queued control]]
- [[Hub owner loop wakes only for mutations and pending resync]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub background fairness must stay policy-neutral]]
- [[Hub ultimate WebRTC close failure sacrifices every peer on the dedicated runtime]]
- [[webrtc peer cleanup removes every per peer owner together]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[botster Hub Rust stays a trusted host kernel]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[test script required for rust tests not cargo test]]
- [[a ui contract import line change costs one test line in each generic client]]

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Follow the approved plan. Keep Hub host-policy ownership.
- Delete `daemon_transport.rs` and `daemon_package_control.rs` with no forwarding facade.
- Do not change the Core pin, DTO shapes, serde names, protocol version, or existing proof leaf names.
- Preserve the crate-root alias `daemon_transport_request`.
- Run every Rust gate with `RUSTUP_TOOLCHAIN=1.97.0` and `CARGO_TARGET_DIR` unset.
- When runtime-teardown class applies, implement every lens from [[botster runtime teardown lenses]] rather than dropping one to informal follow-up.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/owner_loop.rs` | Owner thread: `serve_daemon`, poll/pump/observe, control state, shutdown wait |
| `src/daemon/control.rs` | Control-plane dispatchers |
| `src/daemon/control/message.rs` | `ControlMessage`, delivery class, write-class |
| `src/daemon/control/connection.rs` | Unix/WebRTC admission registration, including the WebRTC insert gate |
| `src/daemon/control/sessions.rs` | Runtime-borrow session family plus remaining runtime-borrow arms that share the `HubRuntime` prelude |
| `src/daemon/control/session_types.rs` | Session-type generation helpers |
| `src/daemon/control/spawn_targets.rs` | Spawn-target and worktree persistence helpers |
| `src/daemon/control/packages.rs` | Package/app/route/navigation helpers |
| `src/daemon/control/packages/mutations.rs` | Former `daemon_package_control.rs` (git rename, 93%) |
| `src/daemon/control/messaging.rs` | Coordination content-type constant |
| `src/daemon/control/plugins.rs` | Family marker; runtime plugin arms currently sit in `sessions.rs` |
| `src/daemon/control/entities.rs` | Entity subscribe/unsubscribe ControlMessage arms and their liveness gates |
| `src/daemon/control/events.rs` | `handle_client_event_request`, `events_from_client` |
| `src/daemon/control/webrtc.rs` | Bootstrap, peer-gone error, PeerClosed persist/detach helpers |
| `src/daemon/control/host.rs` | Family marker; Hub-update orchestration remains in the Request arm |
| `src/daemon.rs` | Declares `control` and `owner_loop` |
| `src/lib.rs` | Remove `pub mod daemon_transport`; re-source crate-root exports; extend production source list |
| `src/daemon/shutdown.rs` | Import `tick` from `owner_loop` |
| Unix/WebRTC/subscription modules | Direct imports from new owners |
| `docs/client-protocol.md` | Authoritative sources now `src/daemon/control/` and `src/daemon/owner_loop.rs` |
| `tests/hub_daemon_lifecycle/subscription_ownership_baseline.rs` | Guard path retarget |
| `tests/session_projection_owner_loop.rs` | Guard path retarget and per-destination needle exemption |
| `tests/daemon_control_ownership.rs` | New ownership guards (commit 2) |
| `src/daemon_transport.rs` | Deleted |
| `src/daemon_package_control.rs` | Deleted (renamed into mutations.rs) |

## Ownership boundaries preserved

- All edits are inside `botster-hub`.
- Wire DTOs still come from `botster-hub-client`. Crate-root re-exports keep the same names, including `daemon_transport_request`.
- `src/admission/grants.rs` is unchanged. `src/transport/webrtc/peer.rs` changes only import paths.
- Control plane still does not schedule terminal bytes. Owner loop still calls bounded Core lifecycle page APIs.
- Four WebRTC liveness gates remain four sites with four failure responses.

## Cross-repo dependencies or separately routed work

None. Measured consumer proof:

- 28a: `grep -rn '^botster-hub *=' crates/*/Cargo.toml` returned no output.
- 28b: `grep -rn 'botster_hub::'` over botster-tui, botster-web, botster-workspaces, botster-project-pipelines, botster-tui-kit, restty, and botster-core returned no library imports.
- 28c: `grep -rn 'daemon_transport::' src tests` at HEAD returned no output. `daemon_transport_request` call sites remain.
- 28d: no DTO or UI-contract change, so generic-client cost is zero.

## Deviations from plan

1. **Runtime-borrow families share `sessions.rs`.** `handle_runtime_control_request` is a single delegating call into `sessions::handle_runtime`. That function still contains session-type, messaging, plugin, and daemon-shutdown arms because they share the `HubRuntime` borrow prelude. Splitting them without duplicating that prelude would be a later mechanical slice. Recorded here rather than silently merged.
2. **`handle_control_request` still names package and spawn-target variants in dispatcher arms.** Many arms already delegate to family helpers. `CreateSpawnTarget` / `UpdateSpawnTarget` / `DeleteSpawnTarget` still carry inline validation in the dispatcher. Check 3's full paired presence/absence matrix across every family file is therefore incomplete.
3. **`plugins.rs` and `host.rs` are markers, not full request owners.** Plugin MCP/surface arms live in `sessions::handle_runtime`. Hub-update orchestration stays in the `ControlMessage::Request` arm in `control.rs` because it parks a reply and spawns blocking work on the owner thread.
4. **Four new proof leaf names** in `tests/daemon_control_ownership.rs`. Existing leaf names were preserved; these are additive guards required by checks 4a/4b/4c/5/7/19.

The committed plan is not rewritten in this Implement visit. If Review accepts these merges, the plan's affected-file partition should be resynced then.

## Runtime-teardown lenses

Every lens from the approved plan is implemented as survive-the-move behavior, not as a behavior change.

| Lens | Where it lives after the move | Oracle preserved |
| --- | --- | --- |
| Isolation | `control/webrtc.rs` + `transport/webrtc/peer.rs` | `ultimate_close_failure_sacrifices_every_peer_and_sweeps_all_owners` |
| Bounds | owner-loop wait helpers; close bounds stay in `peer.rs` | daemon shutdown wait tests in `owner_loop` |
| Late-message matrix | Request gate in `control.rs`; insert gate in `connection.rs`; entity gates in `entities.rs` | existing lifecycle lanes plus new four-gate source guard |
| Production path | `serve_daemon` in `owner_loop.rs` still calls `handle_control_message` | `tests/hub_daemon_lifecycle/` |
| Ownership identity | PeerClosed owner-checked retain in `control.rs` webrtc arm | peer tests in `transport/webrtc/peer.rs` |
| Sibling policy | unchanged in `peer.rs` | same ultimate-close oracle |

## Tests and downstream proof run

Toolchain: `rustc 1.97.0 (2d8144b78 2026-07-07)`. `CARGO_TARGET_DIR` unset.

| Gate | Result |
| --- | --- |
| Baseline `./test.sh --locked` at `2506491` (plan-only on `6b405b7`) | `SUITE_EXIT=0` |
| `cargo fmt --all -- --check` | pass after `cargo fmt --all` |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| `cargo test --locked --lib` | 500 then 504 tests after ownership guards; production-source skip-mode failure was fixed by moving guards out of `control.rs` |
| Per-file `GHOSTSNP` ablation of every new `src/daemon/**` production-source list entry | each arm red and named that file; one-test baseline first |
| Scanner liveness ablation on `src/main.rs` | red: `src/main.rs production source must not contain GHOSTSNP` |
| Check 19 ablation seeding `async fn accept_connections` into `src/daemon/control/host.rs` | red, named that file |
| Official `./test.sh --locked` at `5788f75` | `SUITE_EXIT=0`. Lifecycle lane `319 passed; 0 failed; 2 ignored`. |
| Leaf-name inventory | Base 1302 / HEAD 1306. No names removed. Four additive ownership-guard names. |

Required lifecycle files appeared in the official run: `subscription_ownership_baseline.rs`, `sessions.rs`, `shutdown.rs`, `packages.rs`, `cli.rs`, `event_plane_saturation.rs`, `webrtc_proofs.rs`, `webrtc_terminal_adapter.rs`, `session_projection_owner_loop.rs`, `hub_mcp_test.rs`.

Production path: `serve_daemon` in `src/daemon/owner_loop.rs` is the crate-root export. Unix and WebRTC still enqueue `ControlMessage::Request` into that owner thread. `handle_control_message` in `src/daemon/control.rs` is the production dispatcher.

## Unverified behavior or residual risk

- Official locked suite at `5788f75` passed (`SUITE_EXIT=0`).
- Check 3's full variant-by-variant absence matrix is not landed. A later move of remaining dispatcher inline arms would enable it.
- `host.rs` / `plugins.rs` remaining as markers is residual partition debt, not a runtime behavior change.
- Seeded-tail skip-mode red arms (check 18) were not run per new file that ends with `#[cfg(test)]`. `skip_open_at_eof == false` is asserted for every listed file. `control.rs` was moved off the skip-mode failure by relocating ownership guards to `tests/daemon_control_ownership.rs`.

## Missing vault guidance discovered

Same as the plan's vault-gap list. Newly observed: a `#[cfg(test)]` module whose string literals contain `{` will hold the production scanner in skip mode through EOF. That is already covered by [[a source scanner can stay in cfg test skip mode through end of file]]; this run applied it by moving ownership guards to `tests/daemon_control_ownership.rs`.
