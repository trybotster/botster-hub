# Implement follow-up: helper privacy and retargeted source-guard exemptions

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Ticket | `ticket_1787894965_150479` |
| Run | `run_1788030103_935368` |
| Step | `botster_stack_implement` (`run_step_1788043820_715305`) |
| Merge policy | direct into `main`; do not create a PR |
| Verify return | `review_1788043814_425916` (`changes_required`); findings listed in `artifact_1788043786_776517`, not as structured finding IDs |
| Runtime-teardown class | applies; production teardown bodies and live-peer gates were not edited |

Routing is unchanged: ticket and run `target_id` still map to `trybotster/botster-hub`. Work stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]]
- [[rust file splits can silently widen private helper visibility]]
- [[retargeted source guards must keep named exemptions]]
- [[hub moves must extend source scanning guard file lists]]
- [[code moves need paired absence and presence source guards]]
- [[Hub official gates must not set CARGO TARGET DIR]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[botster Hub pipeline shells can override RUSTUP TOOLCHAIN below the CI pin]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

[[project-pipelines-playbook]] was not loaded.

## Files changed

| Path | Why |
| --- | --- |
| `src/daemon/control/connection.rs` | File-local admission helpers are private |
| `src/daemon/control/entities.rs` | File-local subscribe/unsubscribe helpers are private |
| `src/daemon/control/host.rs` | File-local hub-update helpers are private; `hub_update_check_completed` stays `pub(crate)` |
| `src/daemon/control/packages.rs` | File-local package response helpers are private; `handle_request` stays `pub(crate)` |
| `src/daemon/control/packages/mutations.rs` | File-local compensation/reload helpers are private; public mutation APIs stay `pub(crate)` |
| `src/daemon/control/plugins.rs` | File-local `plugin_lifecycle_response` is private |
| `src/daemon/control/spawn_targets.rs` | File-local spawn/worktree helpers are private; `handle_request` stays `pub(crate)` |
| `src/daemon/control/webrtc.rs` | File-local persist/detach/bootstrap helpers are private; PeerClosed entry points stay `pub(crate)` |
| `tests/hub_daemon_lifecycle/event_plane_saturation.rs` | Successor file list complete; named `spawn_targets.rs` exemption plus presence assertion |
| `tests/hub_daemon_lifecycle/sessions.rs` | Three comments retargeted off `daemon_transport.rs` |
| `docs/reports/hub-decomposition-4b-visibility-and-source-guard-followup.md` | This report |

## Ownership boundaries preserved

Hub still owns daemon control-plane topology. Family entry points remain `pub(crate)` for real cross-module callers (`handle`, `handle_request`, `handle_runtime`, `handle_peer_closed`, `hub_update_check_completed`, `install_registry_package`, `session_type_entity_snapshot`, and the other crate-wide mutation/session-type APIs). File-local helpers no longer leak crate-wide. Public crate-root DTOs, `serve_daemon`, `daemon_transport_request`, serde names, protocol version, Core pin, grant-row retention, and the four WebRTC live-peer gates are unchanged.

Parent-module private helpers in `packages.rs` remain visible to `packages/mutations.rs` because Rust private items are visible to descendant modules.

## Verify findings (no structured IDs)

Verify listed three issues in `artifact_1788043786_776517` / `review_1788043814_425916` instead of `open_findings`. No `project_pipelines_resolve_finding` call is possible.

1. **Medium.** Restore minimum visibility on helpers that became `pub(crate)` without cross-file callers. 64 file-local helpers are private `fn` again. `cargo check --workspace --all-targets --locked` exits 0 after the narrowing.
2. **Low.** `event_plane_saturation_source_guards_hold` lists every successor daemon control file plus `owner_loop.rs`, `daemon_maintenance.rs`, `subscription/entity.rs`, and `session_projection.rs`. The permitted `package_event_router().try_ingress` site is named as `src/daemon/control/spawn_targets.rs` and must remain present.
3. **Low.** The three `tests/hub_daemon_lifecycle/sessions.rs` comments now name `src/daemon/control/sessions.rs` and `src/daemon/owner_loop.rs`. `tests/` has zero remaining `daemon_transport.rs` needles. Historical `docs/plans/` and `docs/reports/` records were left alone.

## Cross-repo routing

None. No Core pin, DTO, or consumer-import change.

## Deviations from plan

None. This visit restores move-only ownership tightness that the file split accidentally widened. It does not add the wake-driven data plane or dedicated DataChannels.

## Tests and downstream proof

Always `RUSTUP_TOOLCHAIN=1.97.0` (`rustc 1.97.0 (2d8144b78 2026-07-07)`) and `CARGO_TARGET_DIR` unset:

- `cargo fmt --all -- --check` — exit 0
- `git diff --check` — exit 0
- `cargo clippy --workspace --all-targets --locked -- -D warnings` — exit 0
- `cargo check --workspace --all-targets --locked` — exit 0 after the visibility narrowing
- `cargo test --locked --test hub_daemon_lifecycle_test event_plane_saturation_source_guards_hold -- --exact` — 1 passed
- `cargo test --locked --test daemon_control_ownership` — 8 passed
- Seed `package_event_router().try_ingress` into `src/daemon/control/host.rs` — red: `src/daemon/control/host.rs operation handlers must not wait on router ingress`
- Rename the permitted `try_ingress` call in `spawn_targets.rs` — red compile (`try_ingress_removed` is not a method)
- Tree restored after each ablation; `git status --porcelain` matched the intended uncommitted set before commit
- Prebuild: `cargo build --locked -p botster-core-daemon --bin botster-session-worker` and `cargo build --locked --bin botster-hub`
- `./test.sh --locked` — exit 0; 1307 passed, 0 failed, 3 ignored across workspace binaries plus doctests. `hub_daemon_lifecycle_test` 319 passed, 2 ignored in 271.84s

## Runtime-teardown lenses

This visit did not edit PeerClosed, grant retention, late-message reject/sweep, or hard-stop production paths. Isolation, bounds, late-message matrix, production-path proof, ownership identity, and sibling fail-closed policy remain the survive-the-move invariants from the approved plan.

## Unverified behavior or residual risk

- Downstream TUI and Web were not rebuilt; no public seam they consume changed.
- Remaining `pub(crate)` helpers in `src/daemon/shutdown.rs` and `src/daemon/error.rs` that are file-local were not narrowed. They predate this split and were not in the Verify finding.
- Ablation of the spawn_targets presence assertion reddened at compile time rather than at the assertion line because the renamed method does not exist. The assertion still requires the exact needle in `spawn_targets.rs` on the green path.
- `unreachable_pub` remains disabled, so a future split can still widen visibility without Clippy.

## Missing vault guidance

None. Verify already captured [[rust file splits can silently widen private helper visibility]] and [[retargeted source guards must keep named exemptions]]. This visit applied those notes.

## Git state

- Repair commit: `83bbfa0`
- Branch: `project-pipelines/ticket_1787894965_150479`
- No PR, because the ticket requires a direct merge to main
