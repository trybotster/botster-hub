# Implement report: publish exact Unix attach occupancy after connection EOF

| Field | Value |
| --- | --- |
| Ticket | `ticket_1786870433_515008` |
| Run | `run_1786870436_668945` |
| Step | `botster_stack_implement` |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Authoritative path | spawn target `botster-hub` via `list_spawn_targets` (`~/Projects/botster-hub`) |
| Pipeline worktree | this ticket worktree on `project-pipelines/ticket_1786870433_515008` |
| Base | Hub `origin/main` `60b79b814df0af234c8b4d6429b6c577b52c6dd6` |
| Locked Core | `Cargo.lock` pins `botster-core` / `botster-core-daemon` at `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Delivery | direct-merge; no pull request |
| Class | runtime-teardown (`teardown_class_applies: yes`) |
| Plan | `docs/plans/publish-exact-unix-attach-occupancy-after-connection-eof.md` revision 3 |
| Session-type eligibility consumer | false |
| Hub-test-support version | `0.1.37` remains unpublished on npm; fixture bytes stay on this unpublished coordinate |
| Protocol | 7 / conformance 43 |

Independent routing: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `botster-hub`. The approved plan used the same target. This run did not route from the process working directory.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]] — public DTO / feature / TypeScript overlay inside this repo

### Class overlay

- [[botster runtime teardown lenses]] — every lens implemented below

### Targeted atomic notes

- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[botster hub client crate is the external client boundary]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[terminal subscription lifecycle is Core owned while host session policy is Hub owned]]
- [[Core terminal subscription ownership is session, subscription, and generation]]
- [[daemon socket attach must detach subscriptions on disconnect and exit]]
- [[PeerClosed attach occupancy must use the live attach route set]]
- [[mux envelope delivery does not prove Hub route ownership]]
- [[Hub route registry names describe ownership not attach queues]]
- [[attach failed cleanup is route aware and idempotent]]
- [[Unix mux host events are unsolicited control frames]]
- [[first-party Unix attach clients use split Hello and subscription close events]]
- [[an ablation that reddens at the first assertion does not vouch for later ones]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[live hub proof records distinct hub and locked core binary provenance]]
- [[Hub bee15e7 builds the session worker from botster-core-daemon]]
- [[hub generated protocol changes are a four site release chain]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[colon worktree paths break cargo dyld library paths]]

**Not loaded:** [[project-pipelines-playbook]] — Project Pipelines package/plugin paths and workflow-policy implementation are out of scope. Other repository charters were not loaded.

### Constraints applied before edits

- Work only in this `botster-hub` ticket worktree.
- Public occupancy oracle is sibling `DaemonRequest::Status`.
- Unix EOF cleanup is generation-scoped for this `client_id`. It does not send pair-only `DaemonRequest::Detach`. It does not `ShutdownSession`.
- Occupancy updates go through `record_attached_subscription_change`.
- `FEATURE_ATTACH_OCCUPANCY` is advertised only. Default `DaemonCompatibilityRequirement::current()` does not require it.
- Protocol stays 7. Conformance becomes 43.
- IsolatedHub ablations use a `BOTSTER_ENV=test` env hook because IsolatedHub is a real `botster-hub` process.
- No TUI, Web, or Core source edits.

## Files changed

| Path | Change |
| --- | --- |
| `crates/botster-hub-client/src/lib.rs` | `FEATURE_ATTACH_OCCUPANCY`, `DaemonAttachOccupancy`, `DaemonStatus.live_attach_occupancy`, conformance 43, `for_attach_occupancy()`, advertised feature list, compatibility tests |
| `crates/botster-hub-client/src/typescript.rs` | optional occupancy field and interface |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | regenerated protocol artifact |
| `src/daemon_projection.rs` | Status constructor includes empty occupancy |
| `src/daemon_transport.rs` | `RegisterUnixAdmission` oneshot ack; Unix EOF cleanup uses live generation + Core detach + `record_attached_subscription_change`; Status overlays Hub∪Core occupancy; IsolatedHub ablation env; unit tests |
| `src/daemon_attach_stream.rs` | `recorded_generation` helper |
| `src/lib.rs` / `src/main.rs` | re-export and stopped-status literal |
| `crates/botster-hub-test-support/src/isolated_hub.rs` | `IsolatedHubBuilder::env` for production-process ablation |
| `tests/hub_daemon_lifecycle/session_fixtures.rs` | IsolatedHub env helper |
| `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs` | IsolatedHub occupancy, two occupancy ablations, pair-only Detach ablation, Spawn-then-EOF, replacement-owner occupancy check |
| `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs` | conformance 43 pin |
| `docs/client-protocol.md` / `README.md` | occupancy oracle, feature token, pair-absence meaning |
| `packages/hub-test-support/*` | synced protocol, matrix (`attach_occupancy` in `supported_features` only), metadata checksums, README; version stays `0.1.37` |
| `docs/plans/publish-exact-unix-attach-occupancy-after-connection-eof.md` | approved plan revision 3 |
| `docs/reports/publish-exact-unix-attach-occupancy-after-connection-eof-implement.md` | this report |

## Ownership boundaries preserved

- Hub owns Unix EOF cleanup, `live_attach_routes`, Status occupancy projection, and `RegisterUnixAdmission` ack.
- `botster-hub-client` in this repo owns the public DTOs, feature token, and generated TypeScript.
- Core still owns `(session, subscription, generation)` and `detach_terminal_subscription`. No Core ticket.
- TUI already depends on this ticket (`dependency_1786870438_296010`). This run did not edit TUI.
- Web is out of scope. npm publish of `@trybotster/hub-test-support` is not in this ticket.

## Cross-repo dependencies or separately routed work

- No new outbound dependency.
- TUI `ticket_1786868597_171437` consumes the public Status field after merge.
- Downstream proof here is `botster-hub-client` Status reachability plus a scratch-patch TUI compile probe. This run did not run `script/test-live-hub ghostty-shared`.

## Deviations from plan

- IsolatedHub is a subprocess, so `#[cfg(test)]` hooks cannot ablate production cleanup. Ablations use `BOTSTER_HUB_UNIX_EOF_ABLATION` when `BOTSTER_ENV=test`, plus `IsolatedHubBuilder::env`. The production path stays env-free.
- The test-only `handle_connection` wrapper no longer enqueues pair-only `DaemonRequest::Detach` after EOF. That wrapper was never the production path.
- `@trybotster/hub-test-support@0.1.37` is not on npm (`0.1.36` is latest). Source stays `0.1.37`. No `0.1.38` cutover.
- TUI `--all-targets` failed first on the pinned old `botster-hub-test-support@4f30d695` (`FEATURE_TERMINAL_STREAMING` / `FEATURE_RESIZE`), not on a missing `live_attach_occupancy` field. Production `cargo check --workspace` compiled clean. The TUI test helper `status_response_with_package_counts` is a full `DaemonStatus` literal and will need the new field on the TUI ticket.
- `project_pipelines_create_vault_checklist` timed out twice. Workflow evidence is in this report. No second Plan checklist was created.

## Runtime-teardown lenses implemented

| Lens | Implementation |
| --- | --- |
| Isolation | One Unix connection releases only generations still owned by its `client_id`. Sibling attach on the same session stays. Host session stays. Entity subscriptions on that connection still release. |
| Bounds | Cleanup stays on the owner thread. `detach_terminal_subscription` is the existing synchronous Core host-tick API. Adapter close is non-blocking. No `block_on` of a hanging close. Typed Core miss is already-gone. Other Core detach errors increment `cleanup_failed` for that row only. |
| Late-message matrix | Ordinary requests remain request/response. `RegisterUnixAdmission` waits for owner insert ack before the request loop. EOF cleanup does not send pair-only `Detach` or `ShutdownSession`. Replacement-owner protection is live generation lookup on a different connection. |
| Production-path proof | IsolatedHub Unix: Hello → RegisterUnixAdmission ack → request loop → socket EOF → `ConnectionCleanupGuard` → `handle_connection_cleanup` → generation-scoped Core detach → `record_attached_subscription_change(Detach)` → sibling Status omits the old pair. |
| Ownership identity | Core owner is `(session_id, subscription_id, generation)`. Hub occupancy key is the pair. Connection owner is `client_id`. Public Detach is pair-only. EOF uses the generation still owned by this `client_id`. |
| Sibling / fail-closed | Successful EOF keeps sibling attach and host session. Cleanup failure increments `cleanup_failed`, does not `ShutdownSession`, does not detach the sibling, and does not invent occupancy absence. |

## Tests and downstream proof run

Locked worker first:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
```

Production IsolatedHub proof (`./test.sh --test hub_daemon_lifecycle_test unix_eof_`):

- `unix_eof_releases_exact_attach_occupancy_on_sibling_status` — sibling Status advertises `attach_occupancy`, lists both pairs, then omits only A's pair after A's socket EOF. B `SendInput` is accepted. Host `ListSessions` still contains the session.
- `unix_eof_leave_route_ablation_keeps_named_pair_on_status` — first failure location is exact-absence.
- `unix_eof_skip_core_detach_ablation_keeps_named_pair_on_status` — first failure location is exact-absence.
- `unix_eof_pair_only_detach_ablation_drops_replacement_owner_generation` — first failure location is B's generation occupancy.
- `unix_spawn_then_eof_keeps_host_session`
- `unix_adapter_stale_disconnect_does_not_cancel_replacement_owner` plus public occupancy of B's pair

Unit:

- `register_unix_admission_acks_before_request_loop`
- `client_eof_detaches_connection_subscriptions` (no pair-only Detach on the socket after EOF)
- `occupancy_rows_union_hub_routes_and_core_inventory`
- `independent_counter_sub_does_not_clear_named_occupancy`
- `default_requirement_accepts_daemon_before_optional_attach_occupancy`
- `attach_occupancy_requirement_rejects_old_daemon_and_accepts_current_daemon`

Gates:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- `npm run check --prefix packages/hub-test-support`
- Full `./test.sh` after the locked worker build (recorded in gate evidence)

Live IsolatedHub provenance from the occupancy test:

- Hub binary: `.../target/debug/botster-hub` from this worktree
- Session worker: `.../target/debug/botster-session-worker`
- Hub source lineage: `60b79b8` plus this commit
- Locked Core: `fc541a59`

Scratch TUI probe (`fc1ff62`, isolated worktree, `CARGO_TARGET_DIR` local, then removed):

- `cargo check --workspace`: pass. Production TUI compiles against the candidate `botster-hub-client`.
- `cargo check --workspace --all-targets`: fail in pinned `botster-hub-test-support@4f30d695` (`FEATURE_TERMINAL_STREAMING`, `FEATURE_RESIZE`). Not a Status occupancy field error. TUI test helper `status_response_with_package_counts` still names every `DaemonStatus` field and will need `live_attach_occupancy` on the TUI ticket.

`near_limit_snapshot_assembly_stays_within_owner_turn` is a 25ms wall-clock owner-turn test in untouched `daemon_entity_subscriptions.rs`. Isolated command `cargo test --locked -p botster-hub --lib near_limit_snapshot_assembly_stays_within_owner_turn` passed on this branch (3x) and on `~/Projects/botster-hub` main (1x). Parallel `--lib` on main once passed 348/348 and once failed 346/348 (`near_limit` plus `separators_close_when_item_bytes_fit_but_commas_do_not`). Parallel `--lib` on this branch failed only `near_limit` (350/351). This is a pre-existing parallel-load flake, not occupancy behavior. Occupancy IsolatedHub tests pass.

## Unverified behavior or residual risk

- This run did not run TUI `script/test-live-hub ghostty-shared`.
- npm publish of `@trybotster/hub-test-support` is intentionally not done.
- Web consumers of generated protocol are not updated.
- A delayed IsolatedHub Status probe can increment `cleanup_completed` before a later attach EOF. Occupancy tests wait for that counter, then assert pair absence. They do not treat the counter as the occupancy oracle.
- `BOTSTER_HUB_UNIX_EOF_ABLATION` is readable whenever `BOTSTER_ENV=test`. It is not a production control.

## Missing vault guidance discovered

Still true after Implement. Capture after commit:

1. Unix EOF occupancy must share `record_attached_subscription_change` / `live_attach_routes` with Attach, Detach, and PeerClosed.
2. A public occupancy oracle must union Hub routes with Core inventory.
3. `live_attach_subscriptions` and sibling echo are not identity oracles. The feature token is required so an omitted field cannot be read as absence.
4. Ordinary Unix requests are request/response. Only `RegisterUnixAdmission` needs an owner ack.

No convention conflict with loaded notes.

## Assumptions

- Sibling `DaemonRequest::Status` is the public query.
- Including `generation` on the public row is allowed. TUI lookup remains the pair.
- Empty occupancy without `attach_occupancy` is not proof.
- Direct merge after Review/Verify. This Implement step commits on the ticket branch and does not open a PR.
