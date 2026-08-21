# Implement report: Hub bounded event-plane observability counters and four load-campaign seams

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Pipeline worktree | `/Users/jasonconigliari/botster-sessions/trybotster-botster-hub-project-pipelines-ticket_1787267568_492780` |
| Ticket | `ticket_1787267568_492780` |
| Run | `run_1787278338_832165` |
| Step | `botster_stack_implement` (`run_step_1787324301_257051`) |
| Approved plan | `docs/plans/publish-bounded-event-plane-observability-counters-and-four-load-campaign-seams.md` revision 15 |
| Merge policy | `direct` into `main`; do not create a PR |
| Implement commit | `6814d4b2ca6b8ec6e127108faf567c95f0047b7f` |
| Integrated base | `origin/main` `12e0cc6` (sibling notice-reaction merge, revision 45 / package `0.1.40`) |
| Locked Core | `7eafa470a18025895995bbedc20d34b58106a03b` |
| `teardown_class_applies` | false |
| `CONFORMANCE_FIXTURE_REVISION` | 46 |
| `@trybotster/hub-test-support` | `0.1.41` (in-tree cutover, no publish) |
| `PROTOCOL_VERSION` | 7 |
| `DEFAULT_MINIMUM_CONFORMANCE_FIXTURE_REVISION` | 36 |

Independent routing: ticket/run `target_id` and the approved plan both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. Work stayed in this run worktree after rebase onto the sibling merge.

Botster MCP was not available in this session (`BOTSTER_SESSION_UUID` was not expanded into the MCP child). Pipeline context was loaded from the Project Pipelines plugin SQLite store.

## Repository playbook and other playbooks/notes applied

### Playbooks

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster-hub-client-playbook]]

### Targeted atomic notes

- [[load diagnostics must not cost work proportional to what they measure]]
- [[saturation counters do not acquire the contended lock they report]]
- [[Hub event plane lacks seven load campaign signals]]
- [[package event handler timeouts are discarded as successful completions]]
- [[spawned Hub tests can reach only four of fourteen Core test builders]]
- [[hub client event queue max requires Botster test mode]]
- [[test names do not prove their bodies can fail on the named claim]]
- [[router ingress uses try_lock only and contention is shed_busy]]
- [[admitted event holders survive producer unload until Core completion]]
- [[events.emit is a non-blocking router ingress not an owner-pumped host bridge]]
- [[botster hub events use bounded priority lanes instead of unbounded queue fuses]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[Hub suite runs prebuild the session worker before the locked test wrapper]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[generated typescript dtos must encode serde field optionality]]
- [[generated dto drift tests need symmetric field and type checks]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub test support npm releases need external consumer smoke]]
- [[botster web generated protocol drift checks need explicit hub artifact paths]]
- [[conformance fixture revisions must be unique per published content]]
- [[test script required for rust tests not cargo test]]
- [[implementation artifacts must match actual git state]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]

### Explicitly not loaded

- [[project-pipelines-playbook]] — no Project Pipelines package/plugin path is in scope
- [[botster runtime teardown lenses]] — plan class is false

### Constraints applied before edits

- Work only in the Hub run worktree for `tgt_7e208a0c76a44980a83b63af976b1f22`
- Follow approved plan revision 15
- Keep Hub host-profile observation; do not change production budgets or retirement semantics
- Public DTO work follows the Hub-client charter inside this workspace member
- Rebase onto sibling `ticket_1787278643_145174` before writing revision 46 / package `0.1.41`

## Files changed

- `src/event_plane_counters.rs` (new)
- `src/lib.rs`
- `src/package_event_router.rs`
- `src/daemon_event_subscriptions.rs`
- `src/daemon_maintenance.rs`
- `src/daemon_transport.rs`
- `src/daemon_projection.rs`
- `src/local_webrtc.rs`
- `src/runtime.rs`
- `src/lua_runtime.rs`
- `src/main.rs`
- `Cargo.toml`
- `Cargo.lock`
- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`
- `packages/hub-test-support/daemon-protocol.ts`
- `packages/hub-test-support/package.json`
- `packages/hub-test-support/metadata.json`
- `packages/hub-test-support/README.md`
- `packages/hub-test-support/first-party-client-support-matrix.json`
- `packages/hub-test-support/late-attach-history-conformance-fixture.json`
- `packages/hub-test-support/mode-flags-conformance-fixture.json`
- `packages/hub-test-support/session-lifecycle-subscription-conformance-fixture.json`
- `packages/hub-test-support/session-plugin-binding-conformance-fixture.json`
- `docs/client-protocol.md`
- `README.md`
- `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`
- `tests/hub_daemon_lifecycle/webrtc_terminal_adapter.rs`
- `docs/reports/publish-bounded-event-plane-observability-counters-and-four-load-campaign-seams-implement.md`

## Ownership boundaries preserved

- Hub owns counters, timeout classification, owner-turn and ready-wait measurement, and the four test seams.
- Hub Client owns the public `DaemonStatus.observability` DTO, generated TypeScript, and conformance revision 46.
- Core is unchanged. T1 reads `PluginInvocationFailureKind` that Core already produces.
- Data plane is untouched. T4 classifies `io::ErrorKind` only.
- No edits in `botster-tui` or `botster-web`. No npm publish.

## Cross-repo dependencies or separately routed work

- Sibling Hub `ticket_1787278643_145174` is closed and merged (`12e0cc6`, revision 45 / `0.1.40`). This ticket rebased onto that merge and allocated 46 / `0.1.41`.
- Consumer saturation campaign `ticket_1786663585_879846` remains separately routed.
- TUI and Web source upgrades are consumer work. AC10 measured them from this run and did not edit those repositories.

## Deviations from plan

1. **Loom AC19 case 0 is unproven.** `RUSTFLAGS="--cfg loom" cargo test -p botster-hub --lib queue_age_model` fails compiling `webrtc` with type-inference errors. The `loom = "0.7.2"` cfg-gated dev-dependency is in the manifest and lockfile. Deterministic seqlock tests remain. This matches the plan fallback: report the ordering claim as unproven rather than drop it silently.
2. **TUI scratch Cargo patch did not compile.** Cargo could not fetch GitHub tag `botster-ui-contract-v0.3.3` from the TUI worktree. Source inspection of `botster-tui/crates/botster-tui/src/app.rs:26139` still shows the one `cfg(test)` `DaemonStatus` literal that lacks `observability`.
3. **`script/run-lifecycle-suite` returned `verdict=environment_dirty`.** The census named leftover `botster-session-worker` processes under `/Users/jasonconigliari/Projects/botster-hub`, not this worktree.
4. **Some plan red-first variants against withdrawn designs were not compiled as alternate implementations.** The landed tests assert the decided contracts (seqlock, intrusive list, lock-free snapshot, inert seams, diagnostic failure does not change acceptance).
5. **Botster MCP tools were unavailable.** Gate/artifact submission used the plugin SQLite identity of the run after local git durability.

## Tests and downstream proof run

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Prebuild `botster-session-worker` then `botster-hub` locked | pass |
| `./test.sh --locked` | pass; zero failures |
| `RUSTFLAGS="--cfg loom" cargo test -p botster-hub --lib queue_age_model` | fail to compile `webrtc`; loom model unproven |
| `script/run-lifecycle-suite` | `verdict=environment_dirty` (unrelated `Projects/botster-hub` workers) |
| Hub-client serde, optionality, unknown-kind/state, generated TypeScript drift | pass |
| Router snapshot while inner held; diagnostic reserve does not change acceptance; registry lock does not block ingress | pass |
| Four seam `*_from` inertness tests | pass |
| Packed `@trybotster/hub-test-support@0.1.41` + `@trybotster/ui-contract@0.3.3` local install | pass: revision 46, protocol 7, optional `oldest_age_us` / `producer_generation` / `queue_count`, two permissive unions |
| Web scratch `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` `npm test` | expected drift against vendored `daemon-protocol.ts` |
| Web scratch `npm run typecheck` | pass against current vendored copy |
| TUI scratch Cargo patch | blocked on unpublished ui-contract tag; source cost is one `observability` field on the test helper |

Production entry point: `DaemonRequest::Status` projects `HubRuntime::event_plane_counters_snapshot()` into `DaemonStatus.observability`. Ingress, delivery, T1–T4, owner-turn, and ready-wait update those atomics on the live daemon paths. Age cells are created after admission commit, not on the event allocator path.

## Unverified behavior or residual risk

- Loom does not prove the `AcqRel` opening RMW on this compile graph.
- Lifecycle suite was not a clean verdict in this environment.
- T1 `TimedOut` while Lua is still inside `invoke` was not live-proven against Core's waiter in this run (assumption A3).
- TUI/Web consumer source edits are not in this ticket.
- Mailbox age cells use generation `0` keyed by connection id; replacement of a live mailbox identity is connection cleanup, not package generation.
- `#[global_allocator]` counting allocator is `cfg(test)` on the hub lib test binary only.

## Missing vault guidance discovered

- `RUSTFLAGS="--cfg loom"` is process-wide and can break unrelated crates. A crate-local loom harness is required before an executable model check can live beside WebRTC.
- A public DTO `#[non_exhaustive]` struct needs constructors for other workspace crates; `..Default::default()` struct expressions are illegal across the crate boundary.
- Conformance-revision pin tests that hardcode the previous integer fail on every additive DTO ticket unless they assert the constant or the default floor.

## Assumptions

- A9: npm registry latest published `@trybotster/hub-test-support` is `0.1.39`; sibling merged source is `0.1.40` / revision 45. This ticket writes `0.1.41` / 46.
- A3 remains unverified live: Core timeout while the Lua mutex is held.
- Merge policy `direct` means this run does not open a PR.
