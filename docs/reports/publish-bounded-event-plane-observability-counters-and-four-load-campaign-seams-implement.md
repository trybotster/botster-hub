# Implement report: Hub bounded event-plane observability counters and four load-campaign seams

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| `target_id` | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Pipeline worktree | Hub run worktree for `ticket_1787267568_492780` |
| Ticket | `ticket_1787267568_492780` |
| Run | `run_1787278338_832165` |
| Step | `botster_stack_implement` (`run_step_1787328774_416743`) |
| Review return | `review_1787328763_940695` (`changes_required`) |
| Approved plan | `docs/plans/publish-bounded-event-plane-observability-counters-and-four-load-campaign-seams.md` revision 15 |
| Merge policy | `direct` into `main`; do not create a PR |
| Implement commit | `6814d4b2ca6b8ec6e127108faf567c95f0047b7f` |
| Review-return commit | `cd3cb2e014cadb8cca09057a84de75cc63450f17` |
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
- `tests/hub_lua_runtime_test.rs`
- `docs/plans/publish-bounded-event-plane-observability-counters-and-four-load-campaign-seams.md` (path-neutral scrub only)
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

## Review findings addressed

| Finding | Severity | Resolution |
| --- | --- | --- |
| `finding_1787328763_312074` mailbox age stale / unbounded | high | `take_ready_event`, expiry, unsubscribe, and connection cleanup now publish or retire mailbox age. Churn test prunes retired rows on the next live subscribe. |
| `finding_1787328763_424511` consumer oldest age uses `now` | high | `update_consumer_age` reads the front envelope timestamp after enqueue, expiry, pull, byte-limit requeue, and delivery requeue. |
| `finding_1787328763_203686` per-invocation env reads | high | `HubRuntime` reads the four seams once. `LuaPluginRuntime` stores `event_handler_hold_ms` and does not reread the environment. |
| `finding_1787328763_554792` missing live T1/T4 proofs | high | `apply_plugin_completion` distinguishes `TimedOut` from `HandlerFailed` on an in-flight package-event. T4 now passes `HubRuntime` and asserts `stalled_write_timeouts == 1` and `stalled_writes == 2`. |
| `finding_1787328763_519287` TUI/Web compile probe | high | Disposable TUI helper adds `observability`. After that field, remaining TUI errors are `botster-ui-contract` 0.3.2 versus 0.3.3, not a missing status field. Web drift against the new artifact fails as expected. Web `typecheck` and `build` pass after installing local `@trybotster/ui-contract@0.3.3` and copying the new protocol. No consumer repo is edited. |
| `finding_1787328763_795396` clippy constant asserts | high | Lifecycle pin tests use `const _: () = assert!(CONFORMANCE_FIXTURE_REVISION >= 45)`. `cargo clippy --workspace --all-targets --locked -- -D warnings` passes. |
| `finding_1787328763_872765` home path / session UUID | high | Plan and report now use path-neutral labels. A scan of added lines found no home paths or session UUIDs. |
| `finding_1787328763_714973` lifecycle suite dirty | medium | Census leftover workers are all `botster-session-worker` binaries from the primary Hub checkout, not this run worktree. This branch's `hub_daemon_lifecycle_test` ran 265 passed, 1 ignored, 0 failed inside `./test.sh --locked`. |

## Deviations from plan

1. **Loom AC19 case 0 is unproven.** `RUSTFLAGS="--cfg loom"` still fails compiling `webrtc`. Deterministic seqlock tests remain.
2. **TUI scratch full `cargo check --workspace` does not go green.** After the one required `observability` field, remaining errors are a dual `botster-ui-contract` pin (TUI 0.3.2 versus this Hub 0.3.3), not the status DTO field. No TUI source is committed.
3. **`script/run-lifecycle-suite` remains `verdict=environment_dirty`.** Leftover workers belong to the primary Hub checkout. This run did not kill those processes.
4. **A3 is still not a Core waiter live proof.** T1 is proven through the production `apply_plugin_completion` classifier on an in-flight package-event, not through Core timing out a Lua mutex hold.
5. **Some plan red-first variants against withdrawn designs were not compiled as alternate implementations.**

## Tests and downstream proof run

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | pass |
| Prebuild `botster-session-worker` then `botster-hub` locked | pass |
| `./test.sh --locked` | pass; zero failures. Hub lib 482 passed. `hub_daemon_lifecycle_test` 265 passed, 1 ignored, 0 failed |
| `RUSTFLAGS="--cfg loom" cargo test -p botster-hub --lib queue_age_model` | fail to compile `webrtc`; loom model unproven |
| `script/run-lifecycle-suite` | `verdict=environment_dirty`. Census leftover binaries are all from the primary Hub checkout `target/debug/botster-session-worker`, not this run worktree |
| Hub-client serde, optionality, unknown-kind/state, generated TypeScript drift | pass |
| Mailbox pop/unsubscribe/expiry/churn age tests | pass |
| Consumer front-envelope age tests (enqueue, pull, requeue, expiry, byte-limit) | pass |
| T1 `TimedOut` versus `HandlerFailed` on `apply_plugin_completion` | pass |
| T4 timeout versus other write with `HubRuntime` | pass |
| Four seam `*_from` inertness tests | pass |
| Packed `@trybotster/hub-test-support@0.1.41` + `@trybotster/ui-contract@0.3.3` local install | pass |
| Web scratch `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` `npm test` | expected drift (exit 1) against vendored `daemon-protocol.ts` |
| Web scratch `npm run typecheck` and `npm run build` | pass after installing local ui-contract 0.3.3 and copying the new protocol |
| TUI scratch compile | one `cfg(test)` `observability` field is the DTO cost. Remaining errors after that field are ui-contract 0.3.2 versus 0.3.3, not a missing status field |

Production entry point: `DaemonRequest::Status` projects `HubRuntime::event_plane_counters_snapshot()` into `DaemonStatus.observability`. Ingress, delivery, T1–T4, owner-turn, and ready-wait update those atomics on the live daemon paths. Age cells are created after admission commit, not on the event allocator path.

## Unverified behavior or residual risk

- Loom does not prove the `AcqRel` opening RMW on this compile graph.
- Lifecycle suite was not a clean verdict in this environment.
- T1 `TimedOut` while Lua is still inside `invoke` was not live-proven against Core's waiter (assumption A3). The production classifier is covered.
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
