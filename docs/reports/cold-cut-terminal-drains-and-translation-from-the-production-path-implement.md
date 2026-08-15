# Implement report: Hub cold-cut terminal drains and translation

Ticket: `ticket_1786661010_198387`
Run: `run_1786754929_522007`
Step: `botster_stack_implement`
Plan: `docs/plans/cold-cut-terminal-drains-and-translation-from-the-production-path.md` (rev 4)

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `botster-hub` (`trybotster/botster-hub`) |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Worktree HEAD before edits | `959c58f55726d098299cced8af151d8f496f41e3` |
| Locked Core SHA | `aef6516d5809d563961ed7fdd07da29a7b4edddc` |
| Merge policy | direct into `main`; no PR |

Independent routing matched the approved plan. This run did not infer the repository from the ambient directory.

## Repository playbook and other playbooks/notes applied

Applied before edits:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]] (class applies)
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[first-party clients put terminal mechanism tokens only in terminal compatibility]]

Not loaded: [[project-pipelines-playbook]] (package/plugin paths out of scope).

## Constraints applied

- Work stayed in this Hub worktree.
- Core `aef6516` is consumed, not reimplemented.
- Production terminal bytes stay on bound adapters. Hub does not decode READY/PAGE/FINISH or GHOSTSNP bodies.
- Host Drain remains readable on protocol 7 and returns no terminal bodies.
- `HubClientApi::Attach` fail-closes. Production Attach is Unix/WebRTC bind only.
- Host descriptor no longer advertises or requires `terminal_streaming`, `resize`, or `snapshot_delivery=ready_then_history`.
- Protocol stays 7. Conformance revision 42 / unpublished `@trybotster/hub-test-support@0.1.37`.
- Deleted Hub GHOSTSNP goldens were not restored.

## Files changed

- `Cargo.toml`, `Cargo.lock`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`, `crates/botster-hub-test-support/build.rs`
- `src/runtime.rs`, `src/client_api.rs`, `src/daemon_attach_stream.rs`, `src/daemon_transport.rs`, `src/daemon_entity_subscriptions.rs`, `src/main.rs`, `src/lib.rs`, `src/local_webrtc.rs`, `src/local_webrtc_smoke.rs`
- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `packages/hub-test-support/**` (0.1.37 / revision 42, regenerated)
- `README.md`, `docs/client-protocol.md`
- Tests under `tests/hub_client_api_test.rs`, `tests/hub_local_runtime_test.rs`, `tests/hub_daemon_lifecycle/*`
- Plan and this report

## Ownership boundaries preserved

Hub still owns admission, routes, adapters, host Drain, and the owner loop.

Core still owns attach generations, bind, observe/baseline, and terminal frames.

`botster-hub-client` remains the host DTO boundary. Terminal mechanism tokens stay on `Hello.terminal_compatibility`.

No TUI or Web source was edited. No Core APIs were implemented in Hub.

## Cross-repo dependencies or separately routed work

Closed dependencies used as given:

- Web `ticket_1786661008_897067`
- TUI planes `ticket_1786661009_551067`
- TUI Hello repair `ticket_1786756492_156718` at `fc1ff6238ae707c355febbc03eeab5130cccf91c`

No new Core ticket. Live TUI (`fc1ff623`) and live Web attach against this candidate Hub remain Verify work.

## Deviations from plan

None accepted as scope changes.

Production implementation follows rev 4: Core pin, observe/baseline slices, always bind, fail-closed local Attach, empty Attach bodies, host-only Drain, no `ATTACH_DRAIN_INTERVAL`, host tokens removed, protocol 7.

`HubRuntime::drain_subscription` / `drain_runtime_once` compile only under `cfg(test)` for in-crate unit tests. Integration tests use observe + ReadScreen. Production owner loop, Drain handler, entity tick, and smoke do not call them.

Review `review_1786778236_174399` required four follow-ups: keep the canonical session projection current with zero subscribers using paged observe/baseline/journal APIs; delete Hub terminal event translation and retired attach-phase predicates; remove Hub-owned terminal mechanism constants; scan every production item, not the prefix before the first `#[cfg(test)]`.

`DaemonConnection` skips Unix mux terminal frames when reading a host response and retains those frames for callers. Always-bind inserts mux frames on default Unix connections; without this skip, host ReadScreen after Attach cannot parse. IsolatedHub live-byte tests now observe retained adapter frames instead of Drain bodies.

## Runtime-teardown lenses

| Lens | Implemented |
| --- | --- |
| Isolation | One attach owns one adapter, route, and generation. Sibling routes keep opaque frames. ProcessExited does not `ShutdownSession`. |
| Bounds | Owner-loop observe/baseline use 32-item / 64 KiB / 25 ms budgets. WebRTC local close bound is unchanged. Smoke waits stay deadline-bounded. No 25 ms Drain loop. |
| Late-message matrix | Hello reject, Attach fail-closed without bind, Drain authorize-only, PeerClosed + observe sweep, Detach generation-aware, entity unsubscribe independent of terminals. |
| Production-path hard-stop | IsolatedHub Unix bind + peer-loss WebRTC proofs drive production handlers. Adapter close uses the live route set. |
| Ownership identity | Hub `(client_id, session_id, subscription_id, generation)` plus Unix `client_id` or WebRTC `grant_id`. Stale N must not delete N+1 (existing replacement-owner tests kept). |
| Sibling / fail-closed | Success: siblings live. Ultimate WebRTC close failure: sibling attach/entity rows cleared by existing fail-closed handler. |

## Tests and downstream proof run

Commands:

```sh
cargo build --locked -p botster-core-daemon --bin botster-session-worker
cargo fmt --all -- --check
BOTSTER_ENV=test cargo clippy --workspace --all-targets --offline -- -D warnings
./test.sh --locked
```

Passed on this tree:

- Session-worker locked build
- rustfmt
- strict clippy
- Hub lib tests: 269 passed, including fail-closed local Attach, negative architecture scan, WebRTC bind/peer-loss/fail-closed sibling
- `hub_client_api_test`: 33 passed
- IsolatedHub Unix always-bind, empty Attach, host Drain empty, ReadScreen marker, replacement-owner
- Lifecycle oracles rewritten off Attach/Drain translation: mux frames, `ReadScreen`, host OperatorError, session-entity patches
- `hub_daemon_lifecycle_test`: 201 passed, 1 ignored (larger local many-PTY)
- Full `./test.sh --locked` workspace: all binaries ok (lifecycle 201/1 ignored; lib 269; no FAILED results)
- `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` passed in the locked suite
- Missing-session host Drain is a typed OperatorError (`drain_runtime` / `terminal_stream_unavailable`)
- SendInput/ModeGatedInput/Resize/Spawn/Shutdown/Remove observe through `pump_bound_unix_routes` so host inventory advances without terminal Drain
- Idle observe ticks the host logical clock on each slice
- Replacement Attach detaches generation N before bind of N+1
- Support-matrix descriptor test now requires `terminal_streaming`, `resize`, and `snapshot_delivery=ready_then_history` to stay off host `supported_features`
- Owner-loop session projection advances with zero entity subscribers; later subscribe receives the ended row (`session_projection_observes_exit_without_subscribers_then_later_snapshot_includes_ended_row`)
- `HubClientEvent` no longer has terminal variants. Architecture scan covers items after `#[cfg(test)]` imports
- Hub-owned `FEATURE_TERMINAL_STREAMING` / `FEATURE_RESIZE` / `FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY` constants deleted; negotiation uses `botster-terminal-protocol`

## Unverified behavior or residual risk

- Live TUI at `fc1ff623` and live Web against this candidate Hub were not attached in this Implement turn. Verify must run that proof and record Hub SHA plus locked Core SHA separately.
- `HubRuntime::drain_*` exists only under `cfg(test)` for in-crate unit tests.
- Control-thread `try_recv` prefers queued host requests over idle reconcile. Burst `ReadScreen` can delay the 500 ms idle observe until the queue drains. Mutations now observe on the request path.
- CoreDaemon on `aef6516` does not expose `pump_bound_adapters`. Owner-loop observe uses `observe_lifecycle_slice`, which calls Core `drain_runtime_once` internally.
- Downstream TUI/Web crates that still imported the deleted hub-client `FEATURE_*` constants must import `botster-terminal-protocol` instead. Those consumers are separately routed.

## Missing vault guidance discovered

None that blocked the cut.

After this ships, capture:

- Host Hello no longer carries Core terminal mechanism tokens. Protocol 7 stayed because live first-party clients are a deployment boundary.
- Hub production advances adapters through `observe_lifecycle_slice`, not `drain_subscription`.
