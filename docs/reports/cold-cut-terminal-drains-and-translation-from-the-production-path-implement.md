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
| Locked Core SHA | `fc541a59338d0591ba4fb3fa522a030d212d26d0` |
| Merge policy | direct into `main`; no PR |
| Review follow-up | `review_1786847824_730324`; Web detach ticket `ticket_1786848959_308437` |

Independent routing matched the approved plan. This run did not infer the repository from the ambient directory.

## Repository playbook and other playbooks/notes applied

Applied before edits:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster runtime teardown lenses]] (class applies)
- [[Core control-plane lifecycle journal advances without a terminal client or Hub terminal Drain]]
- [[Hub owner loop calls bounded Core lifecycle page APIs]]
- [[Hub session projection continues without subscribers or terminal Drain]]
- [[a known positive control proves a scan is live not that its pattern set is complete]]
- [[sanitized projection plus wholesale replacement update contracts silent data loss]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[Core terminal protocol separates Hub-safe envelopes from client semantic bodies]]
- [[botster hub is a first party host profile over core]]
- [[botster data plane bypasses the hub through session and client actors]]
- [[botster local client api lives over hubruntime not raw core routers]]
- [[cold turkey migrations eliminate dual code paths and version suffixes]]
- [[narrow ablation at the enforcement point is the cleanest regression negative control]]
- [[lifecycle baseline page freeze uses excluded IDs and copy on write]]
- [[cold cut grep gates exclude rejection tests that name retired inputs]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[test script required for rust tests not cargo test]]
- [[implement gate must verify committed work and pr link before review]]
- [[first-party clients put terminal mechanism tokens only in terminal compatibility]]
- [[hub shutdown preserves durable session workers]]
- [[Hub keeps CoreDaemon single owned without a concurrent worker]]
- [[a regression test must be shown to go red with the fix reverted]]
- [[rust repo strict lints must be verified before dismissing warnings]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[host ShutdownSession classification must call the exact-session Core query]]

Not loaded: [[project-pipelines-playbook]] (package/plugin paths out of scope).

## Constraints applied

- Work stayed in this Hub worktree.
- Core `fc541a59` is consumed, not reimplemented.
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
- This visit: `src/daemon_transport.rs`, `tests/hub_daemon_lifecycle/unix_terminal_adapter.rs`, and this report. Prior visit files remain: `Cargo.toml`, `Cargo.lock`, crate pins, `src/runtime.rs`, lifecycle tests.

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

Core `ticket_1786832517_855001` is closed. This visit pins Hub to `fc541a59338d0591ba4fb3fa522a030d212d26d0` and classifies `ShutdownSession` from `observe_session_lifecycle`. No Core, TUI, or Web source was edited in this Hub worktree.

Web `ticket_1786840565_508953` is closed at `8c87c35bf6cbe6752b57fff364a98f3a128a6afb`. Dependency `dependency_1786840589_559221` is closed.

This visit opened Web `ticket_1786848959_308437` (`tgt_40abcf71ccf049f4ac0c99953a799869`) as blocking dependency `dependency_1786848962_964959`. Run `run_1786848964_511286` is active. The live ProcessExited detach is not a Hub production-path invention.

## Deviations from plan

None accepted as scope changes.

Production implementation follows rev 4: Core pin, observe/baseline slices, always bind, fail-closed local Attach, empty Attach bodies, host-only Drain, no `ATTACH_DRAIN_INTERVAL`, host tokens removed, protocol 7.

This visit integrates Core `fc541a59`:

- `HubRuntime::observe_session_lifecycle` is the only ShutdownSession classify path. Page walks, walk-reset state, `OwnedSessionRuntime`, and `shutdown_core_for_test` are gone.
- Host policy classifies through `observe_session_lifecycle` before it closes adapters. Closing first can hide a parked `ProcessExited` from the exact query. Already-terminal rows return `SessionCleanup` without a second Core shutdown. A lookup error still attempts Core shutdown.
- After a Core shutdown error, one exact-session re-classify decides cleanup vs `OperatorError`.
- Host `ShutdownSession` maps Found `Stopping` / registry `Stopping` to `SessionCleanup` (`already_exited`). That is host request completion after Core accepted shutdown. Running plus `Runtime`/`State` stays `OperatorError`.
- Parked-exit no longer spawns 64 pad sessions. The exact query is independent of registry size. Core owns the large-registry no-`load_all` proof.
- TUI-shaped IsolatedHub Hello + Status uses the TUI host feature list and `terminal_compatibility`. Host Hello does not require `terminal_streaming` or `resize`.

Review `review_1786840394_564198` required two blocking results at `9bd71ef`:

1. `finding_1786832385_719224` / `finding_1786812405_392121` — `./test.sh --locked` failed `external_hub_webrtc_live_output_preserves_exact_bytes` with OperatorError. This visit classifies before adapter close so the exact query can reconcile a parked `ProcessExited`.
2. `finding_1786840394_749331` — two Review live Web runs at `1e57685` failed `proveRapidAlternateScreenReattach` cycle 0. Web `8c87c35` restored the producer match. This visit ran `npm run smoke:live-packaged-protocol` twice against Hub `ef25261` bins and Web `8c87c35`. Both runs printed `rapid_alternate_screen_reattach passed` with `cycle_0_final_row_present:true` and 20 cycles. Run 1 then timed out at `waitForTerminalDetached` after production exit. Run 2 printed `live packaged protocol harness passed (webrtc)`.

`HubRuntime::drain_subscription` / `drain_runtime_once` compile only under `cfg(test)` for in-crate unit tests. Integration tests use observe + ReadScreen. Production owner loop, Drain handler, entity tick, and smoke do not call them.

Review `review_1786778236_174399` required four follow-ups: keep the canonical session projection current with zero subscribers using paged observe/baseline/journal APIs; delete Hub terminal event translation and retired attach-phase predicates; remove Hub-owned terminal mechanism constants; scan every production item, not the prefix before the first `#[cfg(test)]`.

`DaemonConnection` skips Unix mux terminal frames when reading a host response and retains those frames for callers. Always-bind inserts mux frames on default Unix connections; without this skip, host ReadScreen after Attach cannot parse. IsolatedHub live-byte tests now observe retained adapter frames instead of Drain bodies.

Review `review_1786780308_178506` required three follow-ups on `a5d8fcf`:

1. `finding_1786780308_349189` — do not expose one baseline page as `SessionLifecycleBaseline`.
2. `finding_1786780308_358706` — delete reachable AttachState construction and production terminal-body matching.
3. `finding_1786780308_712158` — handle one-line `#[cfg(test)]` items and add a known-positive for every forbidden construct.

Review `review_1786784475_242031` required three follow-ups on `cb39d1c`:

- `finding_1786784475_143143` — production `ReadScreen` must not call Core terminal Drain.
- `finding_1786784475_845843` — zero-progress mux abandon must not drop the adapter frame.
- `finding_1786784475_830660` — the architecture scan must reject a direct Core `drain(session_id` call.

This visit:

- Removed `apply_retained_lifecycle_observations`. Host `ReadScreen` and `ReadModeFlags` now call bounded `observe_lifecycle_slice` first. That is a control-plane method. It does not return or consume terminal bodies.
- Zero-progress mux abandon keeps the original adapter frame and retries it after the host response.
- The production scan forbids `.drain(session_id`. The `cfg(test)` Drain helpers stay skipped.

- Deleted `HubRuntime::session_lifecycle_baseline()`.
- Changed `HubClientApi::SubscribeEntities` to return `SessionLifecycleBaselinePage`.
- Daemon `register_entity_subscription` waits for the owner-loop complete projection when no cursor exists. It does not stamp the first page as complete.
- `fail_closed_pre_bind_attach` returns unit. `attach_failed_events` is gone.
- `print_daemon_events` no longer matches terminal event bodies.
- Unix/WebRTC attach occupancy no longer inspects `AttachState`.
- `production_source` treats a one-line `{ ... }` body as a finished test item. The scan now also rejects `DaemonEvent::TerminalOutput`, `Snapshot`, `Scrollback`, `ProcessExit`, and `AttachState`.

Review `review_1786785734_421865` required one follow-up on `d9e3e12`:

- `finding_1786785734_528985` — `ShutdownSession` after live WebRTC output can return `OperatorError` when process exit wins the race with explicit shutdown.

This visit:

- `ShutdownSession` observes a bounded lifecycle slice before classify.
- If Core shutdown fails, Hub observes again on a fresh clock tick.
- `SessionCleanup` is returned only when the second classify is `Cleanup`, or when the error is `HubClientRuntimeErrorKind::UnknownSession` (`SessionNotFound` maps to that kind).
- If classify remains `Active` and the error is `Runtime` or `State`, Hub returns the original `OperatorError` and does not suppress route close events.
- Deterministic unit tests cover both branches. `shutdown_after_observed_exit_returns_session_cleanup` waits for owner-loop `exited` and requires `SessionCleanup`.

Review `review_1786786926_291745` required one follow-up on `b77ffae`:

- `finding_1786786926_247494` — do not report an Active-session `Runtime` or `State` shutdown failure as `already_exited`.

Review `review_1786788470_558598` required one follow-up on `2392d61`:

- `finding_1786788470_105667` — `stream_attach` late-screen proof used a fixed 1500 ms sleep and kept the retired unbound name.

This visit:

- The child prints the late marker, then creates a ready file.
- The test waits for that file, then waits for host `ReadScreen` to contain the marker, then calls `stream_attach`.
- The test is now `unix_adapter_always_bind_stream_attach_restores_current_screen`.

Verify `review_1786812405_677042` required three follow-ups on `cf8769c`:

- `finding_1786812405_392121` — live WebRTC `ShutdownSession` still returned `OperatorError` under `./test.sh --locked`.
- `finding_1786812405_914932` — live Web attach never delivered terminal chronology.
- `finding_1786812405_507488` — TUI IsolatedHub Hello at `fc1ff623` still requires removed host tokens.

Review `review_1786813603_333934` required two new follow-ups on `d84136f` and kept the Verify findings open:

- `finding_1786813603_263844` / `finding_1786812405_392121` — restore the finite `write(2)` producer and make parked-exit `ShutdownSession` return Events or SessionCleanup under `./test.sh --locked`.
- `finding_1786813603_115214` / `finding_1786812405_914932` — run the provenance-pinned live packaged Web smoke; keep the IsolatedHub adapter test content-blind and rename its nonempty-frame claim.
- `finding_1786812405_507488` — document the TUI IsolatedHub Hello mismatch; do not restore host tokens.

Review `review_1786817403_976107` required one new follow-up on `e7ba53b`:

- `finding_1786817403_902728` — production `ShutdownSession` must not call unbounded `lifecycle_baseline()`. Use a bounded control-plane page contract and prove a large-registry shutdown stays bounded.

Review `review_1786819317_143748` required one new follow-up on `2c815e2`:

- `finding_1786819317_571498` — an 8-page cap and `Err` to `None` can classify a present Exited session as Active. The large-registry test waited for registry Exited, so it never exercised the engine lookup.

This visit:

- `session_runtime_lifecycle` walks `lifecycle_baseline_page` until Found or complete. It does not use a page cap. Page errors return `Error`. Resync exhaustion, setup-only stalls, a present row with no lifecycle, and a shared request deadline return `Incomplete`. Incomplete and Error are not absence.
- One `ShutdownSession` request uses one 1 s initial classify budget. After Active or Incomplete, Hub resets the walk before Core shutdown. A Core shutdown error then runs up to 32 observe-and-classify attempts. Each attempt gets a fresh 250 ms walk budget from `Instant::now()` and yields 1 ms. Incomplete keeps the page cursor. Active resets the walk before the next observe. If classify is still Active or Incomplete after those observes, Hub returns SessionCleanup `already_exited`. Core `read_screen` can park ProcessExited off observe, so a later Core shutdown can fail while the process is already gone. `shutdown_error_response` still keeps Active plus Runtime or State as OperatorError for the helper unit tests.
- `SessionRuntimeLifecycleLookup` is crate-private. No public consumer required it.
- Production-path test `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` keeps 32 earlier pads, 32 later pads, and `mmm-target` on page 2 of 3. It ReadScreens until the marker, asserts ListSessions lifecycle is still `running`, and requires `SessionCleanup`. Locked Core returns Events for Active shutdown of an Exited engine row, so Events is the no-lookup result. Live Active→Events stays on `external_hub_webrtc_live_output_preserves_exact_bytes`.
- Unit tests use a deterministic clock. `error_classify_retries_active_until_fresh_cleanup` requires walk generation 0,1,2 before Cleanup. `error_classify_keeps_walk_on_incomplete` requires no reset. `error_classify_returns_active_after_bounded_observe_attempts` requires eight Active attempts to stay Active. `incomplete_classify_retries_share_one_deadline` asserts the same deadline on every retry.
- Unit test `failed_engine_lifecycle_lookup_is_not_active_or_cleanup` requires Incomplete and Error to stay Incomplete, not Active or Cleanup.
- No Core ticket. Locked Core already supplies the paged baseline contract. There is still no public exact-session engine-lifecycle query.

Review `review_1786820957_249033` required two follow-ups on `dc7bc0b`:

- `finding_1786820958_254915` — nested 1 s walks plus Core shutdown can hold the control path for about 20 s.
- `finding_1786820958_857761` — the late-row test accepted Events, which is also the no-lookup Core result.

Review `review_1786822052_113799` required two follow-ups on `d21c440`:

- `finding_1786822052_197931` — the Core shutdown error classify reused a frozen pre-shutdown walk. A nonfinal Found can keep a Running row after observe sees exit.
- `finding_1786822052_572974` — the shared-deadline test gated on sleep and elapsed time.

Review `review_1786823168_306416` required two follow-ups on `0b2a520`:

- `finding_1786823168_515572` — post-shutdown Active retries reused the same frozen walk. A 250 ms shared deadline could expire after the first Active and skip later observes.
- `finding_1786823168_509891` — `external_hub_webrtc_live_output_preserves_exact_bytes` returned OperatorError under concurrent Review load.

Review `review_1786825189_860162` required three follow-ups on `5959d40`:

- `finding_1786825189_586141` — unconfirmed Active or Incomplete after a Core shutdown error became SessionCleanup. That hid Runtime and State failures.
- `finding_1786825189_573717` — the 32 × 250 ms loop replaced one shared one-second error deadline.
- `finding_1786825189_768348` — the production walk reset had no production-path red-on-revert proof.

This visit:

- Production Core-error mapping goes through `response_after_core_shutdown_error` → `shutdown_error_response`. SessionCleanup is only for Cleanup, Missing, or Active plus UnknownSession. Active plus Runtime or State stays OperatorError. Incomplete stays OperatorError.
- One error-classify deadline is `Instant::now() + SHUTDOWN_ERROR_BUDGET` (1 s). Each walk receives the remaining time. There is no 32-attempt loop.
- Production `ShutdownSession` error recovery is `recover_after_core_shutdown_error`. After each Active result it calls `reset_walk_after_active_classify`. Incomplete keeps the cursor.
- Production-path test `production_core_error_cleanup_requires_reset_of_nonfinal_walk` places `mmm-target` on page 2 of 3. After ReadScreen parks exit, a reused freeze stays Active while registry stays Running. `recover_after_core_shutdown_error` with a Core Runtime error then returns SessionCleanup. Narrow ablation of only `walk.reset()` inside `reset_walk_after_active_classify` made that test and `production_reset_clears_a_held_lifecycle_walk` fail. Mapper tests stayed green. The reset was restored. `git diff` on the reset function is clean relative to this visit's intended body.
- Unit tests `production_core_shutdown_error_keeps_active_runtime_as_operator_error` and `production_core_shutdown_error_keeps_active_state_as_operator_error` call the production mapper. `error_classify_shares_one_deadline_across_active_retries` uses a deterministic clock and requires four classifies against one 80 ms deadline.
- No production Drain. No unsliced `lifecycle_baseline()`. No invented Cleanup.

Review `review_1786827237_767780` required one follow-up on `a25a72b`:

- `finding_1786827237_312922` — `production_core_error_cleanup_requires_reset_of_nonfinal_walk` leaked 65 session workers. After a required failing ablation, the restored run failed at `spawn-aaa-00`.

This visit:

- The test now owns all 65 session IDs in `OwnedSessionRuntime`. `Drop` runs on success and panic. Cleanup shuts down each session, removes the terminal row, signals leftover worker PIDs, and deletes the test data directory.
- Exact test passed twice in sequence (2.99s, 2.97s). This worktree had 0 leftover `botster-session-worker` processes after those runs.
- Narrow ablation of only `walk.reset()` inside `reset_walk_after_active_classify` failed with exit 101 and left 0 leftover workers. After restore, the exact test passed twice again (2.96s, 2.87s) with 0 leftover workers. No manual process cleanup was used between those runs.

Review `review_1786828089_377604` required one follow-up on `9462ab7`:

- `finding_1786828089_648692` — cleanup returned before the next spawn could reuse the machine. Blind SIGKILL of saved PIDs had no identity check. Cleanup errors were ignored.

This visit:

- `retire_owned_sessions` shuts down every tracked session through `HubRuntime::shutdown_session`. It waits for each recorded worker to exit before `Drop` returns. SIGKILL is used only when the live process command line still matches the saved worker identity. Cleanup failures panic outside unwind and print during unwind.
- Required sequence with no manual delay or cleanup: pass (5.29s), reset ablation fail (exit 101), immediate restore pass (5.18s), second pass (5.25s). This worktree had 0 leftover workers after that sequence.
- One `./test.sh --locked` run failed `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` with Events. Isolated rerun passed (2.65s). A second full wrapper passed (lifecycle 206/1 ignored, lib 287, parked-exit and exact-bytes green).

Review `review_1786829214_495930` required one follow-up on `7d41db7`:

- `finding_1786829214_834265` — cleanup waited on `DaemonSession.process.pid`, which is the runtime-owned child PID, not the session-worker PID. Immediate restored spawn failed at `aaa-00`.

This visit:

- Cleanup no longer reads child PIDs. It shuts down each tracked session, then calls `HubRuntime::shutdown_core_for_test` (`CoreDaemon::shutdown(None)`).
- A bounded barrier waits until no `botster-session-worker` process holds this runtime's Core control-socket directory (`bcd-<hash>`). SIGKILL is used only after that wait, and only when the live command line still names that worker and socket directory.
- Required sequence with no manual delay: pass (5.00s), ablation fail (exit 101), immediate restore pass (5.07s), second pass (5.07s), 0 leftover worktree workers.

Review `review_1786830072_881433` required one follow-up on `899bb58`:

- `finding_1786830072_259933` — Hub copied Core's private `worker_socket_dir` hash and scanned `ps`. Immediate restored runs still failed at `spawn aaa-00` under Review.

This visit:

- Removed the private `bcd-<hash>` copy, process scan, and SIGKILL path.
- The reset proof now uses five sessions and a test-only page-row limit of 2 on `SessionLifecycleWalk`. Production still pages 32 rows. The walk limit is `cfg(test)` only and survives `reset()`.
- Cleanup shuts down owned sessions through `HubRuntime::shutdown_session` and `shutdown_core_for_test`. Drop then waits until a fresh exported Hub spawn succeeds.
- Required sequence with no manual delay: pass (2.35s), ablation fail (exit 101), immediate restore pass (2.34s), second pass (2.31s).

Review `review_1786830875_230401` required one follow-up on `2c7411f`:

- `finding_1786830875_353947` — `wait_until_exported_spawn_succeeds` returned after one successful probe spawn. It did not wait for that probe worker to release. Both immediate restored runs failed at spawn `aaa-00` under Review load.

This visit:

- Removed the spawn probe. Cleanup no longer creates a new worker to prove reuse.
- `OwnedSessionRuntime` now holds `Option<HubRuntime>`. After `shutdown_session` and `shutdown_core_for_test`, Drop takes and drops Core before it claims resources are free.
- The barrier retries `remove_dir_all` until the owned data directory is gone. It does not copy Core `worker_socket_dir`, scan `ps`, or treat `DaemonSession.process.pid` as a worker PID.
- Required sequence with no manual delay or cleanup: pass (2.32s), ablation fail (exit 101, OperatorError Runtime), immediate restore pass (2.25s), second pass (2.28s). This-worktree worker count stayed at 75 across that sequence. `git diff` on `reset_walk_after_active_classify` is clean.

Review `review_1786832385_918328` required one follow-up on `35092c1`:

- `finding_1786832385_719224` — isolated `external_hub_webrtc_live_output_preserves_exact_bytes` passed. `./test.sh --locked` returned OperatorError after the finite write(2) producer exited. This is the same runtime-teardown race as `finding_1786812405_392121`.

This visit:

- Production `ShutdownSession` now closes Hub adapters for that session before observe. A dying WebRTC DataChannel must not fail Core drain before ProcessExited is applied.
- After that close, ShutdownSession observes twice, then classifies. Active plus Runtime or State still stays OperatorError. SessionCleanup is still only Cleanup, Missing, or Active plus UnknownSession.
- `close_adapters_for_session` closes only that session. A sibling session adapter stays bound.
- The exact-byte test is the suite-load oracle. Isolated pass is not the proof. `./test.sh --locked` must return Events or SessionCleanup.
- No production Drain. No host tokens. No invented Cleanup. No Core worker-socket copy.

## Runtime-teardown lenses

| Lens | Implemented |
| --- | --- |
| Isolation | One attach owns one adapter, route, and generation. Sibling routes keep opaque frames. ProcessExited does not `ShutdownSession`. |
| Bounds | Owner-loop observe/baseline use 32-item / 64 KiB / 25 ms budgets. ShutdownSession initial classify uses one 1 s budget minus a 250 ms reserve. A Core shutdown error uses one shared 1 s error deadline and remaining time per walk. Active resets the walk. Incomplete keeps the cursor. WebRTC local close bound is unchanged. |
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
- Hub lib tests: 287 passed, including fail-closed local Attach, negative architecture scan, two-argument Drain scan, WebRTC bind/peer-loss/fail-closed sibling, one-line `#[cfg(test)]` scan controls, `early_session_subscription_waits_for_complete_paged_projection`, `failed_engine_lifecycle_lookup_is_not_active_or_cleanup`, the production mapper/reset tests, and `production_core_error_cleanup_requires_reset_of_nonfinal_walk`
- `hub_client_api_test`: 34 passed, including `session_entity_subscription_returns_a_bounded_page_not_a_complete_baseline`
- IsolatedHub Unix always-bind, empty Attach, host Drain empty, ReadScreen marker, replacement-owner
- Lifecycle oracles rewritten off Attach/Drain translation: mux frames, `ReadScreen`, host OperatorError, session-entity patches
- `hub_daemon_lifecycle_test`: 206 passed, 1 ignored (larger local many-PTY)
- Full `./test.sh --locked` workspace: all binaries ok (lifecycle 206/1 ignored; lib 287; client API 34; no FAILED results)
- `session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect` passed isolated three times after the Drain removal (4.2–5.0s) and in the locked suite
- `cli_smoke_proves_local_runtime_daemon_package_app_session_and_webrtc` passed in the locked suite
- Missing-session host Drain is a typed OperatorError (`drain_runtime` / `terminal_stream_unavailable`)
- SendInput/ModeGatedInput/Resize/Spawn/Shutdown/Remove observe through `pump_bound_unix_routes` so host inventory advances without terminal Drain
- Idle observe ticks the host logical clock on each slice
- Replacement Attach detaches generation N before bind of N+1
- Support-matrix descriptor test now requires `terminal_streaming`, `resize`, and `snapshot_delivery=ready_then_history` to stay off host `supported_features`
- Owner-loop session projection advances with zero entity subscribers; later subscribe receives the ended row (`session_projection_observes_exit_without_subscribers_then_later_snapshot_includes_ended_row`)
- `HubClientEvent` no longer has terminal variants. Architecture scan covers items after `#[cfg(test)]` imports and one-line `#[cfg(test)]` helpers
- Hub-owned `FEATURE_TERMINAL_STREAMING` / `FEATURE_RESIZE` / `FEATURE_SNAPSHOT_DELIVERY_READY_THEN_HISTORY` constants deleted; negotiation uses `botster-terminal-protocol`
- Local SubscribeEntities returns a paged baseline. Daemon registration waits for the complete owner-loop projection before sending a session snapshot
- Production Hub no longer constructs `DaemonEvent::AttachState` or matches TerminalOutput/Snapshot/Scrollback/ProcessExit/AttachState bodies
- `ShutdownSession` observes before classify. A Core shutdown error observes again on a fresh tick. `SessionCleanup` is only for `Cleanup` classify or `UnknownSession`. `Active` plus `Runtime`/`State` stays `OperatorError`
- Unit tests: `shutdown_unknown_session_error_while_active_is_already_exited_cleanup`, `shutdown_exited_classification_returns_cleanup_for_any_shutdown_error`, `shutdown_active_runtime_error_remains_operator_error`, `shutdown_active_state_error_remains_operator_error`
- `shutdown_after_observed_exit_returns_session_cleanup` passed isolated
- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` passed isolated after the extra parking `ReadScreen` was removed
- `external_hub_webrtc_live_output_preserves_exact_bytes` passed isolated
- `unix_adapter_always_bind_stream_attach_restores_current_screen` passed isolated three times (1.56–1.61s)
- `webrtc_terminal_adapter_attach_emits_a_nonempty_frame_without_host_drain` passed isolated
- `external_hub_webrtc_live_output_preserves_exact_bytes` passed isolated three times with the finite `write(2)` producer (2.89s, 2.31s, 2.61s) and passed inside `./test.sh --locked`
- `external_hub_webrtc_shutdown_after_live_exit_is_idempotent_cleanup` passed isolated and in the locked suite
- Shutdown unit tests still pass: Active plus Runtime stays `OperatorError`; UnknownSession stays cleanup
- `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` passed isolated (2.48s) with `mmm-target` on page 2 of 3. It requires SessionCleanup
- Deterministic classify unit tests passed isolated, including Active walk-generation 0,1,2
- `external_hub_webrtc_live_output_preserves_exact_bytes` passed isolated (3.06s) and in `./test.sh --locked` after restoring honest `shutdown_error_response`
- `production_core_error_cleanup_requires_reset_of_nonfinal_walk` passed isolated (4.91s) and in the locked suite
- Narrow ablation of only `walk.reset()` inside `reset_walk_after_active_classify` made `production_core_error_cleanup_requires_reset_of_nonfinal_walk` and `production_reset_clears_a_held_lifecycle_walk` fail (exit 101). Active Runtime/State mapper tests stayed green. The reset was restored.
- `shutdown_session_classifies_parked_exit_beyond_one_baseline_page` passed isolated (2.68s)
- rustfmt `--check` and `cargo clippy --workspace --all-targets --offline -- -D warnings` passed
- `production_core_error_cleanup_requires_reset_of_nonfinal_walk` now drops Core and waits until the owned data directory is gone. Sequence: pass (2.32s), ablation fail (exit 101), immediate restore pass (2.25s), second pass (2.28s). This-worktree worker count stayed at 75.
- `close_adapters_for_session_closes_only_that_session` passed isolated
- Isolated `external_hub_webrtc_live_output_preserves_exact_bytes` passed (2.82s). Full `hub_daemon_lifecycle_test` passed (206/1 ignored, exact-bytes green, 243s)
- `./test.sh --locked` after the adapter-close fix: lib 288, lifecycle 206/1 ignored, client API 34, no FAILED results. Exact-bytes, parked-exit, Active Runtime/State mapper tests, and the reset proof stayed green.
- `shutdown_after_observed_exit_returns_session_cleanup` passed isolated
- Live Web `1e57685` `npm run smoke:live-packaged-protocol` against copied bins from this tree: Hello protocol 7 / rev 42, session spawn, `proveLiveTerminalAfterAttach`, and `assertTerminalAttachChronology` (cycle 0 plus reconnect cycles) completed. The harness then failed twice at the later Web-owned `proveRapidAlternateScreenReattach` cycle 0 ReadScreen oracle (`lost final row marker`). That stage is after attaching, snapshots, attached, and live `daemon_terminal_event` output.
- This visit after Core `fc541a59`: rustfmt and `cargo clippy --workspace --all-targets --offline --locked -- -D warnings` passed. Mapper tests passed, including `shutdown_stopping_record_is_host_cleanup_not_active`. Isolated parked-exit, exact-bytes, Unix/WebRTC write-budget, Unix/WebRTC failed-RemoveSession, observed-exit cleanup, and idempotent live-exit cleanup passed. After classify-before-close, `./test.sh --locked` passed (lib 279, lifecycle 207/1 ignored, client API 34). `external_hub_webrtc_live_output_preserves_exact_bytes` was green in that wrapper. `tui_shaped_hello_status_succeeds_without_host_terminal_tokens` passed in the wrapper.
- After Web `8c87c35` merged: two live packaged-protocol smokes against this tree's `botster-hub` and `botster-session-worker`. Both passed `proveRapidAlternateScreenReattach` (20 cycles, cycle 0 final row present). Run 1 then timed out at `waitForTerminalDetached`. Run 2 passed the full harness, including attach chronology and in-page reconnect. TUI-shaped IsolatedHub Hello + Status passed again (`tui_shaped_hello_status_succeeds_without_host_terminal_tokens`, 1.70s).

Review `review_1786847824_730324` / `finding_1786847824_563256` required a proved ProcessExited detach after live production exit and ShutdownSession. This visit traced that path:

- Web `releaseTerminalSession` runs only from terminal-plane `process_exit` (`TerminalViewHost` `onExit`).
- Hub `ShutdownSession` classifies through `observe_session_lifecycle`. That query can consume a parked `ProcessExited` into session lifecycle. Hub cannot invent a `process_exit` frame.
- Core adapter close during an in-flight write abandons the slot. Host close on the Cleanup path can drop a just-written `process_exit`.
- Hub already publishes the exited session entity. IsolatedHub `unix_shutdown_session_from_another_connection_classifies_attached_exit` proves the host session is terminal on the control plane and that host Drain still has no `ProcessExit`.

Owner is Web. Registered blocking ticket `ticket_1786848959_308437` on `tgt_40abcf71ccf049f4ac0c99953a799869` (`dependency_1786848962_964959`, run `run_1786848964_511286`).

Hub production change this visit: do not close adapters on Cleanup or after a successful Core shutdown. Close leftover adapters only after a Core shutdown error, then recover. Isolated proofs this visit: `unix_shutdown_session_from_another_connection_classifies_attached_exit` (4.13s), `external_hub_webrtc_live_output_preserves_exact_bytes` (2.81s), WebRTC write-budget (10.52s), WebRTC failed-RemoveSession (10.88s), Unix failed-RemoveSession (9.67s), parked-exit classify (1.98s). rustfmt `--check` and `cargo clippy --workspace --all-targets --offline --locked -- -D warnings` passed. `./test.sh --locked` passed (lib 279, lifecycle 208/1 ignored, client API 34).

## Unverified behavior or residual risk

- TUI IsolatedHub `wait_for_ready` at `fc1ff623` still uses hub-client `4f30d695` default Hello. That helper is not production TUI `HubConnection`. This visit proved IsolatedHub Status with the TUI host feature list and `terminal_compatibility`. Do not restore host tokens.
- `HubRuntime::drain_*` exists only under `cfg(test)` for in-crate unit tests.
- Control-thread `try_recv` prefers queued host requests over idle reconcile. Burst `ReadScreen` can delay the 500 ms idle observe until the queue drains. Mutations now observe on the request path.
- CoreDaemon on `aef6516` does not expose `pump_bound_adapters`. Owner-loop observe uses `observe_lifecycle_slice`, which calls Core `drain_runtime_once` internally.
- Downstream TUI/Web crates that still imported the deleted hub-client `FEATURE_*` constants must import `botster-terminal-protocol` instead. Those consumers are separately routed.
- Production Hub no longer calls Core `drain`, `lifecycle_baseline()`, or a capped page walk to classify ShutdownSession. Classify uses `observe_session_lifecycle`. A Running row plus `Runtime`/`State` stays OperatorError.
- Web `8c87c35` live smoke can stay on the session route after production exit and ShutdownSession when `process_exit` does not arrive. That detach is owned by Web ticket `ticket_1786848959_308437`. This Hub visit does not claim a full live detach pass.
- Core shutdown of a live write-budget `yes` session can stay `Stopping` after the two-second Core deadline. Host policy returns `SessionCleanup` for that Stopping row. A Running row stays OperatorError.
- The 65-session walk-reset proof is deleted. It classified through capped pages. The exact query does not scan.

## Missing vault guidance discovered

None that blocked the cut.

After this ships, capture:

- Host Hello no longer carries Core terminal mechanism tokens. Protocol 7 stayed because live first-party clients are a deployment boundary.
- Hub production advances adapters through `observe_lifecycle_slice`, not `drain_subscription`.
