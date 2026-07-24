# Eliminate stale-daemon recovery UI proxy readiness race

Ticket: `ticket_1784168175_561269`
Run: `run_1784234857_352973`
Returned Plan step: `run_step_1784237920_358954`

## Context loaded

- Project Pipelines current context: both prior plan artifacts, failed Implement report `artifact_1784236909_385350`, approved reviews `review_1784236401_958678` and `review_1784237692_485524`, the latter review's four findings, human answers `question_1784236756_337513`, `question_1784238031_807799`, `question_1784238940_476643`, and `question_1784242208_662992`, and the returned Plan step.
- Required planning authority: `[[planner-playbook]]`, `[[botster-planner-playbook]]`, `[[botster-architecture]]`, `[[cli-patterns]]`, `[[spa-patterns]]`, and `[[prefer framework and library components over custom solutions]]`.
- Socket/lifecycle authority: `[[botster hub socket cleanup must preserve connectable sockets and repair missing socket paths]]`, `[[botster hub socket liveness requires a protocol handshake]]`, `[[daemon probe order changes require lifecycle integration tests]]`, `[[botster runtime artifact resolution should be read only]]`, and `[[port patterns on touch do not bulk retrofit existing sites]]`.
- Merged constraint: ticket `ticket_1784168174_908058`, PR #140, commit `01549e9`, and `docs/plans/eliminate-loaded-local runtime-restart-teardown-race.md`. Its safety premise remains binding: an exiting daemon must not reclaim a pathname removed for replacement or unlink a replacement-owned socket.
- Failed implementation evidence: protocol-derived fixture health passed its deterministic stale-file test and turned red when reverted to cached `existsSync`, but the named production path then failed after 11.83 seconds with `WebHealth`, `daemonReady=false`, and `connect ENOENT`. The fixture change was fully reverted. This proves the stale/missing public pathname is permanent for that run; health-only hardening exposes the production defect but does not repair recovery.
- Human decision: failed `up` is not acceptable. Re-plan this ticket for listener-ownership-safe daemon socket repair. The initial answer proposed bind-time identity and identity-checked cleanup, required replacement startup to remove only a confirmed stale/incompatible pathname and bind once, and made the retiring-daemon ownership invariant mandatory. Keep protocol-derived bridge health and all no-timing-workaround requirements.
- Human clarification after Plan Review: first instrument the deterministic failure to identify the actual pathname remover/rebinder. The production recovery fix must make the named incompatible-daemon test red when reverted. A cleanup ownership guard may ship even when that named test stays green only if a forced old-listener/new-listener interleaving test is red without the guard and green with it. Drop every mechanism that lacks its own deterministic negative control or duplicates an invariant already enforced by the transport.
- Implement actor trace: the incompatible daemon bound inode A; recovery waited for that daemon to exit and removed A; the replacement bound inode B; no later cleanup, removal, or rebind acted on B. The apparent permanent recovery failure was instead the generated fixture expanding a valid 94-byte relative socket path to an approximately 200-byte absolute Node connection path beyond the macOS Unix-domain socket limit. Human answer `question_1784238940_476643` therefore directs a short-path fixture correction plus protocol-derived health/proxy assertions and no daemon transport change.
- Loaded acceptance reconciliation: after merging current `origin/main`, exact-SHA run `29541755699` stopped at repetition 1 with 95 passed, 1 failed, and 1 ignored. Both ticket-owned stale-daemon tests and the merged delayed-listen readiness test passed. The sole unrelated signature, `local WebRTC data channel closed before response`, remains mapped to reopened owner `ticket_1784168176_163113`; the previously observed stalled-attach failure passed and was removed from this leaf's mapping. Human answer `question_1784242208_662992` moves 20-suite-green convergence to the final umbrella run after sibling integration; this leaf must preserve exact first-red and cleanup evidence and may not describe the run as suite-green.

Botster layers: Rust hub daemon transport plus Rust lifecycle integration fixture. Target `tgt_7e208a0c76a44980a83b63af976b1f22` and the assigned ticket worktree remain binding.

## Scope

1. Instrument the now-deterministic incompatible-daemon recovery failure before choosing production mechanics. Trace or assert every configured-socket pathname removal, bind, rebind, and listener identity transition across `recover_owned_stale_local_runtime_daemon`, `prepare_socket_path`, `serve_daemon`, and cleanup; record the actor and ordering in the Implement artifact, then remove temporary instrumentation that does not belong in focused tests.
2. Implement only the smallest production recovery mechanism that corrects the identified actor. Its isolated revert must make `cli_local_runtime_up_recovers_owned_incompatible_daemon` fail for the original permanent-pathname/proxy reason. Active missing-path repair is a candidate, not a preselected answer: keep it only if instrumentation names missing-path loss while a listener remains active and reverting repair makes the named path red.
3. Preserve the PR #140 ownership invariant independently. First determine whether current lifecycle ordering already prevents an old listener from unlinking a replacement. Add listener ownership identity and guarded cleanup only if a deterministic forced old-listener/new-listener interleaving is red without it and green with it. The test must force the replacement swap inside the cleanup decision window, not rely on scheduler luck. If compare-then-unlink cannot survive that forced window, use a stronger repository/toolchain primitive that arbitrates the entire decision-and-remove operation; stop and ask rather than claim timing safety.
4. Drop any candidate mechanism whose correct focused negative control remains green on isolated revert, or whose invariant is already enforced by an existing mechanism. Do not retain identity wrappers, repair helpers, diagnostics, or test seams defensively.
5. Preserve retirement and startup ordering mechanically for whatever mechanism remains: process control/shutdown messages before any repair; once shutdown is observed, return without repair; keep `prepare_socket_path`'s hello/ack admission and single confirmed-stale removal/bind; never delete a competing socket after a failed bind.
6. Reapply the validated embedded `write_botster_web_package` health change: `/health.ok` requires a fresh bridge-to-daemon hello/ack plus side-effect-free `Status`, not cached pathname existence. Keep the deterministic stale-file negative regression and make the named `up` test prove packaged health plus real proxy traffic.
7. Add only the focused transport tests required by the mechanism decision: the named-path recovery negative control for the actual fix, plus forced ownership interleaving coverage if a new cleanup guard ships. Existing ownership behavior may be characterized without adding unused production machinery.
8. Verify immediate incompatible-daemon replacement, PR #140 restart paths, default-parallel lifecycle coverage, mechanism-specific deterministic red-on-revert, and this leaf's tests under the exact-SHA loaded residual-tail campaign with unchanged timeouts/concurrency. Preserve and map every unrelated first-red signature; full 20-suite-green convergence belongs to `ticket_1784087788_242994` after sibling integration.

## Non-scope

- No PID/socket completion wait, fixed sleep, retry loop, timeout increase, `--test-threads=1`, new global test lock, reduced synthetic load, or acceptance of 502/WebHealth failure.
- No unconditional rebind/unlink, `prepare_socket_path` reuse from the repair path, pathname-existence readiness, or bare-connect readiness.
- No shipping both repair candidates merely because the remover was unknown before instrumentation. No production tracing hook, optional ownership mode, compatibility path, or speculative safety layer survives after the mechanism decision.
- No public daemon/client DTO, protocol version, compatibility feature, metadata-file format, stale PID ownership policy, session transport, Lua/TUI/MCP/Rails, dependency, or broad transport refactor.
- No change to the health-only rejection fixture `write_health_only_botster_web_package`. It has no daemon protocol machinery and is used only to prove that a health-only entrypoint is rejected; `[[port patterns on touch do not bulk retrofit existing sites]]` keeps it untouched.
- No botster-web product/React feature work. The embedded package fixture is changed because it is the package launched by the real `botster-hub up` integration path.

## Assumptions and unknowns

### Assumptions

- Human answer `question_1784236756_337513` supersedes the prior plan's daemon-transport non-scope; clarification `question_1784238031_807799` governs which concrete mechanism may ship while preserving that invariant.
- The deterministic focused failure is sufficient to identify the pathname actor with scoped instrumentation; the plan no longer treats the remover as unknowable.
- If a listener identity guard is necessary, Unix device/inode identity or an equivalent generation token can identify ownership, but identity observation alone does not make compare-then-unlink atomic.
- The daemon accept/control loop remains a retirement barrier: it drains shutdown control messages before considering any repair and returns immediately after shutdown cleanup.
- If active missing-path repair proves necessary, binding an absent pathname is the admission operation. If another owner wins, preserve it and surface the existing bounded diagnostic; do not retry or clean up the winner.
- Protocol-derived health remains necessary after transport repair because socket presence still is not readiness.
- `Status` is the smallest side-effect-free bridge readiness request.

### Unknowns

- The actual remover/rebinder is unknown at Plan time but must be named from the deterministic repro before the production mechanism is finalized.
- Whether a new ownership guard is needed is intentionally conditional on the forced-interleaving negative control and proof that current ordering does not already enforce the invariant.
- If active repair is the named-path fix, the best existing diagnostic sink for a failed bind must be confirmed during implementation. Do not add a public diagnostic contract solely for this ticket.

No further human question blocks the plan. Ask again if the implementation requires weakening identity comparison, retrying startup, changing public contracts, or accepting failed recovery.

## Affected surfaces/files

- `src/daemon_transport.rs` — primary investigation and conditional production surface: instrument pathname actors; implement only the actor-driven recovery fix; add an ownership guard only if its forced-interleaving negative control proves necessary; preserve retirement/startup ordering.
- `tests/hub_daemon_lifecycle_test.rs` — protocol-derived embedded bridge health, deterministic stale-file health test, immediate incompatible-daemon replacement/UI proxy proof, and ownership-safe lifecycle assertions using existing helpers.
- `src/main.rs` — investigation and wiring surface. Temporary scoped instrumentation may identify `recover_owned_stale_local_runtime_daemon` as the pathname actor; retain a production edit here only if the trace and named-test revert prove it is the actual fix. The production Web launch consumes structured `local_url`, then verifies `/health` and UI before `local_runtime_up` prints readiness.
- `crates/botster-hub-client/src/lib.rs` — unchanged PR #140 disconnect normalization; run its existing tests as regression coverage.
- `docs/plans/eliminate-loaded-local runtime-restart-teardown-race.md` — unchanged merged constraint source.
- `script/run-loaded-daemon-lifecycle`, `.github/workflows/loaded-daemon-lifecycle.yml`, and `docs/loaded-daemon-lifecycle-runner.md` — unchanged acceptance path.

Every production line must implement the identified recovery fix, a separately proven ownership invariant, or ordering made necessary by those changes. Every fixture/test line must prove protocol readiness, mechanism necessity, deterministic ownership safety, or the named user path.

## Implementation plan

1. Add temporary, test-scoped observation at every candidate pathname mutation and bind. Run the deterministic named recovery test and produce an ordered actor trace containing process/listener identity, operation, pathname identity before/after, and call site. Remove broad logging after the actor is known; retain only narrow assertions or seams needed by deterministic tests.
2. Use that trace to choose one recovery fix. If the active daemon loses its pathname and remains the required owner, replace the no-op with a single direct absent-path bind/swap that never invokes stale cleanup. If a different actor is responsible, fix that actor instead. Immediately prove necessity by reverting only this fix and requiring the named recovery test to reproduce the original permanent ENOENT/proxy failure.
3. Characterize the PR #140 ownership invariant separately. Build a deterministic test that pauses the cleanup decision, replaces the old pathname with a new listener specifically inside the decision/remove window, resumes cleanup, and asserts the replacement survives and accepts a protocol connection.
4. If current code fails that focused test, implement the smallest ownership guard that makes it green. A stat-then-unlink helper is insufficient if the forced swap can occur after stat; the selected repository/toolchain primitive must arbitrate the complete cleanup transaction against Botster replacement bind/removal. If no such primitive is available without a new public contract, dependency, or timing assumption, stop and ask. Revert only the guard and require the forced-interleaving test to turn red. If current code already passes or the guard's revert stays green, ship no new cleanup machinery.
5. Remove every losing candidate and temporary diagnostic. Inspect the diff to ensure no unused identity wrapper, no inactive repair helper, and no test-only synchronization surface leaks into production APIs.
6. Keep `prepare_socket_path`'s protocol admission policy and PR #140 disconnect handling unchanged except where the actor trace proves a directly necessary correction. Do not add a loop or change probe ordering.
7. Restore the Implement-validated fixture change in `write_botster_web_package`: each `/health` request completes hello/ack + `Status`, closes its one-shot connection, and derives `ok` from current protocol evidence plus ownership mode. Restore `botster_web_health_rejects_stale_daemon_socket_file` and its non-timing listening marker/launch-result synchronization.
8. Strengthen `cli_local_runtime_up_recovers_owned_incompatible_daemon`: start the owned incompatible daemon, call `up` immediately with no wait, require `runtime=ready` and `daemon=started`, prove the stale child exits, complete a protocol request through the packaged proxy, and shut down cleanly. Assert the replacement socket remains present/reachable after the old child is reaped.
9. Run the PR #140 immediate restart tests unchanged to prove the selected mechanism preserves teardown/replacement behavior.
10. Produce separate red proof for each behavior that ships: cached-existence health makes the stale-file health test red, and restoring the long fixture data path makes the named `up` path red with the evidenced macOS socket-path failure. Restore all changes before commit. In loaded verification, require both ticket-owned tests to pass and preserve any unrelated first red with exact signature, run/SHA, cleanup evidence, and mapped owner; no new unmapped failure is accepted.

## Risks

- **Identity TOCTOU:** metadata comparison followed by unlink can race a replacement bind. The replacement-preservation test must force the swap after identity observation but before removal. Plain compare-then-remove is rejected if it fails that interleaving; a shipped guard must arbitrate the entire operation using a repository/toolchain primitive, not ordering luck.
- **Retiring daemon resurrection:** an exiting daemon that reaches repair can block replacement with `AlreadyRunning`. Control-message-before-repair ordering and direct return after shutdown are required and tested.
- **Competitor clobber:** repair must not run `prepare_socket_path` or remove a mismatched pathname; an atomic bind failure is preservation, not permission to clean up.
- **Listener/identity drift, if a guard ships:** swapping the listener without its identity makes later cleanup unsafe. Keep them in one private owner value.
- **Repair failure invisibility, if active repair ships:** leaving the old unlinked listener alive without a public path is degraded. Emit bounded existing diagnostics while preserving the competing path; do not spin or retry.
- **False readiness:** filesystem identity proves ownership, not protocol usability. Keep hello/ack + `Status` health.
- **Connection leaks:** health probes must close one-shot bridge connections in success and failure paths.
- **PR #140 regression:** startup probe order and disconnect classification remain unchanged; immediate restart tests are mandatory.
- **False green under load:** focused tests do not replace default-parallel loaded evidence. This leaf requires its owned tests green on the exact SHA and exact attribution of every unrelated first red. Full 20-suite-green convergence remains live under `ticket_1784087788_242994`; run `29541755699` is not suite-green.
- **Speculative dual fix:** actor uncertainty could leave both active repair and cleanup ownership code in the diff. Instrument first, apply mechanism-specific negative controls, and delete every unproved candidate.

## Acceptance checks/tests

1. Deterministic actor characterization: run the named recovery test with scoped instrumentation and attach the ordered removal/bind/rebind trace naming the actual pathname actor. Evidence must distinguish old daemon cleanup, `recover_owned_stale_local_runtime_daemon`, `prepare_socket_path`, active repair, and any external remover; "unknown" requires exact instrumentation attempted and a new stop-and-ask, not both speculative fixes.
2. Focused transport tests selected through `./test.sh --lib <exact-test> -- --exact --nocapture` and limited to shipped mechanisms:
   - the actor-driven repair restores the public path and accepts a fresh protocol handshake/request;
   - if a cleanup guard ships, a forced swap after ownership observation and before removal proves the old listener cannot remove the replacement, and matching-owner cleanup still removes its own pathname;
   - any repair preserves a path already owned by another listener.
3. Deterministic bridge negative test: `./test.sh --test hub_daemon_lifecycle_test botster_web_health_rejects_stale_daemon_socket_file -- --exact --nocapture` passes with `socketExists=true`, protocol readiness false, and `ok=false` from one synchronized health request.
4. Named user path: `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_recovers_owned_incompatible_daemon -- --exact --nocapture` passes without PID/socket wait and proves old exit, replacement protocol reachability, packaged `/health`, proxy `Status`, readiness output, socket persistence, and clean `down`.
5. PR #140 regression paths remain immediate and green:
   - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_bootstrap_reuses_live_daemon_and_preserves_state_after_restart -- --exact --nocapture`
   - `./test.sh --test hub_daemon_lifecycle_test cli_local_runtime_up_starts_reuses_and_down_stops_runtime -- --exact --nocapture`
6. Neighboring packaged launcher/bridge tests, including `removed_legacy_launcher_launcher_starts_botster_web_in_existing_hub_mode_and_shuts_down`, `removed_legacy_launcher_launcher_bridge_request_endpoint_uses_same_daemon_state`, and the unchanged health-only rejection test.
7. Default-parallel lifecycle target: `./test.sh --test hub_daemon_lifecycle_test -- --nocapture`; no `--test-threads=1` substitute.
8. Workspace checks: `./test.sh --workspace`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and `git diff --check`.
9. Mechanism-specific deterministic red-on-revert evidence as described in implementation step 10. The recovery fix is judged by the named recovery test; a cleanup guard is judged by the forced-interleaving test; bridge health is judged by the stale-file test. Record exact temporary diffs and expected failure reasons; do not commit reverted states or retain a mechanism because a different test happens to cover it.
10. Loaded acceptance: run the exact restored subject SHA through the existing runner with `test_target=lifecycle-suite`, `stress_profile=residual-tail`, 20 requested repetitions, default Cargo parallelism, unchanged budgets, first-red stop, resource samples, and clean owned-process teardown. Both ticket-owned tests must pass. Preserve every unrelated failure with its exact signature, run/SHA, cleanup evidence, and mapped owner; accept no new unmapped failure. Run `29541755699` at `5be9e00ba74cb029ba4408e663ff764e93b6cd0e` satisfies this leaf criterion but is not suite-green: the sole mapped local WebRTC failure remains live under `ticket_1784168176_163113`, while stalled-attach passed after integrating current main. The final 20-suite-green criterion belongs to `ticket_1784087788_242994` after all sibling branches are merged and rebased.
11. Runtime wiring review: prove the actual actor-driven fix is called from the production `up` path, any ownership guard arbitrates the real cleanup/replacement operations, shutdown cannot fall through to repair, losing candidates are absent, and the generated fixture is consumed by `botster-hub up` before readiness publication. Static helper presence is insufficient.

## Pipeline gates and artifacts

- This revision supersedes plan artifact `artifact_1784237255_253286` using failed Implement evidence `artifact_1784236909_385350`, approved conditional review `review_1784237692_485524`, and binding human answers `question_1784236756_337513` plus `question_1784238031_807799`.
- Implement evidence must include the actor trace, exact changed files, the decision to keep/drop each candidate with its own isolated revert result, any ownership arbitration mechanism, cleanup/rebind ordering, all focused red/restored-green proofs, named runtime path, PR #140 regressions, and confirmation that the health-only fixture is unchanged.
- Review must reject an unidentified actor without exact failed instrumentation evidence, two defensive mechanisms without separate negative controls, compare-by-path-only cleanup, racy stat-then-unlink, unconditional unlink/rebind, repair through `prepare_socket_path`, retirement fallthrough, retry/timing workarounds, leaked health connections, dead helpers, or unwired production behavior.
- Verify must include the focused health and runtime tests; workspace checks; the default-parallel lifecycle suite; and exact-SHA loaded evidence showing both ticket-owned tests green with every unrelated first red mapped and retained. Final 20-suite-green convergence is verified by `ticket_1784087788_242994`, not waived by this leaf.

## Vault gaps worth capturing

- The current note `[[botster hub socket cleanup must preserve connectable sockets and repair missing socket paths]]` has the correct desired behavior but does not name the actor in this deterministic recovery failure or the concrete arbitration mechanism that coexists with PR #140. After verified implementation, enrich it only with the actor and mechanism that survive their focused negative controls.
- Enrich `[[botster hub socket liveness requires a protocol handshake]]` after verification: ownership identity protects cleanup, while protocol evidence separately proves usability; neither substitutes for the other.
- Capture the exact stale-path remover/rebinder after deterministic instrumentation proves it; do not create a culprit-specific note from the Plan-stage hypothesis.
- No Plan-stage vault write; `capture_path: nil` until implementation and loaded verification establish the durable mechanism.
