# Adopt Core Same-Session Reattach History Fix

## Context loaded

- Pipeline ticket: `ticket_1784052230_812754`, **Hub: adopt Core same-session reattach history fix and prove daemon path**.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]], plus the Botster architecture, CLI, and SPA maps they require.
- Controlling vault context: [[coredaemon attached follows initial snapshots before live terminal output]], [[opaque terminal snapshot bytes do not prove renderable history]], [[terminal subscribe readiness gates on sessionio initial snapshot delivery]], [[initial terminal snapshots must precede live output activation]], [[hub replays full history on every attach so clients must clear per cycle]], [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]], [[conformance fixture revisions must be unique per published content]], [[a regression test must be shown to go red with the fix reverted]], and [[test script required for rust tests not cargo test]].
- Repository baseline: Hub `a9b8c637` pins Core `db69456`. Revision 12 moved the production path to Ghostty-backed Core state, while the Hub late-attach tests weakened visible-marker assertions to `data` non-empty / `bytes` positive.
- Production route inspected: daemon socket -> `daemon_transport` -> `HubClientApi` -> `HubRuntime` -> `CoreDaemon` -> worker-backed session runtime. Hub maps Core `TransportEgress::{Snapshot, Scrollback}` into public daemon history events.
- Pipeline context initially had no findings or reviews. Human answer `question_1784052486_268012` split the work: Core ticket `ticket_1784057849_665844` owns the producer and payload-fidelity fix; this Hub ticket depends on it and owns merged-coordinate adoption plus end-to-end proof.
- Baseline command `./test.sh external_daemon_attach_replays_prior_history_with_renderable_byte_count -- --nocapture` passes one test. That green result is not acceptance evidence: the test uses a second late subscriber, never drops and reconnects the daemon connection, and accepts any non-empty history payload without requiring the retained marker.

## Scope

1. Hold implementation until Core ticket `ticket_1784057849_665844` is closed with a merged producer fix and real Ghostty disconnect/reattach regression evidence.
2. Update `Cargo.lock` to the merged Core `main` coordinate. Use the normal git dependency path; do not use a sibling checkout, path override, or unmerged Core revision as final evidence.
3. Replace or reshape the current real daemon late-attach regression into the actual same-session reconnect lifecycle:
   - spawn one worker-backed session that emits a unique marker before the first attach;
   - attach and drain that marker through a persistent daemon connection;
   - send a second unique marker after the first attach and drain it;
   - drop the client connection so production connection cleanup detaches its subscription;
   - open a fresh daemon connection and attach the same session using the identifier lifecycle used by the downstream TUI reconnect path;
   - require reattach history to contain both retained markers exactly once and in order;
   - require `Attaching < renderable Snapshot/Scrollback < Attached < new live TerminalOutput` for that subscription;
   - fail on an all-NUL/opaque blank history payload instead of treating byte length or event presence as visible-history proof.
4. Keep the no-history branch in the same real daemon coverage: it must observe `Attaching -> Attached -> live` with no fabricated renderable history or `Scrollback`.
5. Tighten `docs/client-protocol.md` so public attach `Snapshot`/`Scrollback.data` is described as renderable history, while `CaptureSnapshot` remains a separate backend-opaque readback payload. Payload length alone is not evidence of visible history.
6. Compare the fixed runtime sequence with `late_attach_history_conformance_scenario()` and its checked-in npm copy. If the existing revision-12 fixture already matches the proven event shape and bytes, leave the revision and package untouched. If exported fixture content changes, allocate a globally unused conformance revision above every published meaning, regenerate every derived asset/checksum, bump the npm package version, and prove a clean external install.
7. After Hub verification, rebuild fresh `botster-hub` and `botster-session-worker` binaries from the adopted coordinate and rerun the blocking TUI live reconnect assertion. Hub remains the TUI ticket's dependency; no TUI code changes belong here.

## Non-scope

- Any change in the botster-core worktree; Core producer implementation and its local tests belong to `ticket_1784057849_665844`.
- Hub-side reconstruction, filtering, fallback hydration, fabricated last-known bytes, or duplicate terminal-state ownership.
- A TUI or browser workaround for NUL/opaque history.
- Reverting Ghostty, enabling the plain fallback, adding a compatibility mode, or retaining dual behavior.
- Protocol framing changes, response chunking, unrelated daemon cleanup, or adjacent test refactors.
- Publishing a new hub-test-support package when exported fixture content did not change.

## Assumptions and unknowns

- Determined: the current Hub projection is structurally wired; it cannot manufacture missing renderable history cleanly. The producer fix must merge in Core first.
- Determined: the existing test's `ReadScreen` marker check proves Core can read visible state, but its separate `Snapshot` predicate proves only non-empty bytes. Acceptance must require the marker in the delivered history event itself.
- Assumption: the merged Core change preserves the current daemon DTO and ordering contract. If Core changes the public event variant or serialized fixture bytes, the conformance disposition in scope item 6 becomes required.
- Assumption: dropping a persistent daemon connection exercises the same automatic detach/reconnect boundary used by TUI `force_reconnect`; implementation must verify the TUI's session/subscription identifier behavior before fixing test identifiers.
- Unknown until Core merges: exact merged commit, whether retained history arrives as `Snapshot`, `Scrollback`, or both, and whether revision-12 fixture bytes remain accurate. Tests should accept the allowed history variants but require exact renderable content and ordering.
- Unknown until downstream rerun: whether the Core/Hub fix alone clears the TUI assertion. A remaining TUI failure must be diagnosed separately, not hidden with a Hub compatibility branch.

## Affected surfaces and files

- `Cargo.lock` — pin all Botster Core git packages to the merged prerequisite commit.
- `tests/hub_daemon_lifecycle_test.rs` — primary production-path same-session disconnect/reattach, payload-fidelity, ordering, live-output, and no-history regression.
- `docs/client-protocol.md` — distinguish renderable attach history from opaque capture/readback payloads.
- Conditional only if exported content changes:
  - `crates/botster-hub-client/src/lib.rs` — conformance revision.
  - `crates/botster-hub-test-support/src/lib.rs` — source fixture and guards.
  - `packages/hub-test-support/late-attach-history-conformance-fixture.json`, metadata, support matrix, README, package manifest, and package tests — regenerated published artifacts.
- No production Hub source file is expected to change unless the merged Core contract requires a thin type/projection adoption. Any proposed producer or terminal-state logic in Hub is out of scope and must return to planning.

## Implementation sequence

1. Reconfirm the Core dependency is closed and inspect its merged commit and regression evidence. Update the lockfile from merged Core `main` and record the exact coordinate.
2. Make the real daemon test reproduce the TUI lifecycle and require exact retained marker content. Run it against the old Core coordinate or ablate the adopted producer change to prove the test goes red, then restore the merged coordinate and prove it green.
3. Preserve and strengthen the no-history assertions within the real worker-backed path.
4. Update protocol prose to match the proven distinction between renderable history events and opaque capture payloads.
5. Compare runtime behavior to the source and checked-in conformance fixture. Apply the no-change or changed-artifact branch from scope item 6; do not bump metadata speculatively.
6. Run focused, workspace, formatting, lint, artifact, and downstream live checks. Attach exact commands and results to the implementation report.

## Risks

- **False-positive history proof:** checking `bytes > 0`, `data != ""`, or `ReadScreen` separately can still accept an all-NUL attach payload. Mitigation: assert retained markers in the delivered history event and reject all-NUL history explicitly.
- **Wrong lifecycle coverage:** a second concurrent late subscriber does not prove disconnect/reconnect. Mitigation: drop the real persistent connection, allow production cleanup to detach, reconnect, and attach the same session.
- **Stale or local dependency evidence:** a sibling Core checkout can pass while the merge coordinate remains broken. Mitigation: update from merged Core `main`, record `Cargo.lock`, and build fresh binaries.
- **Detach/reattach race:** old-connection cleanup could arrive after the new attach. The regression must expose this rather than adding sleeps or retries that conceal it; use bounded observation only for asynchronous output.
- **History duplication or loss:** both retained markers must appear once and in order before new live bytes.
- **No-history fabrication:** a fix that always emits a placeholder snapshot could satisfy ordering while violating semantics. The idle case must have no fabricated renderable history or scrollback.
- **Fixture identity drift:** changing exported bytes without a new globally unique revision recreates the revision-11 collision. Conversely, bumping unchanged content creates needless downstream churn.
- **Overreach into Core or clients:** duplicating producer logic in Hub or filtering in TUI would violate the human disposition and cold-turkey single-path convention.

## Acceptance checks and tests

- Dependency evidence:
  - Core ticket `ticket_1784057849_665844` is closed/merged.
  - `Cargo.lock` records that merged Core commit for `botster-core`, `botster-core-daemon`, `botster-core-test-support`, and `botster-terminal-ghostty` as applicable.
- Focused Hub regression:
  - `./test.sh external_daemon_attach_replays_prior_history_with_renderable_byte_count -- --nocapture`, renamed if the test is renamed, exercises real disconnect and same-session reattach.
  - The reattach subscription observes `Attaching`, renderable retained history containing both unique markers exactly once and in order, `Attached`, then a unique later live marker.
  - No all-NUL history payload satisfies the history assertion.
  - The no-history session observes `Attaching -> Attached -> live` without fabricated `Scrollback` or renderable history.
- Regression strength: with the adopted Core producer fix reverted/ablated, the focused reconnect test fails for missing retained markers; with the merged coordinate restored, it passes.
- Broader Hub checks:
  - `./test.sh --test hub_daemon_lifecycle_test -- --test-threads=1` for the serialized real-daemon suite.
  - `./test.sh` for the workspace, with any failure attributed to the first non-cascade cause rather than waived broadly.
  - `cargo fmt --all -- --check`.
  - `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
  - `git diff --check` and a committed-artifact PII/path scan.
- Conditional fixture/package checks if exported assets change:
  - source fixture, checked-in JSON, metadata, support matrix, and checksums agree;
  - package tests pass;
  - a clean temporary consumer installs the newly published exact package and verifies both revision and ordering/content.
- Downstream product proof: freshly build Hub and session-worker binaries from the final branch and rerun the TUI same-session live reconnect assertion; both retained markers replay once in order and live input/output resumes.

## Pipeline gates and artifacts

- Do not begin Hub implementation while the Core dependency is open.
- Implementation report must record the merged Core commit, old-coordinate red/new-coordinate green regression evidence, exact runtime entry path, fixture disposition, verification commands, and downstream TUI result.
- Plan Review should reject sibling-worktree overrides, Hub producer logic, client guards, marker assertions sourced only from `ReadScreen`, or a test that never disconnects.
- Review/Verify must inspect the final lockfile and rerun the real daemon test from the live worktree; resolved status alone is not evidence.

## Vault gaps worth capturing

- After implementation proves the shape, capture that Ghostty's opaque persistence snapshot and subscription-visible renderable attach history are distinct contracts and must not share a byte-length oracle.
- Reconcile older notes that name the session process as the sole VT parser/source of truth with the current CoreDaemon-owned Ghostty shadow implementation and the producer architecture chosen by the Core prerequisite.
- If the connection-drop test exposes a reusable detach-before-reattach ordering rule, capture it separately; do not infer it before the real path proves it.

No unresolved convention conflict remains. The human split keeps producer ownership in Core, Hub as the adopting host profile, clients free of compatibility workarounds, and the published fixture conditional on actual exported-content change.
