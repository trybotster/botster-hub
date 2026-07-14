# Adopt Core Same-Session Reattach History Fix

> **Superseded contract:** Human decision `question_1784062434_125317`
> replaced this adoption-only plan with the cold-turkey binary-safe history DTO
> correction. `docs/client-protocol.md` and conformance revision 14 are the
> current authority. The renderable `Snapshot.data` / `Scrollback.data`
> requirements below are retained only as historical planning context and must
> not guide implementation or client adoption.

## Context loaded

- Pipeline ticket: `ticket_1784052230_812754`, **Hub: adopt Core same-session reattach history fix and prove daemon path**.
- Required playbooks: [[planner-playbook]] and [[botster-planner-playbook]], plus the Botster architecture, CLI, and SPA maps they require.
- Controlling vault context: [[coredaemon attached follows initial snapshots before live terminal output]], [[opaque terminal snapshot bytes do not prove renderable history]], [[terminal subscribe readiness gates on sessionio initial snapshot delivery]], [[initial terminal snapshots must precede live output activation]], [[hub replays full history on every attach so clients must clear per cycle]], [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]], [[conformance fixture revisions must be unique per published content]], [[a regression test must be shown to go red with the fix reverted]], and [[test script required for rust tests not cargo test]].
- Repository baseline: Hub `a9b8c637` pins Core `db69456`. Revision 12 moved the production path to Ghostty-backed Core state, while the Hub late-attach tests weakened visible-marker assertions to `data` non-empty / `bytes` positive.
- Production route inspected: daemon socket -> `daemon_transport` -> `HubClientApi` -> `HubRuntime` -> `CoreDaemon` -> worker-backed session runtime. Hub maps Core `TransportEgress::{Snapshot, Scrollback}` into public daemon history events.
- Pipeline context initially had no findings or reviews. Human answer `question_1784052486_268012` split the work: Core ticket `ticket_1784057849_665844` owns the producer and payload-fidelity fix; this Hub ticket depends on it and owns merged-coordinate adoption plus end-to-end proof.
- Plan Review `review_1784058311_575755` required three surgical amendments: remove the remaining in-process non-empty-byte oracle, replace rather than supplement the weak daemon test, and make Core merge plus downstream TUI evidence reproducible.
- Baseline command `./test.sh external_daemon_attach_replays_prior_history_with_renderable_byte_count -- --nocapture` passes one test. That green result is not acceptance evidence: the test uses a second late subscriber, never drops and reconnects the daemon connection, and accepts any non-empty history payload without requiring the retained marker.

## Scope

1. Hold implementation until Core ticket `ticket_1784057849_665844` has a producer fix merged into `botster-core` `origin/main` with real Ghostty disconnect/reattach regression evidence. Ticket status alone is not merge evidence.
2. Update `Cargo.lock` to that merged Core `main` coordinate. Prove the exact locked revision is reachable from `botster-core` `origin/main` and is the revision recorded for every `botster-core*` git package. Use the normal git dependency path; do not use a sibling checkout, path override, or unmerged Core revision as final evidence.
3. Rename and rewrite `external_daemon_attach_replays_prior_history_with_renderable_byte_count` as `external_daemon_same_session_reattach_replays_renderable_history_before_live_output`; do not supplement it while leaving the old byte-count test or predicate in place:
   - spawn one worker-backed session that emits a unique marker before the first attach;
   - attach and drain that marker through a persistent daemon connection;
   - send a second unique marker after the first attach and drain it;
   - drop the client connection so production connection cleanup detaches its subscription;
   - open a fresh daemon connection and attach the same session using the identifier lifecycle used by the downstream TUI reconnect path;
   - require reattach history to contain both retained markers exactly once and in order;
   - require `Attaching < renderable Snapshot/Scrollback < Attached < new live TerminalOutput` for that subscription;
   - fail on an all-NUL/opaque blank history payload instead of treating byte length or event presence as visible-history proof.
4. Strengthen `late_attach_receives_prior_terminal_history_before_later_live_output` in `tests/hub_client_api_test.rs`. Because `explicit_runtime()` uses the real session worker, its history predicate must find the retained marker bytes in the delivered `Snapshot`/`Scrollback`, assert their order relative to later live output, and reject all-NUL data. No Hub history test may remain green solely because a history event is present or non-empty.
5. Keep the no-history branch in the same real daemon coverage: it must observe `Attaching -> Attached -> live` with no fabricated renderable history or `Scrollback`.
6. Tighten `docs/client-protocol.md` so public attach `Snapshot`/`Scrollback.data` is described as renderable history, while `CaptureSnapshot` remains a separate backend-opaque readback payload. Payload length alone is not evidence of visible history.
7. Compare the fixed runtime sequence with `late_attach_history_conformance_scenario()` and its checked-in npm copy. If the existing revision-12 fixture already matches the proven event shape and bytes, leave the revision and package untouched. If exported fixture content changes, allocate a globally unused conformance revision above every published meaning, regenerate every derived asset/checksum, bump the npm package version, and prove a clean external install.
8. After Hub verification, build fresh `botster-hub` and `botster-session-worker` binaries from this final Hub branch and rerun `headless_dogfood_runs_against_isolated_hub_when_binaries_are_available` in the `trybotster/botster-tui` repository on branch `project-pipelines/ticket_1783965015_654184`, using the exact command in Acceptance. Hub remains the TUI ticket's dependency; no TUI code changes belong here. If Hub tests are green but that assertion still fails, call `project_pipelines_ask_human` with the Hub evidence and TUI failure before changing Hub behavior.

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
- Assumption: the merged Core change preserves the current daemon DTO and ordering contract. If Core changes the public event variant or serialized fixture bytes, the conformance disposition in scope item 7 becomes required.
- Assumption: dropping a persistent daemon connection exercises the same automatic detach/reconnect boundary used by TUI `force_reconnect`; implementation must verify the TUI's session/subscription identifier behavior before fixing test identifiers.
- Determined: the downstream assertion is `headless_dogfood_runs_against_isolated_hub_when_binaries_are_available` in `trybotster/botster-tui` branch `project-pipelines/ticket_1783965015_654184`; it calls `DogfoodApp::force_reconnect` and checks one ordered replay of both markers.
- Unknown until Core merges: exact merged commit, whether retained history arrives as `Snapshot`, `Scrollback`, or both, and whether revision-12 fixture bytes remain accurate. Tests should accept the allowed history variants but require exact renderable content and ordering.
- Unknown until downstream rerun: whether the Core/Hub fix alone clears the TUI assertion. If it remains red after the Hub adoption tests are green, stop and ask the human to route the cross-repo failure; do not add Hub compensation.

## Affected surfaces and files

- `Cargo.lock` — pin all Botster Core git packages to the merged prerequisite commit.
- `tests/hub_daemon_lifecycle_test.rs` — primary production-path same-session disconnect/reattach, payload-fidelity, ordering, live-output, and no-history regression.
- `tests/hub_client_api_test.rs` — replace the real worker-backed in-process non-empty-byte history oracle with retained-marker and all-NUL-rejection assertions.
- `docs/client-protocol.md` — distinguish renderable attach history from opaque capture/readback payloads.
- Conditional only if exported content changes:
  - `crates/botster-hub-client/src/lib.rs` — conformance revision.
  - `crates/botster-hub-test-support/src/lib.rs` — source fixture and guards.
  - `packages/hub-test-support/late-attach-history-conformance-fixture.json`, metadata, support matrix, README, package manifest, and package tests — regenerated published artifacts.
- No production Hub source file is expected to change unless the merged Core contract requires a thin type/projection adoption. Any proposed producer or terminal-state logic in Hub is out of scope and must return to planning.

## Implementation sequence

1. Fetch `botster-core` `origin/main`, inspect the prerequisite regression evidence, and prove the candidate commit is reachable from that remote branch. Update the lockfile through the normal `main` dependency and verify every locked Core git package resolves to that exact revision.
2. Rename and rewrite the existing weak daemon test so it reproduces the TUI lifecycle and requires exact retained marker content; delete its byte-count-only predicate. Run it against the old Core coordinate or ablate the adopted producer change to prove the test goes red, then restore the merged coordinate and prove it green.
3. Strengthen the in-process worker-backed test's history payload assertion and preserve the no-history assertions within the real worker-backed path.
4. Update protocol prose to match the proven distinction between renderable history events and opaque capture payloads.
5. Compare runtime behavior to the source and checked-in conformance fixture. Apply the no-change or changed-artifact branch from scope item 7; do not bump metadata speculatively.
6. Run focused, workspace, formatting, lint, artifact, and downstream live checks. Attach exact commands and results to the implementation report.

## Risks

- **False-positive history proof:** checking `bytes > 0`, `data != ""`, or `ReadScreen` separately can still accept an all-NUL attach payload. Mitigation: assert retained markers in the delivered history event and reject all-NUL history explicitly.
- **Wrong lifecycle coverage:** a second concurrent late subscriber does not prove disconnect/reconnect. Mitigation: drop the real persistent connection, allow production cleanup to detach, reconnect, and attach the same session.
- **Stale or local dependency evidence:** a sibling Core checkout can pass while the merge coordinate remains broken. Mitigation: update from merged Core `main`, record `Cargo.lock`, and build fresh binaries.
- **Preserved false-positive guard:** adding a strong reconnect test while retaining a green non-empty-byte test continues teaching the wrong contract. Mitigation: rewrite both named weak tests; do not leave any history oracle satisfied by presence, length, or non-empty bytes alone.
- **Detach/reattach race:** old-connection cleanup could arrive after the new attach. The regression must expose this rather than adding sleeps or retries that conceal it; use bounded observation only for asynchronous output.
- **History duplication or loss:** both retained markers must appear once and in order before new live bytes.
- **No-history fabrication:** a fix that always emits a placeholder snapshot could satisfy ordering while violating semantics. The idle case must have no fabricated renderable history or scrollback.
- **Fixture identity drift:** changing exported bytes without a new globally unique revision recreates the revision-11 collision. Conversely, bumping unchanged content creates needless downstream churn.
- **Overreach into Core or clients:** duplicating producer logic in Hub or filtering in TUI would violate the human disposition and cold-turkey single-path convention.

## Acceptance checks and tests

- Dependency evidence:
  - The prerequisite commit is present on fetched `botster-core` `origin/main`; record `git -C <botster-core-checkout> branch -r --contains <full-rev>` (which must include `origin/main`) and `git -C <botster-core-checkout> rev-parse <full-rev>` in the implementation report. A closed ticket without this evidence is insufficient.
  - `Cargo.lock` records that same full revision for every `botster-core*` git package, including `botster-core`, `botster-core-daemon`, `botster-core-test-support`, and `botster-terminal-ghostty` as applicable; no locked Core git package may remain on a different revision.
- Focused Hub regression:
  - `external_daemon_same_session_reattach_replays_renderable_history_before_live_output` replaces `external_daemon_attach_replays_prior_history_with_renderable_byte_count`, exercises real disconnect and same-session reattach, and leaves neither the original byte-count-named test nor its predicate in the suite.
  - `./test.sh external_daemon_same_session_reattach_replays_renderable_history_before_live_output -- --nocapture` passes.
  - `./test.sh late_attach_receives_prior_terminal_history_before_later_live_output -- --nocapture` requires the retained marker inside delivered history and rejects all-NUL/non-renderable data.
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
- Downstream product proof, from the final Hub worktree followed by the `trybotster/botster-tui` checkout on branch `project-pipelines/ticket_1783965015_654184`:
  ```sh
  export HUB_ROOT="$(pwd)"
  CARGO_TARGET_DIR=/tmp/botster-hub-reattach-proof-target cargo build --locked --bin botster-hub
  CARGO_TARGET_DIR=/tmp/botster-hub-reattach-proof-target cargo build --locked -p botster-core --bin botster-session-worker
  BOTSTER_TUI_REQUIRE_HUB_TEST=1 BOTSTER_HUB_BIN=/tmp/botster-hub-reattach-proof-target/debug/botster-hub BOTSTER_SESSION_WORKER_BIN=/tmp/botster-hub-reattach-proof-target/debug/botster-session-worker BOTSTER_PLUGIN_CONTRACT_MATRIX_FIXTURE="$HUB_ROOT/fixtures/plugins/plugin-contract-matrix" CARGO_TARGET_DIR=/tmp/botster-tui-reattach-proof-target ./test.sh -p botster-tui headless_dogfood_runs_against_isolated_hub_when_binaries_are_available -- --nocapture
  ```
  Set `HUB_ROOT` to the final Hub worktree before entering the TUI checkout. Record `git branch --show-current` for both repos. The TUI assertion must show both retained markers replay once in order and live input/output resumes. If it fails while both Hub regressions are green, call `project_pipelines_ask_human` with the exact failure and evidence; do not alter Hub semantics without a new disposition.

## Pipeline gates and artifacts

- Do not begin Hub implementation while the Core dependency is open.
- Implementation report must record the merged Core commit and `origin/main` reachability, lockfile revision equality across Core packages, old-coordinate red/new-coordinate green regression evidence, exact runtime entry path, disposition of both formerly weak tests, fixture disposition, verification commands, both repo branches, and downstream TUI result.
- Plan Review should reject sibling-worktree overrides, Hub producer logic, client guards, marker assertions sourced only from `ReadScreen`, or a test that never disconnects.
- Review/Verify must inspect the final lockfile and rerun the real daemon test from the live worktree; resolved status alone is not evidence.

## Vault gaps worth capturing

- After implementation proves the shape, capture that Ghostty's opaque persistence snapshot and subscription-visible renderable attach history are distinct contracts and must not share a byte-length oracle.
- Reconcile older notes that name the session process as the sole VT parser/source of truth with the current CoreDaemon-owned Ghostty shadow implementation and the producer architecture chosen by the Core prerequisite.
- If the connection-drop test exposes a reusable detach-before-reattach ordering rule, capture it separately; do not infer it before the real path proves it.

No unresolved convention conflict remains. The human split keeps producer ownership in Core, Hub as the adopting host profile, clients free of compatibility workarounds, and the published fixture conditional on actual exported-content change.
