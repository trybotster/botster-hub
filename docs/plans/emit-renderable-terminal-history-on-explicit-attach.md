# Emit Renderable Terminal History On Explicit Attach

## Context Loaded

- Pipeline context: `ticket_1781311109_714175`, run `run_1781311125_693780`, returned step `botster_plan`, gate `botster_plan_gate`.
- Prior plan artifact: `artifact_1781311370_129245`.
- Plan Review: `review_1781311601_860595` returned changes required with findings `finding_1781311601_341295`, `finding_1781311601_271093`, `finding_1781311601_582473`, and `finding_1781311601_814659`.
- Human answer: `question_1781311557_965819` chose option A. The renderable DTO/projection work from `ticket_1781191117_149624` is a dependency and must not be re-authored here.
- Dependency registered: `dependency_1781326871_771291` for `ticket_1781191117_149624`.
- Dependency base applied in this worktree: merged `origin/project-pipelines/ticket_1781191117_149624` at `d112ab5` into this run, producing current HEAD `18e364e`. The run now includes `Snapshot.data` / `Scrollback.data`, `HubClientEvent` renderable history payloads, projection tests, docs, and conformance revision from the dependency branch.
- Role and Botster playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Vault/self context: [[identity]], [[goals]].
- Required Botster architecture notes: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan agents must author vault context as wikilinks not home paths]].
- Terminal/protocol constraints: [[botster terminal clients share one sessionio data plane subscription path]], [[botster durable terminal egress is owned by sessionio and clientworker actors]], [[botster terminal egress is session backed only]], [[botster initial terminal scrollback is delivered by sessionio directly to clientworker]], [[terminal subscribe readiness gates on sessionio initial snapshot delivery]], [[initial terminal snapshots must precede live output activation]], [[daemon event shape changes bump conformance fixture revision not protocol version]], [[external client hub tests use subprocess spawned hub test support]], [[daemon attach drain cannot force snapshot or scrollback variants]], [[botster web live attach tests separate history dto support from trigger support]], [[test script required for rust tests not cargo test]].
- Repo context inspected: `crates/botster-hub-client/src/lib.rs`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/main.rs`, `crates/botster-hub-test-support/src/lib.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `docs/client-protocol.md`.
- Checklist discipline: `checklist_1781311175_633140` records vault context, convention fit, verification planning, and capture decision. Returned-plan updates are attached through this revised plan and gate evidence.

## Scope

- Make explicit `DaemonRequest::Attach` / subscription to an existing running terminal session emit renderable history when history exists, using the already-merged `Snapshot.data` / `Scrollback.data` contract from dependency commit `d112ab5`.
- Preserve the current architecture: daemon socket -> `daemon_transport` -> `HubClientApi` -> `HubRuntime` -> SessionIo/ClientWorker data plane.
- Preserve ordering: initial history for the new subscription must be delivered before later live `terminal_output` for that subscription.
- Preserve no-history semantics: do not fabricate `Snapshot` or `Scrollback` when no history is available.
- Make daemon drain/history cursoring subscription-scoped so one session's prior cursor cannot starve a newly attached subscription.
- Add deterministic in-repo daemon-socket trigger proof that an existing-session attach emits non-empty renderable `Snapshot` or `Scrollback` before later live output.
- Update protocol docs only for the attach-trigger behavior if the dependency docs do not already cover the new trigger semantics.

## Non-Scope

- No botster-web fallback behavior in hub.
- No second terminal transport path, hub byte relay, or direct session-worker protocol exposure.
- No plugin/Pipeline UI changes.
- No broad refactor of runtime ownership, package lifecycle, or unrelated daemon requests.
- No re-authoring `DaemonEvent::{Snapshot, Scrollback}.data`, `HubClientEvent::{Snapshot, Scrollback}.data`, projection tests, conformance revision, or event-shape docs already owned by `ticket_1781191117_149624`.
- No protocol-version bump unless framing or request/response semantics change; event-shape fixture drift should use `CONFORMANCE_FIXTURE_REVISION`.

## Assumptions And Unknowns

- Dependency sequencing is resolved for this worktree by merging `origin/project-pipelines/ticket_1781191117_149624` (`d112ab5`) into HEAD `18e364e`. Implement must not duplicate or diverge from that DTO/projection implementation.
- Existing attach currently returns `Events(Vec::new())` from `HubClientApi::Attach`; `DrainRuntime` is the only visible projection path for terminal egress.
- `daemon_transport` currently tracks `drain_cursors` by session id, seeded at spawn and advanced per drain. That keying is wrong for this ticket: a new subscription to an already-drained session needs its own initial history boundary.
- The plan commits to subscription-scoped cursor or boundary keying. Use a key that includes both `session_id` and `subscription_id`, or introduce a small typed key/helper with equivalent semantics. Do not leave history correctness dependent on the session-scoped cursor.
- The likely surgical fix is to make attach synchronously collect the initial SessionIo/ClientWorker egress for the attached subscription after `runtime.attach_client(...)` and before returning the attach response, then seed or advance that subscription's drain cursor so later drains do not duplicate the same history.
- If the runtime still only emits pre-attach history as `TerminalOutput`, that is not sufficient for this ticket. Implementation must produce a renderable `Snapshot` or `Scrollback` for available history, or stop and ask a human before changing the acceptance target.

## Affected Surfaces And Files

- Daemon socket production path and drain cursor state: `src/daemon_transport.rs`.
- Transport-neutral API only if a narrow helper is needed to collect attach-generated egress: `src/client_api.rs`.
- Public client DTO/compatibility from dependency baseline only, not this ticket's scope: `crates/botster-hub-client/src/lib.rs`.
- CLI event display only if attach-trigger docs/tests expose an existing dependency display gap: `src/main.rs`.
- External live-hub test support and support matrix only if conformance trigger proof is extended there: `crates/botster-hub-test-support/src/lib.rs`.
- In-process and live daemon tests: `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`.
- Protocol documentation: `docs/client-protocol.md`.

## Implementation Plan

1. Confirm dependency baseline before editing trigger code.
   - Verify `DaemonEvent::Snapshot` and `DaemonEvent::Scrollback` include `data` and `bytes`.
   - Verify `HubClientEvent::{Snapshot, Scrollback}` preserve renderable `data` from `TransportEgress`.
   - Verify `CONFORMANCE_FIXTURE_REVISION` reflects the dependency branch's event-shape change.
   - Do not modify those dependency-owned fields unless resolving merge conflicts from the dependency branch.

2. Add failing trigger tests before production changes.
   - Spawn a long-running session that prints prior output.
   - Wait until prior output is observable before explicit attach.
   - Attach a new subscription through the persistent daemon socket path.
   - Assert the attach response, or the first immediate drain if the code intentionally keeps attach responses empty, contains a non-empty renderable `Snapshot.data` or `Scrollback.data`.
   - Send later input and assert later `TerminalOutput.data` arrives after the history event for the same subscription.
   - Add a same-session multi-subscription test: after one subscription has drained live output, attach a second subscription and prove it still receives available history without the first subscription receiving duplicate history.

3. Change the attach production path.
   - Keep `Attach` routed through `HubClientApi::Attach` and `HubRuntime::attach_client`.
   - After a successful attach, synchronously collect the initial egress generated for that subscription before returning the attach response.
   - Replace or augment `drain_cursors: BTreeMap<String, u64>` with subscription-scoped cursor/boundary state keyed by `session_id` + `subscription_id`.
   - Seed attach history from the correct pre-attach boundary for the new subscription, then advance only that subscription's cursor after history is emitted.
   - Filter/correlate emitted egress by subscription id so one client does not receive another client's queued history.

4. Preserve no-history and cleanup behavior.
   - Add a no-history attach test that proves the response is empty or contains only legitimate attach-state metadata.
   - Keep EOF/disconnect cleanup tests passing so dropped sockets detach subscriptions.
   - Keep existing send-input, resize, duplicate-spawn, and dogfood bridge attach tests passing.

5. Document the explicit attach contract.
   - `docs/client-protocol.md` should say explicit attach/subscription to an existing running session emits renderable `Snapshot` or `Scrollback` when history exists.
   - Document ordering: history is emitted before later live terminal output for that subscription.
   - Document no-history behavior.

## Risks

- Duplicate output if attach returns history and the subsequent drain cursor is not advanced consistently.
- Starved history if the existing session-level drain cursor remains the only cursor for all subscriptions.
- Cross-subscriber leakage if subscription ids are not part of cursoring/filtering.
- Ordering regression if live output is activated before initial history delivery.
- DTO double-authorship or merge conflict risk if implementer edits dependency-owned event fields instead of treating `d112ab5` as baseline.
- Event-shape drift between Rust serde DTOs, docs, support matrix, and downstream botster-web TypeScript mirrors if the dependency merge is disturbed.
- Test flake in live subprocess tests if readiness waits only for process existence rather than prior output observation.
- PII/path leakage in docs or plan artifacts; vault notes must stay wikilinked rather than absolute local paths.

## Acceptance Checks And Tests

- `./test.sh daemon_existing_session_attach_emits_renderable_history_before_live_output` or the exact new daemon socket integration test filter; this is the authoritative in-repo trigger proof.
- `./test.sh daemon_second_subscription_gets_history_after_first_subscription_drain` or the exact new multi-subscription cursor regression filter.
- `./test.sh hub_client_api_existing_session_attach_emits_renderable_history_before_live_output` if the trigger helper touches `HubClientApi`.
- `./test.sh external_hub_client_crate_drives_real_daemon_socket_protocol` after extending conformance/support evidence if needed.
- `./test.sh` before handoff if runtime changes are broader than the focused path.
- Confirm live packaged botster-web smoke observes history after browser refresh plus explicit Attach as product-path confirmation; do not use it as the only trigger proof.
- PII scan of the committed diff and plan artifact, including local home-path and username patterns.

## Vault Gaps Worth Capturing

- Capture a durable note explaining why daemon terminal drain/history cursors must be subscription-scoped for restored history.
- If a new public attach-history helper/request is required, capture the distinction between DTO support, public triggerability, and live browser acceptance.
- Capture the dependency sequencing lesson if this stack still exposes friction after merging the registered dependency.

## Checklist Evidence

- Vault notes read: listed in Context Loaded.
- Convention conflicts: none after adopting human option A. The plan keeps terminal bytes in the SessionIo/ClientWorker path, uses the dependency-owned hub-client DTO boundary, scopes this ticket to trigger behavior, and does not add speculative workflow primitives.
- Verification planned: repo wrapper `./test.sh`, not raw `cargo test`; targeted tests first, full test suite when blast radius warrants it.
- Durable capture: capture only if implementation reveals one of the vault gaps above.
