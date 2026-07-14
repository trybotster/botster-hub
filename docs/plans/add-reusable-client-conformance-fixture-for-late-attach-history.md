# Add Reusable Client Conformance Fixture For Late Attach History

> **Superseded contract:** `docs/client-protocol.md` and conformance revision 14
> are the current authority. The renderable `Snapshot.data` / `Scrollback.data`
> semantics below, and commands for the removed
> `external_daemon_attach_replays_prior_history_with_renderable_byte_count`
> test, are retained only as historical planning context and must not guide
> implementation or client adoption.

Ticket: `ticket_1782241199_399036`
Run: `run_1782246171_565810`
Step: Plan

## Context Loaded

- Pipeline context: ticket, run, active Plan step, gate prompt, dependency, artifacts, findings, questions, prior answers, reviews, events, and checklist state via `project_pipelines_current_context`.
- Dependency context: closed dependency `ticket_1782241198_638252` ("Document hub-client terminal history event semantics").
- Required playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Vault context: [[identity]], [[goals]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[botster first party client support matrices belong in hub test support]], [[botster hub client crate is the external client boundary]], [[daemon event shape changes bump conformance fixture revision not protocol version]], [[daemon attach drain cannot force snapshot or scrollback variants]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[plan agents must author vault context as wikilinks not home paths]].
- Repo context inspected: `Cargo.toml`, `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-test-support/src/lib.rs`, `docs/client-protocol.md`, `tests/hub_daemon_lifecycle_test.rs`, and prior plan docs for external conformance and late-subscriber history.
- Existing baseline: `botster-hub-client` owns public `DaemonEvent` DTOs, including `TerminalOutput`, `Snapshot`, `Scrollback`, `ProcessExit`, and `AttachState`; `Snapshot`/`Scrollback` carry renderable `data` plus `bytes`. `docs/client-protocol.md` already documents ordering, no-fabrication, and the public daemon/client boundary.
- Existing runtime proof: `tests/hub_daemon_lifecycle_test.rs` already has a live daemon regression for late attach history replay, ordering before later live output, and no-history behavior. This supersedes the older [[daemon attach drain cannot force snapshot or scrollback variants]] limitation for this specific restored-history path, while the new fixture remains a public DTO scenario rather than another daemon history implementation.
- Checklist discipline: Project Pipelines checklist instructions were loaded. Run checklist `checklist_1782246205_609913` records vault context, no convention conflict, planned verification, and no Plan-time capture.

## Scope

- Add a reusable public client conformance fixture or scenario in `crates/botster-hub-test-support`, built from `botster_hub_client::DaemonEvent` / hub-client DTOs only.
- Cover the late attach history sequence:
  - an attaching client receives `Snapshot` or `Scrollback` history for an existing session;
  - history appears before later live `TerminalOutput`;
  - no `Snapshot` or `Scrollback` history is fabricated when no renderable `data` exists;
  - process/attach/control events remain distinguishable from terminal bytes.
- Prefer a small static fixture/helper plus tests over a live daemon harness. The live daemon path is already covered by the existing daemon lifecycle regression.
- Keep the fixture reusable for web, TUI, and future clients to mirror from stable serde JSON or consume as a dev/test Rust API without compiling private hub/core/session-worker internals.
- Update protocol docs or crate rustdoc so first-party clients know how to use or mirror the fixture and what each assertion proves.

## Non-Scope

- Do not edit `botster-web` or `botster-tui`.
- Do not add or duplicate terminal history caches.
- Do not depend on `botster_core::contract`, session-worker frames, `TransportEgress`, `SessionIo`, `ClientWorker`, daemon internals, TUI internals, or plugin worker internals.
- Do not add a heavy new live daemon harness unless implementation discovers an existing public test-support pattern that already makes this cheap and necessary.
- Do not change the `DaemonEvent` wire shape unless implementation finds the current public DTO cannot express the documented contract.
- Do not add broad protocol abstraction, optional configurability, or adjacent cleanup beyond what the fixture and docs require.

## Assumptions And Unknowns

- Assumption: `crates/botster-hub-test-support` is the canonical fixture home because first-party support matrices and downstream-shaped conformance helpers already live there. `crates/botster-hub-client` remains the DTO and `CONFORMANCE_FIXTURE_REVISION` source.
- Decision: clients mirror a stable serde JSON fixture or consume the typed Rust dev/test API from `botster-hub-test-support`. The fixture is not production-importable from `botster-hub-client`.
- Decision: implement a concrete typed scenario struct, with named positive and no-history constructors or functions returning `Vec<botster_hub_client::DaemonEvent>`. Do not leave fixture shape to implementation choice.
- Decision: `CONFORMANCE_FIXTURE_REVISION` stays `2`. This ticket adds fixture/docs/tests with no `DaemonEvent` wire-shape change, so there is no revision bump; any support fixture derives the revision from `botster_hub_client::CONFORMANCE_FIXTURE_REVISION`.
- Assumption: docs should live in `docs/client-protocol.md` and rustdoc on the test-support fixture API, so browser/TUI/future clients can see both the public path to mirror and the Rust dev/test API.
- Worktree/target assumption: implementation happens in this run's assigned worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`; artifacts should avoid local absolute vault paths.

## Affected Surfaces And Files

- `crates/botster-hub-test-support/src/lib.rs`: primary surface for the reusable public fixture/scenario, likely beside `FirstPartyClientSupportMatrix`, `first_party_client_support_matrix`, `run_client_conformance`, and existing `CONFORMANCE_*` constants.
- `crates/botster-hub-client/src/lib.rs`: DTO and conformance revision source only; no fixture ownership expected unless implementation needs minor client-DTO tests.
- `docs/client-protocol.md`: document how first-party clients should consume or mirror the fixture and how it maps to late attach rendering behavior.
- `tests/hub_daemon_lifecycle_test.rs`: no new harness required by default; keep the existing live daemon late-history regression as runtime proof and add/adjust only enough to prove the published fixture is exercised next to the conformance surface.

## Implementation Plan

1. In `botster-hub-test-support`, add a small public fixture/scenario with stable test data:
   - session id and subscription id values;
   - one history event, preferably `Snapshot` or `Scrollback`, with non-empty `data` and matching `bytes`;
   - a later `TerminalOutput` event for the same subscription;
   - distinguishable non-terminal events such as `AttachState` and `ProcessExit`;
   - a no-history scenario that contains no non-empty `Snapshot`/`Scrollback` data.
2. Shape the API as a typed scenario struct plus named positive/no-history constructors or functions returning `Vec<botster_hub_client::DaemonEvent>`. Expose stable serde JSON so browser/TUI tests can mirror the same scenario without runtime dependency on the Rust crate.
3. Wire the scenario into or beside the existing first-party conformance surface: either reference it from `FirstPartyClientSupportMatrix`/related support metadata or add a test that exercises the published fixture exactly as a downstream client would mirror it.
4. Add unit tests in `botster-hub-test-support` proving:
   - history event precedes live `TerminalOutput`;
   - no-history scenario does not fabricate non-empty `Snapshot`/`Scrollback`;
   - `AttachState`/`ProcessExit` are not classified as terminal bytes;
   - `Snapshot`/`Scrollback` `bytes` match `data.len()` for fixture payloads.
5. Update `docs/client-protocol.md` to name the exact public test-support fixture path and explain that web/TUI/future clients should mirror the stable JSON, render `Snapshot`/`Scrollback.data` before later `TerminalOutput.data`, and treat process/attach events as metadata/control events.
6. Preserve the existing live daemon regression as proof of the runtime path: daemon socket -> `HubClientApi` -> core data plane -> public `DaemonEvent` projection. Do not replace that runtime proof with fixture-only tests.

## Risks

- Dead fixture risk: a static sequence could drift from public DTOs or not be used by clients. Mitigation: build it from actual `DaemonEvent` values in `botster-hub-test-support`, expose stable serde JSON, wire it into or next to `FirstPartyClientSupportMatrix`, and test it the way a downstream client would mirror it.
- Over-abstraction risk: assertion helpers could become a client framework. Mitigation: keep helpers small and tied only to this conformance scenario.
- False runtime confidence: fixture tests alone do not prove live attach behavior. Mitigation: explicitly retain and cite the existing live daemon late-history regression.
- Protocol duplication risk: a new fixture struct could mirror DTO fields. Mitigation: expose DTO-shaped values and avoid parallel terminal event types.
- Fixture revision ambiguity: none for this ticket. `CONFORMANCE_FIXTURE_REVISION` stays `2` because there is no `DaemonEvent` wire-shape change; the test-support fixture must derive the value from `botster_hub_client::CONFORMANCE_FIXTURE_REVISION`.

## Acceptance Checks And Tests

- `cargo test -p botster-hub-test-support`
- Targeted test-support filter if added, for example `cargo test -p botster-hub-test-support late_attach_history_conformance_fixture`
- A test or assertion proving the fixture is referenced from or exercised beside the existing first-party conformance surface, and `docs/client-protocol.md` names the exact public fixture path clients mirror.
- Runtime path preservation check: `cargo test --test hub_daemon_lifecycle_test external_daemon_attach_replays_prior_history_with_renderable_byte_count`
- If docs include compile-checked examples, run the relevant doc test command for `botster-hub-test-support`.
- Verification evidence must state both:
  - fixture path: public `botster-hub-test-support` scenario built from `botster_hub_client::DaemonEvent` values can be consumed in tests or mirrored as stable JSON by clients;
  - runtime path: existing daemon lifecycle test proves the production attach/drain path emits matching public event semantics.

## Pipeline Gates And Artifacts

- Plan artifact: this file.
- Checklist artifact: `checklist_1782246205_609913` with vault context, convention conflict, verification, and capture evidence.
- Plan gate should include this plan path and the explicit assumption that the implementation is an intentional test/docs conformance contract for clients, not a new daemon history implementation.

## Vault Gaps Worth Capturing

- No Plan-time vault gap found. Existing notes already cover the public hub-client boundary, hub-owned test-support placement, renderable history payloads, attach/drain trigger limitations, fixture revision handling, and the no-daemon-cache constraint.
- Capture after implementation only if the final helper establishes durable guidance beyond [[botster first party client support matrices belong in hub test support]], such as the exact stable JSON scenario shape for late attach history.
