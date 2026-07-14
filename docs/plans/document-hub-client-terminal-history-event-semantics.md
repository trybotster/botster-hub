# Document hub-client terminal history event semantics

> **Superseded contract:** `docs/client-protocol.md` and conformance revision 14
> are the current authority. The renderable `Snapshot.data` / `Scrollback.data`
> semantics below, and commands for the removed
> `external_daemon_attach_replays_prior_history_with_renderable_byte_count`
> test, are retained only as historical planning context and must not guide
> implementation or client adoption.

## Context loaded

- Pipeline context: `ticket_1782241198_638252`, run `run_1782241215_310058`, current step `botster_plan`, gate `botster_plan_gate`.
- Role and repo playbooks: [[planner-playbook]], [[botster-planner-playbook]].
- Vault/project notes constraining this plan: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[botster hub client crate is the external client boundary]], [[botster web renderable history payload is data not bytes]], [[daemon attach drain cannot force snapshot or scrollback variants]], [[botster web live attach tests separate history dto support from trigger support]], [[daemon event shape changes bump conformance fixture revision not protocol version]], [[rustdoc doctests prove rust docs tickets against public api]], [[test script required for rust tests not cargo test]], and the Project Pipelines orchestration/checklist notes loaded through [[botster-planner-playbook]].
- Repo context inspected: `crates/botster-hub-client/src/lib.rs`, `docs/client-protocol.md`, `src/client_api.rs`, `src/daemon_transport.rs`, `src/main.rs`, `src/tui.rs`, `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`, `Cargo.toml`, and `test.sh`.
- Current baseline: `DaemonEvent::Snapshot` and `DaemonEvent::Scrollback` already serialize required `data` and `bytes` fields. `src/client_api.rs` preserves Snapshot/Scrollback before later TerminalOutput from core drain order, and `src/daemon_transport.rs` derives `bytes` from raw data length before UTF-8 lossy decoding. Live daemon coverage already asserts late subscription history precedes later live output and no empty-history fabrication occurs.

## Scope

- Tighten public docs/API comments in `crates/botster-hub-client/src/lib.rs` so downstream clients have an explicit contract:
  - `data` is the browser/client-renderable UTF-8 terminal history payload when present.
  - `bytes` is hub DTO metadata derived from original event data length before UTF-8 decoding, not a second renderable payload.
  - Snapshot/Scrollback history for an attach/drain sequence must be rendered before later live `TerminalOutput` for the same subscription.
  - missing `data` with positive `bytes` is an old-hub or opaque-history fallback, not permission to fabricate scrollback.
- Align `docs/client-protocol.md` with the same wording and call out the `stream_attach` helper limitation: it writes only `TerminalOutput`, while clients that need Snapshot/Scrollback event kind, payload, fallback handling, or ordering metadata should use persistent `DaemonConnection` with `Attach` and `Drain`.
- Tighten hub-client serde/contract tests in `crates/botster-hub-client/src/lib.rs` around Snapshot/Scrollback shape, including legacy/unsupported byte-only JSON if the current serde contract can express it without broad compatibility code.
- Add a small public-contract ordering test where most appropriate. Prefer a direct hub-client unit test over a new live daemon test if ordering can be proven without flaky PTY timing; otherwise rely on the existing live daemon ordering assertion and improve its naming/messages only if needed.

## Non-scope

- No daemon-side terminal history caches.
- No core or core-daemon contract changes. Stop and ask if implementation proves current dependencies cannot express the documented semantics.
- No browser/TUI rendering behavior changes unless the docs reveal an existing first-party client contradicts the public contract and the fix is narrowly required.
- No protocol version bump. A conformance fixture revision bump is only in scope if test fixture shape changes, not for comments/docs alone.
- No broad refactors, new abstraction layer, or adjacent cleanup.

## Assumptions and unknowns

- Assumption: the ticket is primarily documentation plus contract-test cleanup because current DTOs and daemon projection already carry renderable `data` and derived `bytes`.
- Assumption: old-hub/no-data fallback can be documented even though current `DaemonEvent::Snapshot` and `DaemonEvent::Scrollback` require `data`; if a deserialization compatibility path is needed, keep it local to the public DTO contract and ask before broadening.
- Unknown: whether there are downstream conformance fixtures outside this checkout that need revision changes. Implementer should inspect repo-local fixture use first and avoid changing `CONFORMANCE_FIXTURE_REVISION` for prose-only updates.
- Unknown: whether rustdoc examples are useful here. If adding API comments with examples, they should compile or be omitted.

## Affected surfaces/files

- `crates/botster-hub-client/src/lib.rs`: public `DaemonEvent` docs and serde/contract tests.
- `docs/client-protocol.md`: downstream client protocol documentation.
- `src/daemon_transport.rs`: existing projection tests may be referenced or adjusted only if needed for clearer byte/data semantics.
- `tests/hub_daemon_lifecycle_test.rs`: existing live ordering coverage may be referenced; avoid adding a new flaky daemon test unless no unit-level contract proof is available.

## Risks

- Overstating live event triggerability: public Attach/Drain historically could not force Snapshot/Scrollback variants in all paths. Docs must distinguish DTO/event semantics from trigger guarantees.
- Confusing `bytes` with renderable data: tests should fail if a future change drops `data` or treats byte counts as payload.
- Creating unnecessary compatibility code for byte-only legacy JSON. Prefer documentation unless current client code must deserialize old-hub events.
- Flaky PTY timing if new runtime tests are added unnecessarily.

## Acceptance checks/tests

- `./test.sh -p botster-hub-client snapshot_and_scrollback_events_carry_renderable_data` or the updated exact hub-client test name.
- `./test.sh -p botster-hub daemon_event_projection_decodes_history_data_and_keeps_raw_byte_counts`.
- `./test.sh external_daemon_attach_replays_prior_history_with_renderable_byte_count` if implementation touches live ordering docs or daemon-facing behavior.
- `./test.sh` as the preferred full regression when feasible; use the wrapper, not raw `cargo test`, so `BOTSTER_ENV=test` is set.
- Manual review: every changed line should trace to the ticket's history-event contract and should not introduce daemon caches, core dependency changes, or protocol-version churn.

## Vault gaps worth capturing

- Capture after implementation only if a new durable rule emerges, such as a concrete old-hub byte-only deserialization fallback convention or a stable hub-client history ordering test pattern. Existing notes already cover the primary contract: [[botster web renderable history payload is data not bytes]], [[daemon attach drain cannot force snapshot or scrollback variants]], and [[daemon event shape changes bump conformance fixture revision not protocol version]].
