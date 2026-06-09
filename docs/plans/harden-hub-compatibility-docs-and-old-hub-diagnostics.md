# Harden Hub Compatibility Docs And Old-Hub Diagnostics

## Context Loaded

- Pipeline context: ticket `ticket_1781041965_845035`, run `run_1781041969_891448`, step `botster_plan`, gate `botster_plan_gate`.
- Gate prompt: attach context loaded, scope/non-scope, assumptions/unknowns, affected files, risks, acceptance checks/tests, and vault gaps.
- Prior artifacts, findings, reviews, questions, and answers: none present at planning time.
- Repo state: clean working tree before this plan artifact.
- Role and Botster planning notes: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], [[spa-patterns]].
- Ticket-specific architecture notes: [[botster hub client crate is the external client boundary]], [[botster hub client compatibility descriptors belong in client crate]], [[external client hub tests use subprocess spawned hub test support]], [[rustdoc doctests prove rust docs tickets against public api]], [[rust repo strict lints must be verified before dismissing warnings]], [[rustdoc intra doc links break on feature gated items]].
- Pipeline discipline notes: [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]].
- Checklist evidence: `project_pipelines_create_vault_checklist` timed out in the plugin worker, so checklist evidence is duplicated here and should also be attached to gate evidence.

## Scope

- Add compile-checked public compatibility examples for the API documented in `docs/client-protocol.md`, especially `DaemonCompatibilityRequirement::current` and `connect_and_hello_with_requirement`.
- Improve old/pre-compatibility hub handling so missing `compatibility` in hello/status-shaped responses becomes a `DaemonTransportError::Compatibility` diagnostic such as "hub predates compatibility handshake" where practical.
- Decide the `DaemonHello.compatibility` direction with the smallest clear change. Preferred plan: keep it as a reserved client requirement field because the server deserializes it today, but add an explicit comment that it is reserved for forward client-admission policy and currently not enforced by the hub.
- Preserve the public client boundary in `botster-hub-client` and the production route through `src/daemon_transport.rs` and `HubClientApi`.
- Keep changes surgical and trace every changed line to compatibility diagnostics, compile-checked docs, or required plan artifact evidence.

## Non-Scope

- Do not edit `botster-tui` or `botster-web`.
- Do not expand the protocol with new features, version negotiation modes, compatibility matrices, optional configurability, or broad compatibility shims.
- Do not move compatibility DTOs out of `botster-hub-client`.
- Do not change terminal streaming, session worker frames, Lua plugin runtime, Project Pipelines plugin behavior, or browser/TUI UI surfaces.
- Do not mutate real Botster identity, durable operator state, local fingerprints, keys, or user-specific paths.

## Botster Layers Touched

- Rust hub client protocol crate: `crates/botster-hub-client`.
- Rust hub daemon socket adapter: `src/daemon_transport.rs`, only if production status/hello mapping needs a narrow adjustment.
- Rust test-support/conformance crate: `crates/botster-hub-test-support`, only if conformance assertions need to verify non-default compatibility evidence.
- Docs: `docs/client-protocol.md` and rustdoc examples in public API docs.
- Test harness: Rust unit/doctest/integration tests; no browser, Rails, TUI, or plugin fixture harness expected.

## Affected Surfaces And Files

- `crates/botster-hub-client/src/lib.rs`
  - Public DTOs and helpers: `DaemonHello`, `DaemonHelloAck`, `DaemonStatus`, `DaemonCompatibility`, `DaemonCompatibilityRequirement`, `connect_and_hello_with_requirement`, `read_frame`, `read_frame_from_reader`, `DaemonTransportError`.
  - Add or adjust tests for missing compatibility in hello ack/status fixture JSON and diagnostics.
  - Add rustdoc examples that compile against the public API. Examples that need no live socket can use `no_run` for connection attempts; pure requirement construction can run normally.
- `src/daemon_transport.rs`
  - Confirm hello ack and status production paths emit `DaemonCompatibility::current()`.
  - Keep production route unchanged unless old-hub diagnostic mapping needs a public helper used by this adapter.
- `crates/botster-hub-test-support/src/lib.rs`
  - Existing conformance already validates `DaemonRequest::Status` compatibility. Touch only if needed to strengthen non-default descriptor evidence or align with the new missing-field diagnostic.
- `docs/client-protocol.md`
  - Update prose to point at compile-checked rustdoc examples instead of standalone unchecked snippets, or mirror examples into rustdoc.
- `tests/hub_daemon_lifecycle_test.rs`
  - Existing tests around external client hello/status compatibility may need one focused assertion if live production evidence is not already strong enough.

## Assumptions And Unknowns

- Assumption: old/pre-compatibility hubs are represented by valid daemon JSON frames that lack `compatibility`, not by completely unrelated wire protocols. Completely invalid JSON should remain a JSON/protocol error.
- Assumption: for hello ack and status responses, missing `compatibility` should not deserialize to defaults because that hides missing runtime evidence. It should map to a compatibility error with an explicit diagnostic.
- Assumption: `DaemonHello.compatibility` can remain on the client hello frame as a reserved forward-compatibility field because removing it would be a needless wire-shape churn and the current server accepts it.
- Unknown: whether the cleanest implementation is custom deserialization for `DaemonHelloAck`/`DaemonStatus`, a `read_frame` wrapper for compatibility-bearing frames, or serde-level field validation. Implementer should choose the smallest approach that preserves normal JSON errors outside compatibility-bearing responses.
- Unknown: whether status path old-hub behavior can be proven entirely with fixture-level deserialization or should also use a local fake socket server. Prefer fixture-level for old response shape plus existing live daemon tests for current production evidence.
- No human question is currently blocking. The ticket wording permits "where practical" and "equivalent fixture-level deserialization case", so the plan can choose fixture-level old-hub proof without waiving acceptance.

## Risks

- Serde defaults could continue masking missing compatibility and make a pre-compat hub appear current. Tests must reject that by proving missing fields become `DaemonTransportError::Compatibility`.
- Over-broad error mapping could turn ordinary malformed JSON into compatibility errors. Keep mapping limited to missing `compatibility` on compatibility-bearing hello/status frames.
- Removing or renaming public DTO fields could break downstream first-party clients. Preserve public behavior needed by TUI/web by keeping API names stable.
- Doctests that open real sockets can be flaky. Use compile-checked examples that either construct requirements only or mark connection attempts as `no_run`.
- Rustdoc links can pass tests but warn under feature combinations. Avoid fragile intra-doc links or run doc checks with warning denial if links are added.
- Real daemon integration tests are slower and serialized in this repo. Keep new live tests minimal and prefer existing coverage unless production evidence is missing.
- Checklist persistence timeout already occurred; gate evidence must duplicate workflow evidence so review is not blocked by missing checklist rows.

## Acceptance Checks And Tests

- `cargo test -p botster-hub-client`
  - Covers compatibility validation and missing hello/status compatibility fixture diagnostics.
- `cargo test -p botster-hub-client --doc`
  - Proves public compatibility examples compile against the real public API.
- `cargo test --test hub_daemon_lifecycle_test external_hub_client_crate_drives_real_daemon_socket_protocol`
  - Proves the production daemon socket path still emits current compatibility descriptors through the public client crate.
- `cargo test --test hub_daemon_lifecycle_test daemon_status_exposes_same_compatibility_descriptor_as_hello`
  - Proves status and hello compatibility descriptors stay aligned if this existing test remains available.
- `cargo test --test hub_client_api_test`
  - Guards transport-neutral local client behavior if daemon status mapping is touched.
- `./test.sh` or the repo's accepted full wrapper should be run by Implement/Verify when feasible, because existing lifecycle/conformance coverage is broad and repo-local.
- `cargo clippy --all-targets --all-features -- -D warnings`
  - Required if Rust changes introduce or touch warnings; failures must be attributed to touched vs baseline files.
- Optional docs warning gate if rustdoc links are added: `RUSTDOCFLAGS="-D warnings" cargo doc -p botster-hub-client --no-deps`.

## Production Path To Prove

- Current client calls `botster_hub_client::connect_and_hello_with_requirement`.
- The helper writes `DaemonHello`, reads `DaemonHelloAck`, validates `ack.compatibility`, and returns `DaemonTransportError::Compatibility` on mismatch.
- The real daemon production route is `src/daemon_transport.rs` `serve_daemon` -> `handle_connection` -> `DaemonHelloAck { compatibility: DaemonCompatibility::current() }`.
- Status goes through `DaemonRequest::Status` -> `handle_runtime_control_request` -> `HubClientApi::handle_request` -> `daemon_status_from_status` -> `DaemonStatus { compatibility: DaemonCompatibility::current() }`.
- The old-hub diagnostic proof should exercise the same public client deserialization/error path used before clients enter session or terminal operations, or document precisely why fixture-level proof is intentionally enough for pre-compatibility responses.

## Pipeline Gates And Artifacts

- Plan gate evidence should point to this plan artifact and include the checklist fallback evidence because checklist creation timed out.
- Plan Review should verify that the plan is surgical, keeps compatibility ownership in `botster-hub-client`, and has an acceptance path for both doctests and old-hub diagnostics.
- Implement gate should require committed code changes and command evidence, not only prose.
- Verify should re-run the exact commands against the live worktree and attribute any baseline failures to untouched files.

## Vault Gaps Worth Capturing

- Potential capture if implementation settles a reusable pattern: "compatibility-bearing daemon frames must reject missing evidence instead of serde-defaulting descriptors."
- Potential capture if the best design is a public helper around compatibility-frame reads: record the helper boundary so future protocol fields do not duplicate ad hoc JSON mapping.
- No convention conflict found. The plan follows the loaded notes: compatibility descriptors stay in `botster-hub-client`, docs proof uses rustdoc doctests, live client tests use the public daemon socket boundary, and no TUI/web edits are planned.
