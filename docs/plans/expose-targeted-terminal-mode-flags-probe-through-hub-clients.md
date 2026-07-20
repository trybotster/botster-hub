# Expose targeted terminal ModeFlags probe through hub clients

## Context loaded

- Pipeline context: ticket `ticket_1784566294_391171`, run
  `run_1784576823_125810`, active Plan step `botster_plan`, required gate
  `botster_plan_gate`, no prior artifacts/reviews/findings/questions/answers, and
  one closed dependency on the core ModeFlags substrate.
- Dependency proof: botster-core PR #106, “Ship authoritative terminal
  ModeFlags readback through core runtime,” is merged on core `main` at
  `7ce1f705952407a1e4f76bcc83cbc6da2efc7efb`. This checkout's `Cargo.lock`
  still pins botster-core crates at pre-dependency revision
  `84c2ff20f3607ff24fb87d196e132c54365c31c5`.
- The merged core API is
  `CoreDaemon::read_mode_flags(ReadModeFlagsRequest) ->
  Result<ReadModeFlagsResult, CoreDaemonError>`. The result carries correlated
  `ModeFlagsReady { request_id, session_id, mode_flags }`. Only the existing
  `ModeFlags.mouse_mode: u8` is authoritative in this revision. Unsupported
  backends and backend query failures are errors, never authoritative defaults.
- Repo path traced:
  `DaemonRequest -> src/daemon_transport.rs -> HubClientRequest ->
  src/client_api.rs -> HubRuntime -> CoreDaemon`. Existing `ReadScreen` and
  `CaptureSnapshot` are the direct production-path precedent.
- Repo artifact precedent: current Plan artifacts live under `docs/plans/`;
  the new plan follows that mainline convention.
- Vault/playbook context:
  [[planner-playbook]], [[botster-planner-playbook]],
  [[botster-architecture]], [[cli-patterns]], [[spa-patterns]],
  [[project pipeline orchestration belongs in a device-level botster plugin]],
  [[project pipelines needs an operator workbench not more primitives]],
  [[project pipelines ui contract belongs in the plugin readme]],
  [[botster orchestration should spawn agents with explicit target ids]],
  [[botster orchestration prompts must bind agents to explicit worktrees]],
  [[botster pipeline needs continuous product owner between agent steps]],
  [[coredaemon must expose terminal truth used by the production hub path]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub client crate is the external client boundary]],
  [[synced state types are allowed while pushed event variants are forbidden]],
  [[generated typescript dtos must encode serde field optionality]],
  [[stale project pipeline worktrees can miss merged dependency apis]], and
  [[test script required for rust tests not cargo test]].

## Scope

Botster layers touched: Rust hub runtime and client API, daemon socket
transport, the external `botster-hub-client` protocol crate and generated
TypeScript surface, hub test support/conformance assets, and public protocol
documentation.

1. Refresh the three botster-core git dependencies in `Cargo.lock` to a core
   revision at or after the verified PR #106 merge, then consume the exported
   `ReadModeFlagsRequest` and `ReadModeFlagsResult` types. Do not copy or
   recreate the core implementation in hub.
2. Add the smallest `HubRuntime::read_mode_flags` facade alongside
   `read_screen` and `capture_snapshot`. It constructs the core request with
   the caller's request id, target session id, and logical timestamp and returns
   the core result unchanged on error.
3. Extend `HubClientRequest`, `HubClientOperation`, admission, request-id
   extraction, `HubClientApi::handle_request`, and `HubClientResponseBody` with
   one targeted read operation. Project the core result into one typed hub
   client DTO containing the response's authoritative `session_id` and
   `mouse_mode: u8`.
4. Route the public daemon request through `src/daemon_transport.rs`, exactly
   parallel to `ReadScreen` and `CaptureSnapshot`, and project one typed daemon
   response body. Runtime errors must continue through the existing structured
   operator-error conversion without closing the connection or substituting
   `mouse_mode = 0`.
5. Extend `botster-hub-client` serde DTOs and all exhaustive maps/examples:
   request variant, response optional body, response kind, request wire-name
   map, response wire-name map, serialization examples, and public re-exports.
   Use the core vocabulary and existing serde convention:
   `ReadModeFlags` / `read_mode_flags`. The response DTO must preserve session
   attribution and the exact `u8` value.
6. Extend `src/typescript.rs`, regenerate
   `crates/botster-hub-client/generated/daemon-protocol.ts`, and sync the
   generated npm test-support copy. The response body property is optional
   because Rust skips an absent `Option` during serialization; `mouse_mode`
   itself is a required TypeScript `number`.
7. Add a deterministic public conformance scenario/fixture for:
   - mouse reporting off (`mouse_mode == 0`);
   - exact combined on value (`mouse_mode == 9`, not a boolean);
   - response session attribution;
   - unknown-session operator error;
   - backend failure/unsupported operator error with no successful default
     response.
   Update the Rust test-support generator, checked Node package assets,
   metadata/exports/read helpers, package tests, and fixture documentation.
   Increment `CONFORMANCE_FIXTURE_REVISION` once (from 14 to 15) because the
   public request/response and published fixture contract change; keep
   `PROTOCOL_VERSION` at 1, matching the existing additive readback precedent.
8. Update the root product/readback table and `docs/client-protocol.md` to name
   the targeted mode probe, its exact DTO semantics, error behavior, fixture,
   revision rationale, and the fact that it is request/response only.

## Non-scope

- No `ModeChanged`, `TerminalModeChanged`, or other pushed daemon/session event.
- No general terminal capability registry, feature-negotiation redesign, or new
  client feature constant solely for this one additive operation; the existing
  `terminal_readback` feature remains the compatibility umbrella.
- No `UiNode`/`terminal_view` schema or property changes.
- No botster-tui, botster-tui-kit, React/browser behavior, entity-frame, or
  terminal renderer changes.
- No terminal byte parsing, Ghostty ABI work, alternative mouse-mode encoding,
  or changes to the existing `ModeFlags` structure.
- No claims that the other `ModeFlags` booleans are authoritative.
- No compatibility alias, alternate discriminator, version-suffixed DTO, or
  dual old/new wire path.
- No npm publication in this ticket. Checked package assets and version/revision
  metadata must be internally consistent, but registry release is separate
  work unless the ticket is explicitly expanded.

## Assumptions and unknowns

- Assumption: `read_mode_flags` is the wire discriminator. This follows the
  merged core API and the established snake-case mapping for `read_screen` and
  `capture_snapshot`; no alias is needed.
- Assumption: the daemon response uses a dedicated optional mode-read body and
  response kind, parallel to existing targeted readback responses. Exact Rust
  DTO type names may follow local naming (`HubClient...` / `Daemon...`) but the
  serialized contract must remain `session_id` plus required `mouse_mode`.
- Assumption: mode readback remains under existing runtime admission and
  `terminal_readback` compatibility rather than adding a new capability bit.
- Assumption: updating the lockfile to core PR #106 is an intended prerequisite,
  not unrelated dependency churn. The lockfile diff should contain only the
  core git revision/dependency consequences required to compile this API.
- Assumption: the target and worktree are already pinned by Project Pipelines:
  target `tgt_7e208a0c76a44980a83b63af976b1f22`, this ticket worktree, and branch
  `project-pipelines/ticket_1784566294_391171`.
- Unknown: the current hub runtime does not expose a production backend
  injection seam for forcing a Ghostty query failure. Prefer a narrow test at
  the existing error-conversion boundary plus the deterministic public
  conformance error fixture. Add no production configurability solely for this
  test. If actual propagation cannot be proven without changing a public
  runtime boundary, stop and ask rather than inventing that seam.
- Unknown: the checked Node package is currently version `0.1.7`. Because
  publication is non-scope, do not silently claim a published new version.
  Keep generated asset metadata internally consistent and leave publication
  coordinates to a release ticket.

## Affected surfaces/files

Required production surfaces:

- `Cargo.lock`
- `src/runtime.rs`
- `src/client_api.rs`
- `src/daemon_transport.rs`
- `src/lib.rs`
- `src/main.rs`
- `crates/botster-hub-client/src/lib.rs`
- `crates/botster-hub-client/src/typescript.rs`
- `crates/botster-hub-client/generated/daemon-protocol.ts`

Required test/conformance surfaces, subject to the existing generator's exact
asset naming:

- `tests/hub_client_api_test.rs`
- `tests/hub_daemon_lifecycle_test.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `crates/botster-hub-test-support/examples/node_package_assets.rs`
- `tests/hub_test_support_conformance_test.rs`
- `packages/hub-test-support/scripts/sync-assets.mjs`
- `packages/hub-test-support/index.js`
- `packages/hub-test-support/package.json`
- `packages/hub-test-support/test.mjs`
- `packages/hub-test-support/metadata.json`
- `packages/hub-test-support/first-party-client-support-matrix.json`
- `packages/hub-test-support/daemon-protocol.ts`
- one generated mode-flags conformance JSON fixture under
  `packages/hub-test-support/`
- `packages/hub-test-support/README.md`

Required public docs:

- `README.md`
- `docs/client-protocol.md`

Files should be omitted if inspection proves they are generated indirectly and
unchanged; no adjacent cleanup is authorized.

## Implementation sequence

1. Update the core lock revision and compile against the merged API before
   editing hub contracts. Confirm `ReadModeFlagsRequest` and
   `ReadModeFlagsResult` resolve from the locked dependency.
2. Wire the internal production path from `HubRuntime` through
   `HubClientApi`. Add focused API tests that assert exact 0/9 values and the
   response's target session id, not merely variant presence.
3. Wire daemon transport and public client DTOs/maps. Add a real daemon-socket
   test that sends the new public request, verifies kind/body/session/value,
   verifies unknown-session operator error, and verifies the same connection
   remains usable afterward.
4. Add a focused error-projection regression for unsupported/backend failure.
   Assert the response is an operator error and contains no mode body; never
   accept a default-valued success. Use an existing test boundary rather than
   widening production runtime construction.
5. Extend TypeScript generation and serde drift tests, regenerate the
   authoritative artifact, then extend/sync hub-test-support fixtures and Node
   package assets. Bump only the conformance fixture revision.
6. Update public docs, format, and run focused then workspace gates. Confirm
   the committed diff introduces no pushed mode event; the exhaustive
   `DaemonEvent` match plus serde/TypeScript drift tests remain the contract
   guard.

## Risks

- **False authority:** converting unsupported/backend failure into
  `ModeFlags::default()` would make `0` indistinguishable from authoritative
  mouse-off. Preserve errors end to end and assert absence of a success body.
- **Value collapse:** treating `mouse_mode` as a boolean loses the existing
  bitmask/combined value. Tests must assert exact `0` and `9`.
- **Session misattribution:** request correlation and session attribution can
  be dropped during core -> hub -> daemon projection. Assert the returned
  `session_id` at both hub API and public socket boundaries.
- **Exhaustive enum drift:** request/response variants require updates in
  operation admission, request id extraction, CLI rendering, serializer maps,
  examples, and TypeScript unions. Workspace strict clippy and drift tests
  should catch missed matches.
- **Generated artifact drift:** changing Rust DTOs without regenerating both the
  crate artifact and checked npm copy would publish a stale client contract.
  Generate from Rust and verify exact checked copies; do not hand-edit them.
- **Fixture compatibility drift:** additive protocol DTOs plus new fixture
  bytes require one conformance revision bump. Failing to update metadata,
  support matrices, hashes, exports, and docs together leaves downstream
  clients on conflicting authority.
- **Dependency over-update:** a broad `cargo update` could introduce unrelated
  lockfile churn. Restrict and review the core dependency update.
- **Test-only abstraction pressure:** forcing backend failure through a new
  public runtime injection layer would exceed this surgical ticket. Use the
  narrowest existing error boundary or ask if none can prove propagation.

## Acceptance checks/tests

Focused behavior and contract checks:

1. `./test.sh --test hub_client_api_test read_mode_flags`
   - authoritative off and exact combined-on values;
   - response session attribution;
   - no defaulting on errors.
2. `./test.sh --test hub_daemon_lifecycle_test read_mode_flags`
   - real `botster-hub-client` request through daemon transport, client API,
     runtime, and locked CoreDaemon;
   - unknown session returns `operator_error` with operation
     `read_mode_flags`, the connection survives, and no mode body exists;
   - backend/unsupported failure remains an operator error without a successful
     zero value.
3. `./test.sh -p botster-hub-client`
   - serde round trips and exact request/response wire-name maps include
     `read_mode_flags`;
   - TypeScript union/body/interface contains required `session_id` and
     `mouse_mode: number`, with only the optional response envelope field
     marked optional;
   - committed generated TypeScript equals generator output.
4. `./test.sh -p botster-hub-test-support`
   - typed fixture and generated JSON assert exact 0/9, attribution, unknown
     session, and backend failure without a successful default body.
5. `./test.sh --test hub_test_support_conformance_test`
   - Rust fixtures, metadata, support matrix, and checked Node assets agree.
6. `npm test --prefix packages/hub-test-support`
   - package exports/readers, protocol tokens, fixture values, metadata,
     revision, and asset hashes pass.

Repository gates:

7. `cargo fmt --all --check`
8. `./test.sh`
9. `cargo clippy --workspace --all-targets --all-features -- -D warnings`
10. `git diff --check`
11. Inspect the committed diff for:
    - no `ModeChanged`/pushed mode event;
    - no alternative mode registry, `UiNode`, TUI, or parser changes;
    - no `unwrap_or_default`/fabricated `mouse_mode` on the read path;
    - generated artifacts derived from source;
    - no absolute worktree/vault paths or other PII in committed artifacts.

The runtime-path proof is the real daemon-socket test in check 2. DTO/source
existence alone does not satisfy the ticket.

## Project Pipelines and vault checklist evidence

- Checklist instructions were loaded before planning.
- The initial vault-checklist create call returned the known plugin-worker
  timeout, but listing reconciled the persisted run checklist as
  `checklist_1784576918_867623`; do not create a duplicate.
- Vault constraint result: no convention conflict. The plan uses the hub-owned
  `HubRuntime` facade over CoreDaemon, keeps the narrow
  `botster-hub-client` external boundary, preserves request/response probing
  without pushed events, and uses repo-visible generated/conformance assets.
- Planned verification is listed above and uses the repo wrapper for tests plus
  workspace strict clippy.
- Durable capture disposition is below; the checklist should be updated with
  these facts before gate submission.

## Vault gaps worth capturing

No new durable architectural gap is known at Plan time. Existing notes already
cover the core/hub terminal-truth seam, hub-client boundary, exact
request/response rather than pushed mode events, serde/TypeScript optionality,
stale dependency worktrees, generated fixture authority, and checklist timeout
reconciliation.

If implementation proves that hub cannot exercise a core backend error without
adding a production injection seam, capture that specific testability boundary
as a vault candidate after the concrete evidence is known. Do not write a
speculative note now.
