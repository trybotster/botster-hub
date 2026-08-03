# Admit package-owned entity families: implementation report

## Target and guidance

- Target repository: `botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket: `ticket_1785725323_447673`
- Run: `run_1785725358_263870`
- Repository playbooks: [[botster-hub-playbook]] and the in-workspace [[botster-hub-client-playbook]].
- Role/workflow playbooks: [[implementer-playbook]], [[botster-implementer-playbook]], and [[project-pipelines-playbook]].
- Applied targeted notes: [[botster hub is a first party host profile over core]], [[botster hub client compatibility descriptors belong in client crate]], [[adding a hub client feature constant is a three site change]], [[daemon event shape changes bump conformance fixture revision not protocol version]], [[generated typescript dtos must encode serde field optionality]], [[hub generated protocol changes are a four site release chain]], [[conformance fixture revisions must be unique per published content]], [[plugin surface handlers must validate against hub locked uinode contract]], [[plugin dynamic ui lists bind to plugin-owned entities]], [[plugin-owned dynamic state uses plugin-namespaced entity frames]], [[plugin query providers match snapshot read model shape]], [[plugin tests must prove worker boundaries not hub leakage]], and [[test script required for rust tests not cargo test]].
- Human namespace decision: `question_1785728138_907576`.
- Approved revised plan: `artifact_1785728851_913690` and `docs/plans/admit-package-owned-entity-families.md`.

## Outcome and assumptions

Hub now admits exact entity families declared by an enabled Lua package, validates package-owned UiNode bindings and action replacements, queries the provider through its isolated worker for every subscribe/reconnect, and transports validated generic JSON records through the existing bounded socket and local WebRTC entity-frame paths. `/session` remains Hub/Core-authoritative and its typed `DaemonSessionEntity` projection remains available.

The canonical v1 owner mapping is Hub-owned and Core remains package-agnostic. Ordinary nonempty single-segment package IDs that do not begin with `bns1_` are unchanged. Every other package ID maps to `bns1_` plus lowercase hexadecimal of its exact UTF-8 bytes. The mapping has a canonical inverse, does not normalize Unicode, and makes the identity and encoded ranges disjoint.

Assumptions preserved from the approved plan:

- providers establish authority with one whole-family snapshot on each subscribe/reconnect;
- this ticket adds query-on-subscribe hydration, not speculative live plugin broadcast infrastructure;
- package authors declare the exact mapped protocol family in descriptors, snapshots, and UiNode paths;
- provider invocation retains the existing one-second plugin worker timeout and existing bounded daemon/WebRTC delivery limits.

## Files changed

- Hub runtime and public transport: `src/lifecycle.rs`, `src/lua_runtime.rs`, `src/runtime.rs`, `src/client_api.rs`, and `src/daemon_transport.rs`.
- Hub client contract and generator: `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, and `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Shared conformance runner/fixture: `crates/botster-hub-test-support/src/lib.rs` and `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/{README.md,botster-package.json,plugin.lua}`.
- Authoritative fixture source: `fixtures/plugins/plugin-contract-matrix/{README.md,botster-package.json,plugin.lua}`.
- Published npm mirror/release chain: `packages/hub-test-support/{package.json,README.md,test.mjs,metadata.json,daemon-protocol.ts,first-party-client-support-matrix.json,late-attach-history-conformance-fixture.json,mode-flags-conformance-fixture.json,session-lifecycle-subscription-conformance-fixture.json,session-plugin-binding-conformance-fixture.json}` and `packages/hub-test-support/fixtures/plugin-contract-matrix/{README.md,botster-package.json,plugin.lua}`.
- Tests: `tests/hub_lua_runtime_test.rs` and `tests/hub_daemon_lifecycle_test.rs`.
- Documentation and delivery records: `README.md`, `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, `docs/plans/admit-package-owned-entity-families.md`, and this report.

## Ownership boundaries and cross-repository routing

- Hub owns package identity/admission, the v1 owner-token mapping, worker registration/invocation, binding validation, bounded subscription lifecycle, cleanup, and the Hub-owned client/test-support release chain.
- Pinned Core revision `5846fc776d31e2b6c98a8d932f50a31078743901` remains unchanged and package-agnostic. Hub consumes Core `EntityContract`, `EntityFrame`, worker handler/descriptor, and resource primitives without editing or weakening Core.
- No Web, TUI, Core, or Project Pipelines repository was edited. Product adoption and the real `project-pipelines.home` durable database proof remain separately routed to blocked downstream ticket `ticket_1785635393_993057`.
- The source-level `JsonValue` consumer narrowing (not a daemon wire change) is separately routed to Web ticket `ticket_1785731573_705379` and TUI ticket `ticket_1785731574_557846`; both depend on this Hub ticket and own their repository-specific migration proof.
- The single-segment `project-pipelines` package maps identically and remains the downstream public namespace; there is no package-specific special case.

## Plan conformance and deviations

There are no deviations from the approved revised plan. The earlier raw-package-name assumption was surfaced to the human, returned through Plan/Plan Review, and superseded before this implementation resumed. The production dotted fixture uses its exact encoded family. Between its first subscription and reconnect, the runner mutates persisted package configuration through the public daemon API, reloads the worker, and proves the reconnect baseline contains generation 2.

The implementation deliberately does not add a Lua owner-token accessor. The exact public mapping and author declaration rule are documented; an accessor remains deferred as recorded by Plan Review info finding `finding_1785729053_547397`.

## Verification and downstream-shaped proof

- `cargo fmt --all -- --check` — passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — passed.
- `./test.sh` — passed after the final fixture and duplicate-record validation changes; 121 daemon lifecycle tests passed with one documented ignored local-only adversarial test, and every other unit/integration/doc suite passed.
- `./test.sh --workspace` — passed after adding `plugin_entity_subscriptions` to the Hub test-support stable support-matrix expectation; this includes all 42 sibling `botster-hub-test-support` tests and workspace doctests.
- Published support-matrix proof — `plugin_entity_frames` is now in `entity_actions.supported_capabilities` (with no stale unsupported entry), and `npm run sync` updated the matrix plus its metadata SHA under unpublished conformance revision 28.
- Namespace unit proof — exact `project-pipelines` identity, exact dotted fixture token, marker-prefixed IDs, `a.b` versus `a_b`, empty input, malformed canonical forms, Unicode round-trip, canonically distinct Unicode byte strings, and pairwise uniqueness passed.
- Lua/provider contract proof — raw dotted owner, foreign encoded owner, malformed marked owner, reserved `session`/`workspace` families, duplicate snapshot IDs, and post-unload provider lookup reject; the positive path also asserts the provider receives the mapped `entity_type` and active `subscription_id`.
- `daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts` — passed through real install/enable, worker render, exact encoded binding, generic socket subscribe, persisted configuration mutation, worker reload, and fresh authoritative reconnect snapshot.
- `daemon_package_entity_provider_streams_fresh_authoritative_reconnect_snapshot` — passed identity mapping, public held-open socket subscription, reconnect, live/high-water counter accounting, held subscription closure on disable, and post-disable admission rejection.
- `local_webrtc_chunks_oversized_encrypted_daemon_response` — passed exact mapped generic provider snapshots and reconnect through the production encrypted local WebRTC adapter.
- `npm install --no-package-lock`, `npm run sync`, and `npm test` in `packages/hub-test-support` — passed; no lockfile was introduced.
- Actual `npm pack` for `@trybotster/hub-test-support@0.1.21` — passed with 16 expected files. Tarball SHA-256: `58d474633f8a1ab65ce05a6718eb5fb25da94de62e82e6e1f98cdf1efac97dd4`. The packed bytes contain protocol version 4, conformance revision 28, advertised and required `plugin_entity_subscriptions`, generic `JsonValue` entity records, the exact dotted owner token, and the `entity_provider` fixture.
- Live harness source checkout before commit: Hub base `3389a81b6fb4862d643b203a94c864287c85b3b6`; locked Core `5846fc776d31e2b6c98a8d932f50a31078743901`.
- Harness binaries: `target/debug/botster-hub` and `target/debug/botster-session-worker`, built in the ticket run worktree.

## Residual risk and unverified behavior

- Registry publication is intentionally not performed premerge. A credentialed human must publish the merged `0.1.21` coordinate, after which the pipeline must externally pack and verify registry bytes before the downstream ticket is unblocked.
- Web and TUI repository-specific rendering/adoption is not verified in this Hub run; tickets `ticket_1785731573_705379` and `ticket_1785731574_557846` own the source-level consumer narrowing. Their shared wire shape is proved here through generated TypeScript, npm assets, conformance materialization, socket delivery, and production local WebRTC delivery.
- Live plugin mutation fanout is not implemented. A new subscription/reconnect always queries authoritative provider state; later live deltas remain outside this ticket.

## Vault guidance and durable capture

No applicable convention conflict remains, and no Rails guidance applies to this Rust/Lua task. Existing vault notes did not define a collision-free mapping for dotted package IDs; that missing guidance was discovered during implementation and resolved by the human decision plus Plan Review. The durable rule is captured in the approved plan, `docs/lua-plugin-abi.md`, `docs/client-protocol.md`, fixture README, and this report. No separate vault note was added because the exact versioned ABI is repository-owned documentation whose bytes must evolve with this release chain rather than a duplicated global convention.
