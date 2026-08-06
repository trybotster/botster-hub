# Hub Session Types Implementation Report

## Routing and authority

- Target repository: `trybotster/botster-hub`
- Target ID: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Pipeline run: `run_1785970246_525412`
- Approved plan: `docs/plans/make-session-types-flexible-editable-authoritative.md`
- Rebased target authority: `origin/main@73d5c13bb5930205b102cb28cffb7b9e40ce2699`
- Implementation commit: `802b511ece1324fc69fda157494f006b6aa736c0`
- Review follow-up implementation commit: `8e3eb33`
- Qualified-ID follow-up implementation commit: `bd794ce`
- Required Core dependency: `33ebcd98d19031d23e91b03d8da0ee3f8d1410d4`

## Guidance applied

- `implementer-playbook`
- `botster-implementer-playbook`
- `botster-hub-playbook`
- `botster-runtime-reviewer-playbook`
- `botster-package-reviewer-playbook`
- `botster-hub-client-playbook`
- `project-pipelines-playbook`
- The targeted atomic notes cited by the approved plan for cold migrations,
  client feature registration, persistence ownership, runtime proof, package
  publication identity, and downstream-shaped verification
- Repository skill guidance for Botster Hub customization and the knowledge-vault
  checklist workflow

No convention conflict was found. The implementation keeps Hub policy and
definitions in Hub, leaves Core taxonomy-free, uses the repository-owned test
wrapper, and performs a cold migration with no aliases or dual readers.

## Implementation

The Hub now owns one flexible session-type definition whose presentation fields
(`label`, optional `description`/`icon`) and semantic fields (`role`,
`interaction`, `traits`, `lifecycle`) are independently validated rather than
encoded as a closed taxonomy. Package, device, and admitted-repository sources
share precedence and provenance rules. Device and repository sources support
source-explicit create/update/delete; package definitions are typed read-only.
Repository writes remain confined to admitted roots and
`.botster/session-types.json`.

Definition changes and spawn-target admission changes advance a durable
generation and drive a first-class `session_type` entity-frame family. Package
install/update/reload/refresh,
enable, disable, and remove compare effective maps and advance the generation
only for observable changes. Overflow resynchronizes with a full snapshot and
fresh connections receive the current authoritative generation. A resync
snapshot that exceeds the daemon frame limit is replaced by a bounded
`entity_error` with the typed `entity_provider_frame_too_large` code and the
subscription is closed.

List, show, target-scoped Lua show, and resolve/spawn share the same effective
row projection, including lower-precedence source identities and override
diagnostics. Both bare IDs and fully-qualified `session_type_id` values retain
that projection: the query-matched subset selects the winner, while all sources
with the winner's bare ID supply lower-precedence diagnostics. CRUD persistence
mutates a freshly loaded durable state, so an unrelated write made after
runtime construction is preserved.

Materialized spawns attach opaque `botster.session_type.*` metadata to the Core
spawn contract. The canonical session entity projection reads that metadata
without inferring classification for ordinary sessions, including after Hub
restart and Core worker adoption.

The daemon/client/CLI/Lua vocabulary is cold-renamed to session types. Protocol
6 uses exact protocol-version agreement and requires
`session_type_entity_subscriptions`. Review found that the TypeScript generator
had hard-coded the `type` discriminator for Rust unions tagged with `source`
and `policy`; the generator and serde-shaped tests now carry the discriminator
explicitly. Conformance revision 31 and npm coordinate
`@trybotster/hub-test-support@0.1.24` were allocated for the corrected bytes.
The earlier 30/0.1.23 meaning is not reused. These coordinates follow the
rebase of the sibling identity ticket, whose protocol 5/revision 29/package
0.1.22 repository meaning was preserved. The published registry remained at
0.1.21 during the final collision check.

## Files changed

- Hub authority/runtime: `src/session_types.rs` (replacing
  `src/session_templates.rs`), `src/client_api.rs`, `src/daemon_transport.rs`,
  `src/runtime.rs`, `src/persistence.rs`, `src/packages.rs`, `src/lib.rs`,
  `src/main.rs`, `src/lua_runtime.rs`, `src/local_webrtc.rs`,
  `src/operator_console.rs`, and `src/profile.rs`
- Client and generated contract: `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`, and
  `crates/botster-hub-client/generated/daemon-protocol.ts`
- Test support and published assets: `crates/botster-hub-test-support/src/lib.rs`
  and the versioned files under `packages/hub-test-support/`
- First-party workflow/docs: `examples/project-pipelines/`, `README.md`,
  `docs/client-protocol.md`, `docs/lua-plugin-abi.md`,
  `docs/loaded-daemon-lifecycle-runner.md`, and
  `script/run-loaded-daemon-lifecycle`
- Proof: `tests/hub_client_api_test.rs`,
  `tests/hub_daemon_lifecycle_test.rs`, `tests/hub_lua_runtime_test.rs`, and
  `tests/hub_mcp_test.rs`
- Dependency pin and plan: `Cargo.lock` and the approved plan named above

## Ownership boundaries and cross-repository work

Hub owns definition semantics, source admission, CRUD, persistence, metadata
construction, entity routing, and daemon/CLI operator surfaces. Core receives
only opaque metadata and generic lifecycle mechanisms. Hub-client owns wire DTOs
and generated TypeScript. Project Pipelines changes are limited to its
first-party Hub package fixture and renamed Lua calls.

No out-of-bound repository was edited. The required Core metadata support was
delivered separately and is pinned at the commit above. Web and TUI adoption
remain separately routed downstream work; this run only publishes their Hub
contract authority.

Review identified two installed first-party packages that still use the
removed vocabulary. Separate dependent tickets were registered:

- Project Pipelines `ticket_1785984129_564146`, target
  `tgt_a72ca1a83d504385b8648f71409119ab`, project target
  `project_target_1785985435_859966`
- Workspaces `ticket_1785984128_479155`, target
  `tgt_71266a8d976d4535902ffed09c18a7ba`, project target
  `project_target_1785985436_995496`

Both depend on this Hub ticket and cover manifest keys, capability scopes, the
Lua capability table, and `session_type_id`. This run does not edit either
downstream repository and adds no compatibility alias.

## Deviations and assumptions

- The target branch advanced during implementation when the Hub identity/update
  ticket merged. The work was rebased before handoff and compatibility/package
  coordinates were reallocated from the superseded branch-local 5/29/0.1.22 to
  6/30/0.1.23. This is required collision handling, not a product-plan change.
- Review then found incorrect generated discriminators in the newly allocated
  30/0.1.23 artifact. Corrected bytes were reallocated to 31/0.1.24 rather than
  changing an existing meaning.
- The session-type definition lane uses full upserts for changed rows rather
  than sparse patches; the approved plan left that wire choice open.
- Lua keeps the package-needed list/show/spawn/managed-worktree capability.
  Definition CRUD is deliberately an operator daemon/CLI surface because
  packages are read-only and plugin-granted writes were not required.
- Assumption: repository definition files are changed through Hub-authorized
  operations. Direct out-of-band edits are not treated as an authority path.

## Verification and downstream proof

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`
- `npm test --prefix packages/hub-test-support`
- `cargo test -p botster-hub-client --lib` — 49 unit tests passed, including
  actual serde JSON versus generated discriminator and field checks
- `cargo test -p botster-hub-test-support` — 42 unit tests and 3 doctests passed
- `./test.sh --test hub_client_api_test -- --test-threads=1` — all 26 client
  integration tests passed, including bare/qualified lookup equality and
  preserved ambiguous-bare-ID rejection
- `./test.sh --test hub_lua_runtime_test` — all 26 Lua integration tests passed,
  including identical bare and fully-qualified show projections
- `./test.sh session_type` — persistence, 9 Hub client, 3 real daemon, and 5
  Lua session-type tests passed on the rebased target
- Bare `./test.sh` — 173 library tests, 13 CLI tests, 14 capability tests, 26
  client tests, 129 daemon lifecycle tests (one documented large local test
  ignored), 1 local-runtime test, 26 Lua tests, 7 MCP tests, 7 plugin-lifecycle
  tests, 7 runtime tests, 2 conformance tests, and doctests passed
- Real Hub/Core restart proof used
  `target/debug/botster-hub` and `target/debug/botster-session-worker`, spawned a
  repository session type, observed canonical metadata, restarted Hub, adopted
  the Core worker, and observed the same classification without inference
- Definition entity proof observed device CRUD and package enable/disable
  upserts with durable generations, package read-only rejection, and reconnect
  snapshots without polling
- A held-open real socket subscription observed an admitted repository
  definition upsert immediately after `CreateSpawnTarget` and its remove after
  `DeleteSpawnTarget`, at consecutive durable generations without polling or
  reconnecting
- List, show, resolve, and real Lua show proof assert identical override sources
  and diagnostics for the same effective definition using both its bare ID and
  the fully-qualified `session_type_id` returned to clients
- A stale-runtime persistence regression writes an unrelated spawn target after
  runtime load and proves subsequent session-type CRUD preserves it
- Oversized session-type resync proof receives the bounded typed error frame and
  verifies the unsendable snapshot is never queued
- Orthogonality proof accepted interactive agent, interactive accessory, and
  service accessory definitions independently
- Ablation: removing Core metadata projection made the restart proof fail with
  absent `session_type_id`; restoring it passed
- Ablation: bypassing package-source immutability made the CRUD proof panic at
  the unreachable mutation path; restoring the typed read-only guard passed
- Published 0.1.21 remains the registry coordinate and repository 0.1.22 remains
  the baseline coordinate. The final 0.1.24 tarball was installed in a clean
  external npm consumer and asserted protocol 6/revision 31, exact `source` and
  `policy` discriminators, the bounded entity-error DTO, checksums, and absence
  of the incorrect `type`-tagged session-type union shapes
- Final cold search found old persisted keys only inside the intentional schema
  v2 rejection fixture in `src/persistence.rs`

## Residual risk and unverified behavior

- The npm artifact was packed and installed externally but was not published;
  publication remains a release action outside this implementation step.
- Web/TUI, Project Pipelines, and Workspaces consumers were not edited or
  executed because their repositories require separately routed tickets. Their
  consumable Hub contract was proven through the packed external Node artifact.
- Failure injection across the repository definition-file write and subsequent
  Hub generation-state write was not exercised. Each individual file write is
  atomic and normal/restart paths are covered, but a forced filesystem failure
  between those two durable resources remains an untested operational edge.

## Missing vault guidance and durable capture

No missing or conflicting vault guidance was discovered. The collision-safe
rebase decision, source authority, cold compatibility boundary, runtime proof,
and residual risk are captured durably in this report and the updated repository
protocol documentation; no new general vault note was necessary.
