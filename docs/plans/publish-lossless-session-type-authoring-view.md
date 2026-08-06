# Publish a lossless session-type authoring view

## Target and context

- Target repository: `trybotster/botster-hub` (`botster-hub`), resolved from the run
  target rather than the process directory.
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Ticket/run: `ticket_1786039258_173310` / `run_1786042047_208281`.
- Repository charter: [[botster-hub-playbook]].
- Role context: [[planner-playbook]], [[botster-planner-playbook]].
- Surface overlay implicated by the changed files: [[botster-runtime-reviewer-playbook]]
  (daemon request/response and client-API surfaces). No package/plugin manifest,
  Lua ABI, or Project Pipelines workflow path changes, so
  [[botster-package-reviewer-playbook]] and [[project-pipelines-playbook]] are not
  loaded.
- Targeted atomic notes: [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  [[conformance fixture revisions must be unique per published content]],
  [[adding a hub client feature constant is a three site change]],
  [[published capability matrices must derive enumerations from source]],
  [[botster first party client support matrices belong in hub test support]],
  [[hub test support npm releases need external consumer smoke]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[session template override sources use package device repo explicit precedence]],
  [[botster hub client state sync is entity frame only]],
  [[device hub owns admitted spawn targets not ambient repo cwd]],
  [[cold turkey migrations eliminate dual code paths and version suffixes]],
  [[prefer framework and library components over custom solutions]],
  [[test script required for rust tests not cargo test]],
  [[rust repo strict lints must be verified before dismissing warnings]],
  [[a regression test must be shown to go red with the fix reverted]],
  [[plan review must check open sibling tickets that own part of the plan scope]],
  [[vault example paths are not repository placement conventions]].
- Repository evidence inspected: `src/session_types.rs`, `src/client_api.rs`,
  `src/daemon_transport.rs`, `src/runtime.rs`, `src/lua_runtime.rs`, `src/main.rs`,
  `src/mcp.rs`, `crates/botster-hub-client/src/lib.rs`,
  `crates/botster-hub-client/src/typescript.rs`,
  `crates/botster-hub-client/examples/generate_typescript.rs`,
  `crates/botster-hub-test-support/src/lib.rs`, `packages/hub-test-support/*`,
  `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs`,
  `docs/client-protocol.md`, `README.md`, `test.sh`, and
  `.github/workflows/loaded-daemon-lifecycle.yml`.
- Plan destination confirmed from repository prior art (`docs/plans/**`,
  `docs/reports/**` are excluded from the path-neutrality audit at `README.md:440`),
  not from a vault example path.
- Baseline on branch `project-pipelines/ticket_1786039258_173310` at `8a60bd5`:
  `./test.sh session_type_device_crud_is_authoritative_and_package_mutation_is_read_only -- --exact`
  -> `1 passed; 0 failed` (package-asset drift check also passed).

## The defect, confirmed in source

`SessionTypeMutation::Update` is wholesale replacement — `src/session_types.rs:297`
does `*existing = definition` — so an editor must submit a complete
`PackageSessionType`. The published row cannot reconstruct one:

- `PackageSessionType` (`src/session_types.rs:23`) carries
  `working_directory: PackageSessionTypeWorkingDirectory` (line 39, policy **and**
  path) and `environment: BTreeMap<String, String>` (line 41).
- `HubSessionType` (line 113) carries only `working_directory_policy: String`
  (line 130) and `allowed_environment_overrides` (line 131). The projection at
  line 705 derives the policy string and discards the path; there is no
  `environment` field at all.

`show_session_type` (line 679) returns that same sanitized row, and
`ResolvedSessionType` (line 139) carries a resolved absolute
`working_directory` and a merged `environment` for one concrete session — computed
values, not authored ones. So a read-modify-write edit silently destroys the
authored working-directory path and the authored environment.

## Product decision ledger

Both open decisions were put to the project question orchestrator as
`question_1786042413_968027` and answered before this plan was finalized.

- **Decision (seam): a lossless editable-definition READ, not a partial update.**
  A new daemon request returns exactly what `UpdateSessionType` consumes. A
  partial-update operation would move merge semantics into Hub and add a second
  mutation path that the required round-trip proof cannot cover as cleanly.
  Endorsed by the orchestrator.
- **Decision (boundary): the sanitized surfaces do not move.**
  `list_session_types`, `show_session_type`, and the `session_type` entity family
  keep byte-identical output. The authoring view widens what an *editor* can read
  for a type it may edit; it does not widen what every subscriber sees.
- **Decision (package rows): typed refusal, not a redacted row.** A package-owned
  id returns the existing `read_only_session_type_source` error kind, matching
  `mutate_session_type` (`src/session_types.rs:221`). Package-authored
  environments are therefore never newly exposed.
- **Decision (admission): the authoring read is gated by editor authority, not
  by the sanitized-read category.** `HubClientAdmission` today groups
  `ListSessionTypes` / `ShowSessionType` / `ResolveSessionType` under
  `allow_packages` and `CreateSessionType` / `UpdateSessionType` /
  `DeleteSessionType` / `SpawnSessionType` / `ReadSessionContext` under
  `allow_runtime` (`src/client_api.rs:698-705`). Because the authoring view
  returns the authored environment and working-directory path, it belongs with
  the mutation group under `allow_runtime`: the caller that may read a definition
  is exactly the caller that may write it. The current `LocalOperator` and
  `deny_all` constructors happen to move both booleans together, so this is not
  observable today — which is precisely why the contract must encode the right
  authority now, before the roles diverge. Corrects a Plan Review finding
  (`finding_1786043185_603153`).
- **Decision (versioning): `PROTOCOL_VERSION` stays 6; `CONFORMANCE_FIXTURE_REVISION`
  goes 31 -> 32.** `ensure_compatible` compares protocol version with **exact
  equality** (`crates/botster-hub-client/src/lib.rs:574`) and conformance revision
  with a **floor** (line 585). Under exact equality, `PROTOCOL_VERSION` is
  reserved for changes that break existing request or response semantics; a purely
  additive request rides the conformance revision. Framing is unchanged, no
  existing request or response changes meaning, and a protocol-6 client that never
  issues the new request stays compatible. Confirmed as Q1 = (a).
  - Consequence, stated positively: `botster-tui` (repinned to protocol 6 by
    `ticket_1785976581_841608`) and `botster-web` need **no** repin for this
    change.
  - Explicit non-goal: the brittleness of exact protocol equality is a known
    project-level concern and is **not** touched here.
- **Decision (feature constants): none added.** The new read is a request, not an
  entity family or negotiated capability, and `current_feature_list()` is not
  touched — so the three-site trap in
  [[adding a hub client feature constant is a three site change]] does not apply.
  The workspace-scoped gate it prescribes still applies because
  `CONFORMANCE_FIXTURE_REVISION` moves.
- **Decision (publication): artifacts in-run, `npm publish` in a separate
  post-merge release ticket.** In-run work regenerates
  `crates/botster-hub-client/generated/daemon-protocol.ts`, re-syncs
  `packages/hub-test-support/**` including `metadata.json` hashes, and bumps the
  conformance revision, all verified by `./test.sh` (which runs
  `sync-assets.mjs --check`). Publishing from an unmerged branch is not a release.
  Confirmed as Q2 = (a); the orchestrator created
  `ticket_1786042460_231768` for the publication.
- **Non-goals:** partial-update semantics, client edit UX, Lua session-type
  mutation capabilities, MCP surface changes, protocol-equality redesign,
  adjacent session-type refactors, and any change to the `session_type` entity
  family.
- **Ask-human threshold:** any discovery that the authoring read cannot be made
  byte-identical on round trip without changing storage or validation semantics.

## Repository ownership boundaries and cross-repository dependencies

Hub owns session-type vocabulary, source precedence, editability policy,
filesystem/state mutation policy, and the local client API over `HubRuntime`
([[botster local client api lives over hubruntime not raw core routers]]).
`botster-hub-client` is the external DTO boundary
([[botster hub client crate is the external client boundary]]) and its crate,
generated TypeScript, and `botster-hub-test-support` assets are intentionally
maintained in this repository — so they are in scope here, not a cross-repo
dependency.

Core is untouched: no lifecycle, metadata, or spawn contract changes.

**No blocking dependency for this run.** `project_pipelines_list_ticket_dependencies`
for `ticket_1786039258_173310` returns none, and nothing in this plan needs another
repository to land first.

Downstream chain, re-queried from durable metadata rather than restated from the
orchestrator's summary (`project_pipelines_list_ticket_dependencies`, exact
result):

- `ticket_1786042460_231768` (Hub release) depends on `ticket_1786039258_173310`
  (this ticket) — `dependency_1786042466_129034`, created 1786042466. That is its
  only edge.
- `ticket_1786039279_917823` (Web edit control,
  `tgt_40abcf71ccf049f4ac0c99953a799869`) depends on **both**
  `ticket_1786039258_173310` (`dependency_1786039286_431750`, created 1786039286)
  **and** `ticket_1786042460_231768` (`dependency_1786042470_319860`, created
  1786042470).

So the effective order is source -> release -> Web, but the original direct
Web -> source edge was never removed when the release ticket was inserted. An
earlier draft of this plan asserted a release-only edge; that was wrong and is
corrected here (`finding_1786043185_104483`).

The redundant direct edge is harmless and is left in place deliberately: the
release ticket already depends on this one, so the direct edge cannot reorder
anything, and it keeps the record that Web needs this source work and not merely
a version bump. The **operative** constraint is still the release edge, because
Web needs a consumable published coordinate rather than a merge. Removing the
redundant edge is the orchestrator's call on tickets this run does not own, not a
change this plan makes unilaterally.

Open sibling tickets checked per
[[plan review must check open sibling tickets that own part of the plan scope]]:
`ticket_1785970234_234515` (Web Hub identity), `ticket_1786036336_442121` (Web
Workspaces driver), `ticket_1786036326_597046` and `ticket_1785970234_132113`
(TUI), `ticket_1785984128_479155` (Workspaces), `ticket_1785970573_178886` (Hub
distribution installer). None of them own any file in this plan's change set; the
Hub distribution ticket is the only same-target sibling and touches installation
receipts, not session types.

## Implementation plan

Every step is additive. No existing request, response, row, or entity frame
changes shape.

1. **Hub policy (`src/session_types.rs`).** Add
   `pub struct HubSessionTypeDefinition { session_type_id: String, source: SessionTypeMutationSource, definition: PackageSessionType }`
   and `pub fn show_session_type_definition(records, state, session_type_id) -> SessionTypeResult<HubSessionTypeDefinition>`.
   It reuses `find_source_session_type_with_row` so bare-vs-qualified id selection,
   precedence, and the ambiguity error are identical to `show_session_type` and the
   qualified-id override behaviour from `bd794ce` is preserved verbatim. Map the
   winning `SourceSessionType` to its mutation source: `Device` -> `Device`,
   `Repo` -> `Repo { target_id: source_name }` (the repo `source_name` *is* the
   `target_id`, `src/session_types.rs:904`), `Package` -> the existing
   `read_only_session_type_source` error. Return `source.session_type.clone()`
   unmodified — the authored definition, bare id included.
2. **Client API (`src/client_api.rs`).** Add
   `HubClientRequest::ShowSessionTypeDefinition`, the matching
   `HubClientOperation` variant with its `request_id`/operation arms, and a
   `HubClientResponseBody::SessionTypeDefinition(...)` variant. Group the new
   operation under **`allow_runtime`, alongside `CreateSessionType` /
   `UpdateSessionType` / `DeleteSessionType`** (`src/client_api.rs:701-705`) —
   **not** under `allow_packages` with the sanitized reads — because the payload
   carries the authored environment and working-directory path. Errors reuse
   `HubClientError::SessionType`.
3. **DTO boundary (`crates/botster-hub-client/src/lib.rs`).** Add
   `DaemonRequest::ShowSessionTypeDefinition { session_type_id }` (wire tag
   `show_session_type_definition`), `DaemonResponseKind::SessionTypeDefinition`,
   the optional `session_type_definition` response field, and
   `DaemonSessionTypeEditableDefinition { session_type_id, source: DaemonSessionTypeMutationSource, definition: DaemonSessionTypeDefinition }`
   — reusing the existing definition and mutation-source DTOs so the payload is
   literally what `update_session_type` accepts. Update the `daemon_request_tag`
   arm (line ~4353) and the `daemon_request_examples()` drift guard (line ~4141).
   Bump `CONFORMANCE_FIXTURE_REVISION` to 32; leave `PROTOCOL_VERSION` at 6 and
   `current_feature_list()` untouched.
4. **Daemon transport (`src/daemon_transport.rs`).** Add the dispatch arm, a
   `daemon_session_type_definition(...)` response builder, the new field in both
   `DaemonResponse` initialisers (lines ~2281 and ~4864), and the operation-name
   arm (line ~7235).
5. **Generated TypeScript (`crates/botster-hub-client/src/typescript.rs` +
   `generated/daemon-protocol.ts`).** Add the request entry, the response field and
   kind, and the new interface; regenerate with
   `cargo run -p botster-hub-client --example generate_typescript`. The
   `--check` mode of that example is the staleness guard.
6. **CLI (`src/main.rs`).** Add `session-types definition <session_type_id>` next
   to `show`, including the usage/help tables at lines ~137, ~254, ~319, and ~352.
   This is the operator-visible production entry point for the new seam and the
   cheapest real-daemon proof path.
7. **Test support and packaged assets.** Extend
   `botster_hub_test_support::first_party_client_support_matrix()` with a
   `session_type_authoring` section naming the request, the response kind, and the
   `read_only_session_type_source` refusal — derived from source constants per
   [[published capability matrices must derive enumerations from source]]. Then
   regenerate `packages/hub-test-support/**` with
   `cargo run -p botster-hub-test-support --example node_package_assets` and
   re-sync `metadata.json` hashes. Extend `packages/hub-test-support/test.mjs`
   alongside its existing `create/update/delete_session_type` assertions
   (lines 171-173) to assert the new request and the conformance revision, so the
   external-consumer surface is proven per
   [[hub test support npm releases need external consumer smoke]]. Do **not**
   publish; `ticket_1786042460_231768` owns that.
8. **Docs (`docs/client-protocol.md` and `README.md`).** Extend the Authoritative
   Session Types section of `docs/client-protocol.md` with the new request, its
   admission group, and its refusal, and add a version-history entry stating that
   conformance advances to 32 while protocol stays 6, with the
   exact-equality-versus-floor rationale. Also update `README.md:1142-1150`, the
   repository's authoritative session-type CRUD/CLI boundary: it currently names
   `session-types create|update|delete` as the mutation boundary and states that
   callers do not receive raw authored data, which the editor-scoped definition
   read would otherwise leave undocumented and misleading. Document the new
   `session-types definition` command, its device/repo scope, its editor
   admission, and the typed package refusal, while stating explicitly that
   ordinary list/show rows and `session_type` entity frames remain sanitized.

Surfaces deliberately untouched: `src/lua_runtime.rs` (the Lua session-type
capability is list/show/spawn only — packages do not author definitions),
`src/mcp.rs` (no session-type tools), `src/runtime.rs` mutation paths,
`src/persistence.rs`, and the `session_type` entity family
([[botster hub client state sync is entity frame only]] is preserved because the
authoring read is an explicit request, not a pushed row).

## Affected surfaces and files

| Surface | Files |
| --- | --- |
| Hub policy | `src/session_types.rs` |
| Local client API | `src/client_api.rs` |
| Daemon transport | `src/daemon_transport.rs` |
| CLI | `src/main.rs` |
| Public DTOs + compatibility | `crates/botster-hub-client/src/lib.rs`, `crates/botster-hub-client/src/typescript.rs`, `crates/botster-hub-client/generated/daemon-protocol.ts` |
| Test support + packaged assets | `crates/botster-hub-test-support/src/lib.rs`, `packages/hub-test-support/daemon-protocol.ts`, `packages/hub-test-support/first-party-client-support-matrix.json`, `packages/hub-test-support/metadata.json`, `packages/hub-test-support/test.mjs` |
| Tests | `tests/hub_client_api_test.rs`, `tests/hub_daemon_lifecycle_test.rs` |
| Docs | `docs/client-protocol.md`, `README.md` (session-type CRUD/CLI boundary, lines ~1142-1150), `docs/plans/publish-lossless-session-type-authoring-view.md`, `docs/reports/*-implement-report.md` |

## Acceptance checks and tests

1. **Round-trip byte-identity (the point of the ticket).** In
   `tests/hub_client_api_test.rs`: author a device session type with
   `working_directory: Relative { path: "nested/dir" }` **and** a non-empty
   `environment`; read it through `ShowSessionTypeDefinition`; submit the returned
   definition unchanged through `UpdateSessionType`; assert the stored
   `PackageSessionType` equals the authored one exactly. Repeat for a repo-sourced
   definition under an admitted target so the atomic file write path is covered.
2. **Round trip fails without the fix.** Per
   [[a regression test must be shown to go red with the fix reverted]], record the
   red run of check 1 driven through the old `ShowSessionType` row — it must lose
   the path and the environment — and the green run through the new seam.
3. **Sanitized surfaces did not move (orchestrator-added requirement).** For the
   same type carrying a relative path and a non-empty environment, assert
   `list_session_types`, `show_session_type`, and the serialized `session_type`
   entity row are byte-identical to their pre-change output, and that no
   `environment` or working-directory path appears in any of them.
4. **Source-aware editability.** `ShowSessionTypeDefinition` on a package-owned id
   returns `read_only_session_type_source`; on a device id returns
   `source = Device`; on a repo id returns `source = Repo { target_id }` matching
   the admitted target. Assert the returned `definition.id` is the **bare** id
   that `UpdateSessionType` matches on — the composite-id hazard named in
   `ticket_1786039279_917823`.
5. **Admission authority, proved negatively.** A `HubClientAdmission` with
   `allow_packages = true` and `allow_runtime = false` can still call
   `ListSessionTypes` / `ShowSessionType` but is **refused**
   `ShowSessionTypeDefinition`; an admission with `allow_runtime = true` is
   allowed. This is the test that stops the authoring payload from drifting into
   the sanitized-read category, and it must be written with the booleans set
   independently rather than through `LocalOperator`, which moves both together
   today.
6. **Selection semantics preserved.** A qualified `source_name/id` returns that
   source's authored definition even when a higher-precedence source overrides it
   (`bd794ce` behaviour); an ambiguous bare id still returns
   `ambiguous_session_type`; an unknown id still returns `unknown_session_type`.
7. **Real daemon path, not just the library.** In
   `tests/hub_daemon_lifecycle_test.rs`, extend the live-daemon session-type
   coverage (`session_type_crud_pushes_authoritative_entity_deltas_without_polling`,
   line ~14420) so the round trip runs over the real socket, and add the new CLI
   subcommand to the CLI smoke table (line ~7014). This is the production
   entry-point proof: the operator CLI and daemon transport, not just
   `session_types.rs`.
8. **Generated and packaged artifacts are current.**
   `cargo run -p botster-hub-client --example generate_typescript -- --check`
   passes, and `./test.sh` passes with its `sync-assets.mjs --check` step, proving
   `daemon-protocol.ts`, the support matrix, and `metadata.json` hashes all moved
   together.
9. **External consumer proof.** `node --test packages/hub-test-support/test.mjs`
   asserts the new request in the published protocol artifact and conformance
   revision 32.
10. **Compatibility semantics.** Assert `PROTOCOL_VERSION == 6` and
   `CONFORMANCE_FIXTURE_REVISION == 32`, that `current_feature_list()` is
   unchanged, and that a requirement pinned at conformance 31 still accepts a
   Hub reporting 32 — the floor behaviour the versioning decision depends on.
11. **Workspace gate, not a target-scoped subset.** Bare `./test.sh` is the
    required gate because `CONFORMANCE_FIXTURE_REVISION` moves
    ([[adding a hub client feature constant is a three site change]],
    [[test script required for rust tests not cargo test]]), plus
    `cargo clippy --workspace --all-targets -- -D warnings` and
    `cargo fmt --check` per
    [[rust repo strict lints must be verified before dismissing warnings]].
12. **Conformance revision uniqueness.** Confirm at merge time that 32 is above the
    newest published `@trybotster/hub-test-support` meaning
    ([[conformance fixture revisions must be unique per published content]]); if a
    sibling branch has claimed 32 for different bytes, reallocate above it rather
    than preserving a branch-local increment.
13. **Documentation contract matches behaviour.** `README.md:1142-1150` and the
    `docs/client-protocol.md` session-types section both describe the
    editor-scoped definition read, the `session-types definition` CLI command,
    its device/repo scope and package refusal, and restate that ordinary
    list/show rows and `session_type` entity frames remain sanitized. A reviewer
    reading only the README must not conclude that callers never receive authored
    data.

Downstream proof required by the charter is satisfied in-repo by checks 8-10 (the
generated client artifact and packaged test-support assets). Registry-install proof
belongs to `ticket_1786042460_231768`.

## Risks

- **Conformance revision collision.** Concurrent Hub branches can claim 32 for
  different bytes. Mitigation: check 11 at merge.
- **Plan Review may prefer a protocol bump.** The decision is recorded above with
  the source lines and the orchestrator's confirmation of Q1 = (a); revisiting it
  means accepting an immediate `botster-tui` repin ticket.
- **Round-trip identity is only as good as serde round-tripping.**
  `BTreeMap` ordering and `Vec` order are deterministic, but a definition that
  fails to survive `PackageSessionType` -> DTO -> `PackageSessionType` would
  silently pass a weak assertion. Mitigation: assert on the stored
  `PackageSessionType`, not on a re-read DTO.
- **`skip_serializing_if` on optional DTO fields** could drop a `None`-versus-absent
  distinction across the wire. Mitigation: the round-trip test carries
  `description`/`icon`/`target_id` in both set and unset states.
- **Admission grouping is invisible today.** `LocalOperator` and `deny_all` move
  `allow_runtime` and `allow_packages` together, so a wrong grouping would pass
  every existing test and only leak authored environments once the roles diverge.
  Mitigation: acceptance check 5 sets the booleans independently rather than
  going through a constructor.
- **Scope creep toward a partial update.** If a reviewer asks for `PATCH`
  semantics, that is a new ticket; this seam intentionally keeps Update as
  wholesale replacement.
- **File contention.** `src/daemon_transport.rs` and
  `crates/botster-hub-client/src/lib.rs` are high-traffic; `ticket_1785970573_178886`
  is the only open same-target sibling and does not touch session types. Expect to
  rebase rather than to conflict semantically.

## Assumptions and unknowns

- Assumption: any admitted local client that can already call
  `UpdateSessionType`/`DeleteSessionType` on device and repo definitions may also
  read them, so exposing authored device/repo definitions to an admitted caller is
  not a new authority boundary. Package definitions are excluded regardless.
- Assumption: `docs/plans/**` remains the plan destination for this repository
  (confirmed from mainline prior art and `README.md:440`, not from a vault
  example).
- Assumption: the effective winner is the right edit target for a bare id, and a
  qualified id is the way to target an overridden source — matching `bd794ce`.
- Unknown, resolved at Implement: the exact key name for the support-matrix
  section (`session_type_authoring`) is a naming choice, not a contract decision.
- Unknown, deferred to `ticket_1786042460_231768`: the next
  `@trybotster/hub-test-support` version number, which must be allocated above the
  published `0.1.24` from the merged commit.

## Vault gaps worth capturing

1. **`PROTOCOL_VERSION` versus `CONFORMANCE_FIXTURE_REVISION` under exact protocol
   equality.** [[daemon event shape changes bump conformance fixture revision not protocol version]]
   predates the move to exact protocol equality and reads as if any
   request-shaped change bumps the protocol. The operative rule is now:
   exact equality makes a protocol bump a stack-wide flag day, the conformance
   revision is a floor, and therefore additive requests ride the conformance
   revision while `PROTOCOL_VERSION` is reserved for breaking existing request or
   response semantics. Worth a refinement note or an amendment to the existing one.
2. **Wholesale-replacement mutations require a lossless authoring read.** The
   general lesson — that a sanitized projection plus a replace-everything update is
   a silent data-loss contract, and that the fix is an editor-scoped read rather
   than widening the published row — is not captured anywhere and is likely to
   recur on other Hub-owned entities.
3. **Publication is a separate ticket from source.** The corrected edge (Web
   depends on the release ticket, not the source ticket, because it needs a
   consumable coordinate rather than a merge) generalises the existing gotcha about
   closed dependency tickets whose PRs are unmerged.
