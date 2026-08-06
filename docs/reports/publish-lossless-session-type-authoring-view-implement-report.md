# Publish a lossless session-type authoring view — implementation report

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Ticket / run: `ticket_1786039258_173310` / `run_1786042047_208281`
- Branch base: `origin/main` at `b64bb9bdac587a2f3815fb58d4228beafcf8692a`
  (rebased from `8a60bd5` after Review; see "Review round 2" below).
- Plan followed: `docs/plans/publish-lossless-session-type-authoring-view.md`
  (revision 2, approved by `review_1786043613_956773`).

## Review round 2 — blocker resolved

`review_1786050793_126679` raised one blocker,
`finding_1786050793_710073`, and it was correct on both counts.

`first_party_client_support_matrix()` gained a `session_type_authoring` object,
but `tests::support_matrix_serializes_to_stable_json_shape` still compared
against an expected `json!` literal that had no such field, so the guard
rejected the new section.

The reason it was not caught is the more important half. On the previous base
`8a60bd5`, `test.sh` ran a bare `cargo test`. The workspace root is itself a
package and declares no `default-members`, so that command tests **only the root
package**: every sibling crate's test binary compiled but never executed. The
first report's "412 passed" was therefore a root-package-only result presented as
a workspace gate — a false green. `crates/botster-hub-test-support` and
`crates/botster-hub-client` unit tests, including two this branch added or
renamed, were never run.

Fix applied:

1. Added the source-derived `session_type_authoring` shape to the expected stable
   JSON literal in `crates/botster-hub-test-support/src/lib.rs`.
2. Rebased onto current `main` `b64bb9b`, whose `c4f217a` corrects `test.sh` to
   `cargo test --workspace` so the wrapper actually executes every member.

Reviewer's exact reproduction command now passes:
`cargo test -p botster-hub-test-support support_matrix_serializes_to_stable_json_shape -- --nocapture`
→ 1 passed, exit 0 (it failed at `lib.rs:7606` with exit 101 before).

All gates below were rerun on the rebased tree under the corrected wrapper. The
real workspace run is **28 targets, 657 passed, 0 failed, 1 pre-existing
ignored** — versus the 12 targets and 412 the old wrapper reached. The
red-on-revert ablation was also re-executed after the rebase and still goes red.

Conformance revision 32 remains unique: `main` at `b64bb9b` is still at 31, and
the newest published `@trybotster/hub-test-support@0.1.24` still reports 31.

## Guidance applied

Repository charter [[botster-hub-playbook]]; role overlays
[[implementer-playbook]] and [[botster-implementer-playbook]]; surface overlay
[[botster-runtime-reviewer-playbook]].

Targeted notes:
[[botster local client api lives over hubruntime not raw core routers]],
[[botster hub client crate is the external client boundary]],
[[botster hub client compatibility descriptors belong in client crate]],
[[daemon event shape changes bump conformance fixture revision not protocol version]],
[[conformance fixture revisions must be unique per published content]],
[[adding a hub client feature constant is a three site change]],
[[published capability matrices must derive enumerations from source]],
[[botster first party client support matrices belong in hub test support]],
[[hub test support npm releases need external consumer smoke]],
[[botster hub client state sync is entity frame only]],
[[session template override sources use package device repo explicit precedence]],
[[test script required for rust tests not cargo test]],
[[rust repo strict lints must be verified before dismissing warnings]],
[[a regression test must be shown to go red with the fix reverted]],
[[implementation artifacts must match actual git state]],
[[implement gate must verify committed work and pr link before review]],
[[implementation steps must persist report artifacts for review]],
[[pipeline vault checklists must cite exact resolvable note titles]].

## The seam

`SessionTypeMutation::Update` replaces a definition wholesale
(`src/session_types.rs`, `*existing = definition`), but `HubSessionType` derives
a `working_directory_policy` string from the authored
`PackageSessionTypeWorkingDirectory` and has no `environment` field at all. A
client that read a row and submitted it back destroyed the authored
working-directory path and the authored environment.

`show_session_type_definition` returns the authored `PackageSessionType`
unmodified plus the exact `SessionTypeMutationSource` that `Update` requires, so
read-modify-write is byte-identical for every untouched field. Selection reuses
`find_source_session_type_with_row`, so bare/qualified id semantics, precedence,
ambiguity, and the qualified-id override behaviour from `bd794ce` are identical
to `show_session_type`. Package-owned ids get the existing
`read_only_session_type_source` refusal, so package-authored environments are
never newly exposed.

## Files changed

| File | Change |
| --- | --- |
| `src/session_types.rs` | `HubSessionTypeDefinition` + `show_session_type_definition`, with the package refusal. |
| `src/lib.rs` | Export `HubSessionTypeDefinition`. |
| `src/client_api.rs` | `ShowSessionTypeDefinition` request/operation, `SessionTypeDefinition` response body, `allow_runtime` admission grouping, and a unit test that sets the admission booleans independently. |
| `src/daemon_transport.rs` | Dispatch arm, `daemon_session_type_definition` builder, hub→daemon source/definition converters, new `DaemonResponse` field in the base and shutdown initialisers, operation-name arm. |
| `src/local_webrtc.rs` | New `DaemonResponse` field in its literal initialiser. |
| `src/main.rs` | `session-types definition <session-type-id>` subcommand, usage strings, `SessionTypeDefinition` print arm. |
| `crates/botster-hub-client/src/lib.rs` | `DaemonRequest::ShowSessionTypeDefinition`, `DaemonResponseKind::SessionTypeDefinition`, optional `session_type_definition` response field, `DaemonSessionTypeEditableDefinition`, tag/example/drift-guard entries, `CONFORMANCE_FIXTURE_REVISION` 31 → 32, compatibility tests. |
| `crates/botster-hub-client/src/typescript.rs` | Request entry, response field, response kind, new interface. |
| `crates/botster-hub-client/generated/daemon-protocol.ts` | Regenerated (+9 lines). |
| `crates/botster-hub-test-support/src/lib.rs` | `SessionTypeAuthoringSupport` matrix section, derived from the public DTOs. |
| `packages/hub-test-support/**` | Regenerated assets and `metadata.json` hashes; `test.mjs` asserts the new request, DTO, response field/kind, matrix section, and conformance 32. |
| `docs/client-protocol.md` | Authoring-read contract, admission group, refusal, selection, CLI, and the protocol-6/conformance-32 version-history entry. |
| `README.md` | Session-type CRUD/CLI boundary now documents the editor-scoped read and restates that list/show/entity rows remain sanitized. |
| `tests/hub_client_api_test.rs` | Round trip (device + repo), sanitized-row lossiness and shape pin, package/unknown/ambiguous refusals, denied-admission path, selection semantics. |
| `tests/hub_daemon_lifecycle_test.rs` | Real-socket authoring read, edit-one-field round trip, package refusal, CLI success path, CLI usage table entry. |
| `docs/plans/publish-lossless-session-type-authoring-view.md` | Approved plan, now tracked. |
| `docs/reports/publish-lossless-session-type-authoring-view-implement-report.md` | This report. |

Also restored: `.gitignore`. The run worktree arrived with it emptied (a
pre-existing modification, not part of this ticket). Left as-is it would have
swept `target/` and a local `.env` into the commit, so it was restored to its
`HEAD` contents. No other pre-existing worktree change was touched.

## Ownership boundaries preserved

Hub kept session-type vocabulary, source precedence, editability policy, and the
local client API over `HubRuntime`; the new read is a `HubClientApi` request, not
a raw core router call. `botster-hub-client` remains the DTO boundary and is
maintained in this repository, so its crate, generated TypeScript, and the
`botster-hub-test-support` assets are in scope rather than cross-repo work. Core
is untouched: no lifecycle, metadata, or spawn contract change. `src/lua_runtime.rs`
(list/show/spawn only), `src/mcp.rs`, `src/runtime.rs` mutation paths,
`src/persistence.rs`, and the `session_type` entity family are unchanged — the
authoring view is an explicit request, not a pushed row, so
[[botster hub client state sync is entity frame only]] is preserved.

The admission grouping is the load-bearing boundary decision: the read sits under
`allow_runtime` with `CreateSessionType`/`UpdateSessionType`/`DeleteSessionType`,
not under `allow_packages` with the sanitized reads.

## Cross-repo dependencies and separately routed work

None blocking. `npm publish` is deliberately **not** done here; it belongs to
`ticket_1786042460_231768` (same target), which `ticket_1786039279_917823`
(botster-web edit control, `tgt_40abcf71ccf049f4ac0c99953a799869`) consumes.
`PROTOCOL_VERSION` stays 6, so `botster-tui` and `botster-web` need no repin.

## Deviations from plan

1. **A third `DaemonResponse` initialiser.** The plan named two
   (`src/daemon_transport.rs`); `src/local_webrtc.rs` has a third literal
   initialiser that also needed the new optional field. Mechanical consequence of
   the same additive change, no behaviour change.
2. **Admission-negative test placement.** Acceptance check 5 requires setting
   `allow_packages`/`allow_runtime` independently, but `HubClientAdmission`'s
   fields are private and only `local_operator()`/`deny_all()` are public. Rather
   than widen the public constructor surface for a test, the grouping assertion
   is a unit test inside `src/client_api.rs`, where the fields are in scope. The
   production denial path (`handle_request` → `AdmissionDenied`) is proved
   separately in `tests/hub_client_api_test.rs` via `deny_all()`. Both halves of
   the check are covered; only the location changed.
3. **Support-matrix section is richer than "name the request".** Plan step 7 asked
   for the request, response kind, and refusal.
   `authored_fields_absent_from_published_row` is derived by differencing the
   serialized key sets of a fully populated `DaemonSessionTypeDefinition` and
   `DaemonSessionType`, so promoting an authored field into the published row
   fails the pinned snapshot. This satisfies
   [[published capability matrices must derive enumerations from source]] rather
   than pinning a hand-maintained list.
4. **CLI usage-table lines.** The plan cited `src/main.rs` lines ~137/254/319/352;
   those already cover the whole `session-types` command and needed no change.
   The real edits were the subcommand dispatch, the usage strings, and the
   response printer.

No plan decision was reversed and nothing was waived, so the committed plan's
decisions, scope, and acceptance checks still hold as written.

## Tests and downstream proof run

All commands run from the run worktree.

All commands below were run on the rebased tree at base `b64bb9b`, under the
corrected `cargo test --workspace` wrapper.

| Gate | Command | Result |
| --- | --- | --- |
| Workspace suite (required: conformance revision moved) | `./test.sh` | exit 0 — 657 passed, 0 failed, 1 pre-existing ignored, across 28 targets |
| Blocker reproduction | `cargo test -p botster-hub-test-support support_matrix_serializes_to_stable_json_shape -- --nocapture` | exit 0, 1 passed (was exit 101 before the fix) |
| Sibling crates the old wrapper skipped | `cargo test -p botster-hub-client` | exit 0, 54 passed |
| Strict lints | `cargo clippy --workspace --all-targets -- -D warnings` | exit 0 |
| Format | `cargo fmt --check` | exit 0 |
| Generated artifact drift | `cargo run -p botster-hub-client --example generate_typescript -- --check` | exit 0 |
| Packaged asset drift | `node packages/hub-test-support/scripts/sync-assets.mjs --check` (runs inside `./test.sh`) | "hub test-support package assets are current" |
| External consumer smoke | `node --test packages/hub-test-support/test.mjs` | 1 pass, 0 fail |
| Registry uniqueness | clean external install of `@trybotster/hub-test-support@0.1.24` | published metadata reports conformance 31 and its `daemon-protocol.ts` has no `show_session_type_definition`, so 32 is strictly above every published meaning |
| Branch collision | `git show origin/main:crates/botster-hub-client/src/lib.rs` | `CONFORMANCE_FIXTURE_REVISION = 31` on main; no sibling branch has claimed 32 |

Red-on-revert ablation, per
[[a regression test must be shown to go red with the fix reverted]]. With
`show_session_type_definition` altered to reconstruct from the sanitized row
(`working_directory` forced to `PackageRoot`, `environment` cleared):

| code | test | result |
| --- | --- | --- |
| fixed | `session_type_definition_round_trips_authored_path_and_environment` | PASS |
| fixed | `session_type_definition_round_trips_repo_sources_and_preserves_selection` | PASS |
| ablated | `session_type_definition_round_trips_authored_path_and_environment` | FAIL — `working_directory: PackageRoot` vs `Relative { path: "nested/dir" }`, `environment: {}` vs the authored pair |
| ablated | `session_type_definition_round_trips_repo_sources_and_preserves_selection` | FAIL — same two fields lost on the repo source |

The ablation was reverted and the suite rerun green before commit.

Production entry point proved, not just library reachability: the real-daemon
test drives `ShowSessionTypeDefinition` over the socket, reads → edits one field
→ submits back, re-reads and asserts the authored path and environment survived,
and then runs the real operator binary
(`botster-hub session-types definition --data-dir <dir> terminal-accessory`)
against the live daemon, asserting `response=session_type_definition`,
`session_type_id=device/terminal-accessory`, `"source":"device"`,
`"path":"nested/dir"`, and `"BOTSTER_MODE":"authored"` in its stdout.

Sanitized boundary held: the same test asserts the socket `ShowSessionType` row
contains neither the authored path nor the authored environment value, and the
client-API test pins the full published `session_type` row key set and asserts
`list_session_types` output is unchanged.

## Unverified behavior and residual risk

- **Not published.** `@trybotster/hub-test-support` is still 0.1.24 on the
  registry. Downstream consumers get the new types only after
  `ticket_1786042460_231768` allocates and publishes a coordinate above 0.1.24.
  Registry-install proof of the *new* content is that ticket's gate, not this one.
- **Revision uniqueness is a merge-time property.** 32 was re-verified unique
  after the rebase (`main` `b64bb9b` is at 31, published 0.1.24 is at 31), but 32
  was verified unique
  against published 0.1.24 and against `origin/main` at implementation time. A
  sibling Hub branch could still claim 32 for different bytes before merge;
  recheck at merge per
  [[conformance fixture revisions must be unique per published content]].
- **Admission divergence is still untested in production wiring.**
  `LocalOperator` and `deny_all` move `allow_runtime` and `allow_packages`
  together, so no shipped caller currently exercises the split. The grouping is
  asserted directly, but no production role yet has package-read authority
  without editor authority.
- **CLI output is line-oriented, not a machine contract.** The `definition=`
  line prints the authored definition as JSON so it can be piped back into
  `session-types update`, but no test asserts a full shell round trip through
  the CLI; the byte-identity proof runs through the socket and the library.
- **Repo-source round trip covers one admitted target.** Multi-target repo
  authoring and concurrent writers to the same `.botster/session-types.json`
  are unchanged from the existing mutation path and were not re-proved here.
- **`node --test packages/hub-test-support/test.mjs` needs a local
  `npm install`** in that package (for `@trybotster/ui-contract`); it is not
  wired into `./test.sh` or CI. The install was done to run the smoke and then
  removed, so the branch carries no `package-lock.json` or `node_modules`.

## Missing vault guidance discovered

0. **A workspace-root package with no `default-members` makes bare `cargo test` a
   false green.** This branch reported a passing "workspace" gate that had never
   executed a single sibling-crate test, and it took Review to catch it. `main`
   `c4f217a` fixed the wrapper with a load-bearing comment, but the underlying
   trap — a repo whose root is both the workspace and a member — is not captured
   anywhere and is not specific to this repository. It is also a sharpening of
   [[adding a hub client feature constant is a three site change]], which already
   says "bare `./test.sh` is the right gate for `CONFORMANCE_FIXTURE_REVISION`
   changes": that advice was correct in intent but, on the old wrapper, did not
   deliver what it promised. Worth a gotcha note including the symptom (test
   binaries compile, target count is suspiciously low) and the check
   (`cargo test --workspace`, and count the `Running` lines).

1. **Wholesale-replacement mutations require a lossless authoring read.** The
   general shape — a sanitized projection plus a replace-everything update is a
   silent data-loss contract, and the fix is an editor-scoped read rather than
   widening the published row — is not captured anywhere and will recur on other
   Hub-owned entities. Worth an atomic note.
2. **Editor-scoped reads belong in the mutation admission group.** The rule "the
   caller that may read authored data is exactly the caller that may write it" is
   not written down, and the `LocalOperator`/`deny_all` coupling means a wrong
   grouping passes every existing test. Worth a gotcha note, including the
   technique of asserting the admission table with the booleans set
   independently instead of through a constructor.
3. **`PROTOCOL_VERSION` versus `CONFORMANCE_FIXTURE_REVISION` under exact
   protocol equality.** [[daemon event shape changes bump conformance fixture
   revision not protocol version]] predates exact protocol equality and reads as
   if any request-shaped change bumps the protocol. The operative rule is that
   exact equality makes a protocol bump a stack-wide flag day while the
   conformance revision is a floor, so additive requests ride the conformance
   revision. Worth amending that note.
