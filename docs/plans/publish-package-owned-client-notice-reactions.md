# Publish a package-owned client notice reaction descriptor

Ticket: `ticket_1787278643_145174`
Run: `run_1787282470_625000`
Target repository: `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`, `https://github.com/trybotster/botster-hub.git`)
Base: `origin/main` at `b3b54f1`
Revision: 2. Revision 1 was returned by Plan Review `review_1787283384_138075`. This revision answers findings `finding_1787283385_955304`, `finding_1787283385_924120`, `finding_1787283385_449120`, `finding_1787283385_295309`, and `finding_1787283385_256180`.

## Product decision ledger

Binding human answer `question_1787283207_365510` (2026-08-20):

- Version one uses `subject_scope = session` only. `none` and any implicit global scope are removed. A package event without a matching session subject produces no transient client notice. A later ticket may add an explicit global scope after its delivery, privacy, and noise policy exist.
- Severity is the fixed enum `info | warning | error`.
- TTL is an explicit integer `ttl_ms` from 1,000 through 60,000 inclusive.
- The text pointer is one top-level RFC 6901 pointer, such as `/notice`. Admission validates pointer decoding. The resolved payload value must be a non-empty bounded string. Deeper paths are rejected.
- No expression evaluation, no entity joins, no defaults that change scope, and no client-specific extensions.

Earlier decision of record: `question_1787278509_823001` on `ticket_1787278327_274484`.

Non-goals confirmed by this ledger: global or unfiltered notices, durable attention state, and client-side product constants.

## Repository playbook loaded

- [[botster-hub-playbook]] (exact ownership charter for the ticket target repository)

## Other playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-client-playbook]] (the public `DaemonPackage` DTO lives in `crates/botster-hub-client`)

Botster domain maps:

- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]]

Atomic notes:

- [[client notice reactions belong to package declarations not client constants]]
- [[hub package event declarations have no client projection]]
- [[package event contracts live on HubPackageManifest not Core PackageManifest]]
- [[exact owner plus name is the only package event subscription key]]
- [[Package-event subject filters are exact strings compiled at admission]]
- [[event plane client proof uses library contract fixtures]]
- [[generic botster clients must not hardcode package event reactions]]
- [[question opened clients subscribe with empty subjects]]
- [[web package event notices are transient and entity state is durable]]
- [[botster package surface semantics live in ui contract while hub owns admission]]
- [[hub test support lacks package event producer fixtures]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub test support npm releases need external consumer smoke]]
- [[conformance fixture revisions must be unique per published content]]
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[vault example paths are not repository placement conventions]]
- [[botster pipeline needs continuous product owner between agent steps]]

Runtime-teardown class: does not apply. This ticket changes a manifest contract, an admission rule, and a sanitized projection. It changes no peer lifecycle, no `SessionIo`/`ClientWorker` teardown, and no resource-spin path. [[botster runtime teardown lenses]] is therefore not loaded.

## Context loaded

Current repository state at `b3b54f1`:

- `src/packages.rs:80-150` defines `HubPackageManifest`, `HubPackageEvents`, `HubEmittedEvent`, and `validate_event_contracts`.
- `src/package_event_schema.rs:60-88` compiles the bounded payload schema subset and exposes `spec()`.
- `src/package_event_router.rs:1189-1219` filters client delivery by exact `payload.subject` membership.
- `src/client_api.rs:1381-1396` defines the sanitized `HubClientPackage`. It has no event or notice field.
- `src/daemon_transport.rs:7319` maps `HubClientPackage` into `DaemonPackage`.
- `crates/botster-hub-client/src/lib.rs:1916-1941` defines `DaemonPackage`. It has no event or notice field and is not `#[non_exhaustive]`.
- `crates/botster-hub-client/src/lib.rs:30-31` sets `PROTOCOL_VERSION = 7` and `CONFORMANCE_FIXTURE_REVISION = 44`.
- `crates/botster-hub-client/src/typescript.rs:13,992-1016` emits the `DaemonPackage` TypeScript interface and imports `PackageSurfaceDescriptor` from `@trybotster/ui-contract`.
- `crates/botster-ui-contract/src/lib.rs:45-227` defines `PackageSurfaceDescriptor` and `validate_package_presentation`.
- `crates/botster-ui-contract/examples/generate_assets.rs` regenerates `packages/ui-contract/index.d.ts`, `schema.json`, and `conformance-fixtures.json`.
- `packages/ui-contract/package.json` is version `0.3.2`. `packages/ui-contract/index.js:5` repeats that version. `index.js` is hand-maintained.
- `packages/hub-test-support/package.json` is version `0.1.39` and depends on `@trybotster/ui-contract` `0.3.2`.
- `packages/hub-test-support/metadata.json` records protocol `7`, conformance revision `44`, and a `daemon-protocol.ts` checksum.
- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/` is the Hub-owned, product-free ABI fixture package that the npm package ships to clients.
- `examples/event-plane-producer/` is the checked-in Hub-owned event producer used by `tests/hub_daemon_lifecycle/package_event_plane.rs`.
- `script/publish-npm-packages:58-84` already packs both packages and compiles a strict TypeScript consumer of the packed UI contract tarball.
- Measured on this worktree at `b3b54f1`: `node packages/ui-contract/test.mjs` passes, and `node packages/hub-test-support/test.mjs` fails with `ERR_MODULE_NOT_FOUND` for `@trybotster/ui-contract` because that dependency is not installed in the checkout.
- CI gates are `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `./test.sh --locked`.

## Scope

In scope:

1. Add the authored declaration shape, the projected client descriptor, the fixed enums, the shared validation, and the shared text resolution helper to `crates/botster-ui-contract`.
2. Admit `events.notices` on `HubPackageManifest` next to `events.emitted`, with Hub-owned admission rules.
3. Project the admitted declaration into a client descriptor that always carries the exact owner, through `HubClientPackage` and `DaemonPackage`.
4. Emit the descriptor in the generated daemon protocol TypeScript artifact with a required `owner` field.
5. Republish the contract through new unpublished `@trybotster/ui-contract` and `@trybotster/hub-test-support` versions, including regenerated assets and metadata.
6. Declare the descriptor in two Hub-owned fixture packages, so both the published client ABI fixture and the live Hub daemon test exercise it without any product plugin.
7. Add admission, projection, generated-artifact, live daemon, and packaged-consumer tests.

Out of scope (non-scope):

- Core `PackageManifest`. Core stays the policy-free execution shape.
- A global or unfiltered notice scope. The product ledger defers it to a later ticket.
- Client rendering, toast presentation, or notice state machines. `botster-web` and `botster-tui` own those.
- Removing the hardcoded product constants in Web and TUI. Tickets `ticket_1787278327_274484` and `ticket_1787278327_199618` own that removal.
- The Project Pipelines package declaration and emitter.
- Durable attention state, entity families, entity joins, expression evaluation, or workflow correlation rules.
- npm publication itself and the `botster-ui-contract-v0.3.3` tag creation. The maintainer runs `script/tag-ui-contract` on merged `main` and `script/publish-npm-packages` for the release.
- Any change to subscribe, `package_event`, `event_gap`, reconnect, or queue-limit behavior.

## Botster layers touched

- Hub package admission and sanitized projection (`src/`).
- Public client DTO and generated TypeScript (`crates/botster-hub-client`).
- Shared UI/client contract crate and npm package (`crates/botster-ui-contract`, `packages/ui-contract`).
- Test support npm package and its ABI fixture (`crates/botster-hub-test-support`, `packages/hub-test-support`).
- Hub-owned example plugin package (`examples/event-plane-producer`).

## Design

### Two shapes, one required owner (answers `finding_1787283385_924120`)

The authored manifest shape and the projected client shape are separate types. The authored shape may omit the owner. The public client shape always carries it.

```rust
/// Authored in botster-package.json. Owner is optional and must equal the package name.
pub struct PackageNoticeReactionDeclaration {
    pub owner: Option<String>,
    pub name: String,
    pub subject_scope: PackageNoticeSubjectScope,
    pub text_pointer: String,
    pub ttl_ms: u32,
    pub severity: PackageNoticeSeverity,
}

/// Projected to every client. Owner is required.
pub struct PackageNoticeReactionDescriptor {
    pub owner: String,
    pub name: String,
    pub subject_scope: PackageNoticeSubjectScope,
    pub text_pointer: String,
    pub ttl_ms: u32,
    pub severity: PackageNoticeSeverity,
}

pub enum PackageNoticeSubjectScope { Session }            // "session" only in version one
pub enum PackageNoticeSeverity { Info, Warning, Error }   // "info" | "warning" | "error"
```

`PackageNoticeReactionDeclaration::into_descriptor(package_name)` is the only construction path for the descriptor. It sets `owner` to the admitted package name.

`HubPackageManifest` holds declarations. `HubClientPackage` and `DaemonPackage` hold descriptors. The generated TypeScript emits `owner` as a required field, so no client can read an optional owner.

### Version-one bounds (answers `finding_1787283385_955304`)

`PackageNoticeSubjectScope` has exactly one variant, `session`. The enum remains a field so that a later ticket can add an explicit scope without a breaking wire change. There is no `none` variant, no empty-subject path, and no default that widens scope.

`validate_package_notice_reactions` enforces the shared shape rules:

- `name` is non-empty and contains no `*` or `?`.
- `owner`, when present on a declaration, is non-empty and contains no wildcard.
- `text_pointer` decodes as RFC 6901: it starts with `/`, contains exactly one segment, and decodes `~1` to `/` and `~0` to `~`. A pointer with a second `/` after decoding is rejected, and a trailing `~` or an unknown `~x` escape is rejected.
- The decoded segment is at most 64 UTF-8 bytes.
- `ttl_ms` is within `1_000..=60_000` inclusive.
- No two declarations share the same resolved `(owner, name)` pair.

### Shared text resolution

`crates/botster-ui-contract` also owns the canonical resolution routine, so no generic client reimplements pointer decoding:

```rust
pub const NOTICE_TEXT_MAX_BYTES: usize = 512;

pub fn resolve_notice_text<'a>(payload: &'a Value, pointer: &str)
    -> Result<&'a str, NoticeTextError>;
```

`resolve_notice_text` decodes the one-segment pointer, requires the resolved value to be a JSON string, requires that string to be non-empty after trimming, and requires it to be at most `NOTICE_TEXT_MAX_BYTES` UTF-8 bytes. `packages/ui-contract/index.js` gains the equivalent `resolveNoticeText` export, driven by the same conformance fixture vectors, so Botster Web uses one routine and Botster TUI uses the crate.

### Hub admission rules (`src/packages.rs`)

`validate_event_contracts` gains the notice rules after the emitted-event rules:

- Call `validate_package_notice_reactions` first.
- Reject a declaration whose `owner` is present and does not equal `self.name`.
- Reject a declaration whose `name` does not appear in `self.events.emitted`.
- Reject a declaration whose matching emitted event does not include the `clients` audience.
- Reject a declaration whose matching admitted `payload_schema` declares no `subject` property, because Hub filters only on `payload.subject` and version one is session-scoped.
- Reject a `text_pointer` whose decoded segment does not name a declared string property of the matching admitted `payload_schema`.

The reserved package name `hub` is already rejected before these checks.

### Projection

- `HubClientPackage` gains `notice_reactions: Vec<PackageNoticeReactionDescriptor>`. `from_record` calls `into_descriptor(&record.manifest.name)`.
- `DaemonPackage` gains `notice_reactions: Vec<PackageNoticeReactionDescriptor>` with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.
- `daemon_package_from_client` copies the projected vector.
- `typescript.rs` adds `("notice_reactions?", "PackageNoticeReactionDescriptor[]")` and imports the type from `@trybotster/ui-contract`. The descriptor interface itself declares `owner` as required.

### Version cutover

- `@trybotster/ui-contract` `0.3.2` -> `0.3.3`. `0.3.2` is published and immutable.
- Both Git-consumed member manifests move to `tag = "botster-ui-contract-v0.3.3"`, and the root `[patch]` comment records the new tag.
- `@trybotster/hub-test-support` `0.1.39` -> `0.1.40`, with `@trybotster/ui-contract` dependency `0.3.3`.
- `CONFORMANCE_FIXTURE_REVISION` `44` -> `45`. `PROTOCOL_VERSION` stays `7`, because the added field is optional on the wire and an unchanged client still deserializes the package row.
- Before selecting `45`, confirm no other branch or published package already names revision `45` with different bytes, per [[conformance fixture revisions must be unique per published content]].

### Fixtures

- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/botster-package.json` gains one emitted event with a `subject` string property and a notice string property, one session-scoped notice declaration, and a README paragraph. This is the published, product-free ABI fixture that Web and TUI conformance harnesses read.
- `examples/event-plane-producer/botster-package.json` gains `subject` and `notice` properties on the `sample.ready` schema and one session-scoped notice declaration. `plugin.lua` gains an emit path that sets both fields, so the Hub daemon test proves declaration, projection, subscription, delivery, and text resolution in one live run.

## Repository ownership boundaries and cross-repository dependencies

- `botster-core` is untouched. Package event and notice policy lives on `HubPackageManifest`, per [[package event contracts live on HubPackageManifest not Core PackageManifest]].
- `botster-ui-contract` owns the renderer-neutral descriptor vocabulary, shared shape validation, and the shared text resolution routine, per [[botster package surface semantics live in ui contract while hub owns admission]].
- `botster-hub` owns admission, the exact-owner rule, the emitted-event cross-check, and the sanitized projection.
- `botster-hub-client` owns the public `DaemonPackage` DTO, the generated TypeScript, and the conformance revision.
- Generic client reaction rendering stays with `botster-web` and `botster-tui`.

Cross-repository dependencies: none block this run. This ticket produces the contract that the downstream tickets consume.

Downstream consumers (no dependency edges to register against this run):

- `botster-project-pipelines`: package declaration and emitter. Its `question.opened` schema must gain a `subject` field before it can declare a session-scoped notice.
- `botster-web`: `ticket_1787278327_274484`.
- `botster-tui`: `ticket_1787278327_199618`.

Consumer tickets after a Hub session-type eligibility parent: not applicable. This ticket has no session-type eligibility parent, and `blocking_dependencies` is empty.

## Assumptions and unknowns

- A1. Version one ships `subject_scope = session` only. Source: binding answer `question_1787283207_365510`. This replaces the revision-1 assumption that added `none`.
- A2. Severity vocabulary is `info`, `warning`, `error`. Source: the same binding answer.
- A3. `ttl_ms` is an integer within `1_000..=60_000` inclusive. Source: the same binding answer.
- A4. The text pointer is one top-level RFC 6901 pointer, decoded and validated, and the resolved value must be a non-empty bounded string. Source: the same binding answer.
- A5. `NOTICE_TEXT_MAX_BYTES` is 512. The binding answer requires a bound but names no number. Plan Review may set a different bound.
- A6. The declaration key is `(owner, name)`. One admitted event carries at most one notice reaction.
- A7. `DaemonPackage` does not become `#[non_exhaustive]` in this run. The implementer measures the downstream source cost first, per [[public dto field additions are source breaking without non exhaustive]], and records it for the consumer tickets.
- U1. The npm coordinates `0.3.3` and `0.1.40`, and conformance revision `45`, must be confirmed unused before selection, per [[Hub test support capability cutovers use a new unpublished package version]], [[an unmerged run that publishes an npm coordinate burns it]], and [[conformance fixture revisions must be unique per published content]].
- U2. The `botster-ui-contract-v0.3.3` tag does not exist until the maintainer runs `script/tag-ui-contract` on merged `main`. The root `[patch]` entry keeps this workspace resolvable in the meantime, per [[git-visible Hub member manifests must use the UI contract tag]].

## Affected surfaces and files

- `crates/botster-ui-contract/src/lib.rs` -- declaration and descriptor types, enums, validation, `resolve_notice_text`, schema and TypeScript declaration entries.
- `crates/botster-ui-contract/src/assets.rs` -- wire enums, JSON schema, conformance fixture entries including text-resolution vectors.
- `crates/botster-ui-contract/tests/ui_contract_test.rs` -- validation and resolution unit tests.
- `crates/botster-ui-contract/tests/generated_assets_test.rs` -- fixture deserialization coverage.
- `packages/ui-contract/index.d.ts`, `schema.json`, `conformance-fixtures.json` -- regenerated.
- `packages/ui-contract/index.js` -- `packageVersion` and `resolveNoticeText`.
- `packages/ui-contract/test.mjs`, `package.json`, `README.md` -- resolution vectors, version, documentation.
- `src/packages.rs` -- `HubPackageEvents.notices` and admission rules.
- `src/client_api.rs` -- `HubClientPackage.notice_reactions` and `from_record`.
- `src/daemon_transport.rs` -- `daemon_package_from_client`.
- `crates/botster-hub-client/src/lib.rs` -- `DaemonPackage.notice_reactions`, `CONFORMANCE_FIXTURE_REVISION`.
- `crates/botster-hub-client/src/typescript.rs` -- import list, `DaemonPackage` interface, descriptor interface with required `owner`.
- `crates/botster-hub-client/generated/daemon-protocol.ts` -- regenerated.
- `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`, `Cargo.toml` -- UI contract tag.
- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/botster-package.json`, `README.md`.
- `crates/botster-hub-test-support/src/lib.rs` -- fixture assertions for the new field.
- `packages/hub-test-support/package.json`, `metadata.json`, `daemon-protocol.ts`, `README.md`, `test.mjs`.
- `script/publish-npm-packages` and `script/fixtures/` -- packed-consumer assertions for the new DTO, metadata, and fixture.
- `examples/event-plane-producer/botster-package.json`, `plugin.lua`.
- `tests/hub_daemon_lifecycle/package_event_plane.rs` -- projection, session-subject delivery, and text-resolution tests.
- `docs/client-protocol.md` -- record the revision bump and the new package field.

## Risks

- R1. Published npm bytes are immutable. Mutating `0.3.2` or `0.1.39` breaks registry trust. Mitigation: bump both versions and regenerate every checksum surface.
- R2. Adding a public `DaemonPackage` field is source-breaking for external Rust consumers that build the struct with a literal. Mitigation: run the scratch patch-redirect probe against `botster-tui` and record the exact cost.
- R3. Fixture byte changes propagate into `metadata.json` checksums and the support matrix. Mitigation: run both package sync checks, the generated-asset equality tests, and the packed-consumer assertions.
- R4. A concurrent branch could also claim conformance revision `45` for different bytes. Mitigation: U1 requires a uniqueness check before selection.
- R5. Session-only scope makes the Project Pipelines notice declaration impossible until that package adds a `subject` field to `question.opened`. Mitigation: the downstream consumer list records this obligation; Hub does not weaken admission to accommodate it.
- R6. Scope creep into client reaction behavior. Mitigation: the plan ships declaration, admission, projection, and one shared resolution routine only.
- R7. Two fixture packages change in one run. Mitigation: each has one distinct duty. The `plugin-contract-matrix` fixture proves the published client ABI; `event-plane-producer` proves live delivery. Neither depends on a product plugin, per [[event plane client proof uses library contract fixtures]].
- R8. The new Git tag does not exist until after merge. Mitigation: U2 records the maintainer step, and the root patch keeps the workspace building.

## Acceptance checks and tests

Contract and admission:

1. `cargo test -p botster-ui-contract` -- validation accepts a well-formed declaration and rejects each of: empty name, wildcard name, wildcard owner, a pointer without a leading `/`, a two-segment pointer, a pointer with a trailing `~`, a pointer with an unknown `~x` escape, an over-long segment, `ttl_ms` below 1,000, `ttl_ms` above 60,000, and a duplicate `(owner, name)`.
2. `cargo test -p botster-ui-contract` -- `resolve_notice_text` returns the decoded string for `/notice` and for an escaped segment, and returns a typed error for a missing property, a non-string value, an empty or whitespace-only string, and a string over `NOTICE_TEXT_MAX_BYTES`.
3. `PackageNoticeSubjectScope` has exactly one variant, and the generated schema and TypeScript union list only `"session"`.
4. `cargo test -p botster-ui-contract --test generated_assets_test` -- the checked-in `index.d.ts`, `schema.json`, and `conformance-fixtures.json` match the generator, and the new fixtures deserialize through the Rust authority.
5. Hub admission unit tests in `src/packages.rs`: a foreign `owner` is rejected; an unadmitted `name` is rejected; an emitted event without the `clients` audience is rejected; a schema without a `subject` property is rejected; a pointer whose decoded segment names no declared string property is rejected; a valid declaration is admitted.

Projection and generated protocol:

6. A `client_api` test proves `into_descriptor` sets `owner` to the admitted package name, and that no code path can produce a descriptor without an owner.
7. A `botster-hub-client` TypeScript test proves the generated artifact declares `owner: string;` as required on `PackageNoticeReactionDescriptor`, carries `notice_reactions?: PackageNoticeReactionDescriptor[]` on `DaemonPackage`, imports the type from `@trybotster/ui-contract`, and still satisfies `generated_typescript_protocol_matches_checked_artifact`.
8. A serde test proves the field is omitted when empty and round-trips when present, per [[generated typescript dtos must encode serde field optionality]].

Live Hub proof (production path, not scaffold):

9. A new test in `tests/hub_daemon_lifecycle/package_event_plane.rs` installs `examples/event-plane-producer` through the real Hub binary, calls the daemon package listing, and asserts the returned `DaemonPackage.notice_reactions` carries owner `event-plane-producer`, name `sample.ready`, `subject_scope: session`, `text_pointer: /notice`, the declared `ttl_ms`, and the declared severity. This proves the client-visible entry point, not only that the type exists.
10. The same test subscribes with `subjects: [<current session subject>]`, emits one matching and one non-matching event, and asserts exactly one delivery.
11. The same test resolves the delivered payload through `resolve_notice_text` with the projected pointer and asserts a non-empty bounded string. It also asserts a typed error for a deliberately empty notice payload.
12. Existing named tests in `tests/hub_daemon_lifecycle/package_event_plane.rs` for subscribe, `package_event`, `event_gap`, reconnect, and queue limits pass unchanged. The implementer records the exact test names and results.

Package and downstream proof (answers `finding_1787283385_449120`):

13. `node packages/ui-contract/test.mjs` passes, and it covers `resolveNoticeText` against the shared conformance vectors.
14. `node packages/hub-test-support/scripts/sync-assets.mjs --check` reports no drift.
15. `node packages/hub-test-support/test.mjs` runs only after its dependency exists. The implementer installs the local UI contract first, for example `npm install --no-save --package-lock=false ../../<packed ui-contract tarball>` inside `packages/hub-test-support`. The plan records this setup because the bare command fails on a clean base with `ERR_MODULE_NOT_FOUND`.
16. Clean packaged-consumer proof, extending the existing flow at `script/publish-npm-packages:58-84`: `npm pack` both packages, create a scratch consumer directory outside the repository, `npm install --no-save --package-lock=false` both tarballs, then assert from the installed packages that `metadata.json` reports `package_version` `0.1.40`, `ui_contract.package_version` `0.3.3`, `conformance_fixture_revision` `45`, `protocol_version` `7`, and a `daemon_protocol.sha256` that matches the installed `daemon-protocol.ts`; that the installed `daemon-protocol.ts` contains `notice_reactions` and the required `owner` field; and that `fixtures/plugin-contract-matrix/botster-package.json` materializes with the notice declaration. Compile the strict TypeScript consumer against the installed packages.
17. Post-publication registry smoke stays the maintainer release gate, per [[hub test support npm releases need external consumer smoke]]. This run does not publish.
18. Downstream source-cost probe per [[scratch cargo patch redirects measure downstream dto breakage]]: a scratch `botster-tui` worktree with a temporary `[patch."https://github.com/trybotster/botster-hub.git"]` redirect to this candidate checkout, a separate `CARGO_TARGET_DIR`, then `cargo check --workspace` and `cargo check --workspace --all-targets`. Record every failing literal. Remove the scratch worktree afterwards.

Repository gates:

19. Build `botster-session-worker` and then `botster-hub` with locked commands before the suite, per [[Hub suite runs prebuild the session worker before the locked test wrapper]].
20. `cargo fmt --all -- --check`.
21. `cargo clippy --workspace --all-targets --locked -- -D warnings`.
22. `./test.sh --locked`.
23. Record the Hub binary SHA and the lockfile-pinned Core worker SHA separately, per [[live hub proof records distinct hub and locked core binary provenance]].

## Worktree hygiene

- Tracked `.gitignore` is present and 53 bytes at plan time. No restore is required.
- The worktree path contains no `:`. No `CARGO_TARGET_DIR` override is required for this run.

## Vault gaps worth capturing

- The version-one notice reaction contract: session-only scope, the fixed severity enum, the `ttl_ms` bound, the one-segment decoded pointer, and the non-empty bounded resolved string. Source: `question_1787283207_365510`.
- The selected cutover coordinates: `@trybotster/ui-contract` `0.3.3`, `@trybotster/hub-test-support` `0.1.40`, conformance revision `45`, and the Hub commit that publishes them.
- The gotcha that `node packages/hub-test-support/test.mjs` requires an installed `@trybotster/ui-contract` and fails on a clean checkout.
- The measured downstream source cost of the `DaemonPackage` field addition, and whether that cost justifies a later `#[non_exhaustive]` decision.
