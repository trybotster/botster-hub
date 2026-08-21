# Publish a package-owned client notice reaction descriptor

Ticket: `ticket_1787278643_145174`
Run: `run_1787282470_625000`
Target repository: `botster-hub` (`tgt_7e208a0c76a44980a83b63af976b1f22`, `https://github.com/trybotster/botster-hub.git`)
Base: `origin/main` at `b3b54f1`

## Repository playbook loaded

- [[botster-hub-playbook]] (exact ownership charter for the ticket target repository)

## Other playbooks and atomic notes loaded

Role playbooks:

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-client-playbook]] (the public `DaemonPackage` DTO lives in `crates/botster-hub-client`)

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
- [[git-visible Hub member manifests must use the UI contract tag]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[public dto field additions are source breaking without non exhaustive]]
- [[scratch cargo patch redirects measure downstream dto breakage]]
- [[generated typescript dtos must encode serde field optionality]]
- [[vault example paths are not repository placement conventions]]

Runtime-teardown class: does not apply. This ticket changes a manifest contract, an admission rule, and a sanitized projection. It changes no peer lifecycle, no `SessionIo`/`ClientWorker` teardown, and no resource-spin path. [[botster runtime teardown lenses]] is therefore not loaded.

## Context loaded

Current repository state at `b3b54f1`:

- `src/packages.rs:80-150` defines `HubPackageManifest`, `HubPackageEvents`, `HubEmittedEvent`, and `validate_event_contracts`.
- `src/package_event_schema.rs:60-88` compiles the bounded payload schema subset and exposes `spec()`.
- `src/package_event_router.rs:1189-1219` filters client delivery by exact `payload.subject` membership.
- `src/client_api.rs:1381-1396` defines the sanitized `HubClientPackage`. It has no event or notice field.
- `src/daemon_transport.rs:7319` maps `HubClientPackage` into `DaemonPackage`.
- `crates/botster-hub-client/src/lib.rs:1916-1941` defines `DaemonPackage`. It has no event or notice field and is not `#[non_exhaustive]`.
- `crates/botster-hub-client/src/typescript.rs:13,992-1016` emits the `DaemonPackage` TypeScript interface and imports `PackageSurfaceDescriptor` from `@trybotster/ui-contract`.
- `crates/botster-ui-contract/src/lib.rs:45-227` defines `PackageSurfaceDescriptor` and `validate_package_presentation`.
- `crates/botster-ui-contract/examples/generate_assets.rs` regenerates `packages/ui-contract/index.d.ts`, `schema.json`, and `conformance-fixtures.json`.
- `packages/ui-contract/package.json` is version `0.3.2`. `packages/ui-contract/index.js:5` repeats that version.
- `packages/hub-test-support/package.json` is version `0.1.39` and depends on `@trybotster/ui-contract` `0.3.2`.
- `packages/hub-test-support/metadata.json` records protocol `7`, conformance revision `44`, and a `daemon-protocol.ts` checksum.
- `crates/botster-hub-client/src/lib.rs:30-31` sets `PROTOCOL_VERSION = 7` and `CONFORMANCE_FIXTURE_REVISION = 44`.
- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/` is the Hub-owned, product-free ABI fixture package that the npm package ships to clients.
- `examples/event-plane-producer/` is the checked-in Hub-owned event producer used by `tests/hub_daemon_lifecycle/package_event_plane.rs`.
- CI gates are `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, and `./test.sh --locked`.

## Scope

In scope:

1. Add `PackageNoticeReactionDescriptor`, `PackageNoticeSubjectScope`, `PackageNoticeSeverity`, and `validate_package_notice_reactions` to `crates/botster-ui-contract`.
2. Admit `events.notices` on `HubPackageManifest` next to `events.emitted`, with Hub-owned admission rules.
3. Project the admitted descriptor through `HubClientPackage` and `DaemonPackage`.
4. Emit the descriptor in the generated daemon protocol TypeScript artifact.
5. Republish the contract through new unpublished `@trybotster/ui-contract` and `@trybotster/hub-test-support` versions, including regenerated assets and metadata.
6. Declare the descriptor in two Hub-owned fixture packages, so both the published client ABI fixture and the live Hub daemon test exercise it without any product plugin.
7. Add admission, projection, generated-artifact, and live daemon tests.

Out of scope (non-scope):

- Core `PackageManifest`. Core stays the policy-free execution shape.
- Client rendering, toast presentation, or notice state machines. `botster-web` and `botster-tui` own those.
- Removing the hardcoded product constants in Web and TUI. Tickets `ticket_1787278327_274484` and `ticket_1787278327_199618` own that removal.
- The Project Pipelines package declaration and emitter.
- Durable attention state, entity families, entity joins, or workflow correlation rules.
- npm publication itself and the `botster-ui-contract-v0.3.3` Git tag creation. The maintainer runs `script/tag-ui-contract` on merged `main`.
- Any change to subscribe, `package_event`, `event_gap`, reconnect, or queue-limit behavior.

## Botster layers touched

- Hub package admission and sanitized projection (`src/`).
- Public client DTO and generated TypeScript (`crates/botster-hub-client`).
- Shared UI/client contract crate and npm package (`crates/botster-ui-contract`, `packages/ui-contract`).
- Test support npm package and its ABI fixture (`crates/botster-hub-test-support`, `packages/hub-test-support`).
- Hub-owned example plugin package (`examples/event-plane-producer`).

## Design

### Descriptor shape (ui-contract owned)

```rust
pub struct PackageNoticeReactionDescriptor {
    pub owner: Option<String>,               // omitted in the manifest; Hub materializes it
    pub name: String,                        // exact admitted event name
    pub subject_scope: PackageNoticeSubjectScope,
    pub text_pointer: String,                // RFC 6901 pointer, for example "/notice"
    pub ttl_ms: u32,
    pub severity: PackageNoticeSeverity,
}

pub enum PackageNoticeSubjectScope { Session, None }     // "session" | "none"
pub enum PackageNoticeSeverity { Info, Warning, Error }  // "info" | "warning" | "error"
```

`validate_package_notice_reactions` enforces the shared shape rules:

- `name` is non-empty and contains no `*` or `?`.
- `owner`, when present, is non-empty and contains no wildcard.
- `text_pointer` is one RFC 6901 segment that starts with `/` and is at most 64 UTF-8 bytes.
- `ttl_ms` is within `1_000..=60_000`.
- No two descriptors share the same resolved `(owner, name)` pair.

The descriptor carries no entity family, no field path beyond the text pointer, and no workflow join. Context targeting uses `subject_scope` over the existing `payload.subject` mechanism only.

### Hub admission rules (`src/packages.rs`)

`validate_event_contracts` gains the notice rules after the emitted-event rules:

- Call `validate_package_notice_reactions` first.
- Reject a descriptor whose `owner` is present and does not equal `self.name`.
- Reject a descriptor whose `name` does not appear in `self.events.emitted`.
- Reject a descriptor whose matching emitted event does not include the `clients` audience.
- Reject a `text_pointer` whose single segment does not name a declared string property of the matching admitted `payload_schema`.
- Reject `subject_scope: session` when the matching admitted `payload_schema` declares no `subject` property, because Hub filters only on `payload.subject`.

The reserved package name `hub` is already rejected before these checks.

### Projection

- `HubClientPackage` gains `notice_reactions: Vec<PackageNoticeReactionDescriptor>`. `from_record` materializes `owner` from the admitted package name, so every projected descriptor carries the exact owner.
- `DaemonPackage` gains `notice_reactions: Vec<PackageNoticeReactionDescriptor>` with `#[serde(default, skip_serializing_if = "Vec::is_empty")]`.
- `daemon_package_from_client` copies the projected vector.
- `typescript.rs` adds `("notice_reactions?", "PackageNoticeReactionDescriptor[]")` and imports the type from `@trybotster/ui-contract` next to `PackageSurfaceDescriptor`.

### Version cutover

- `@trybotster/ui-contract` `0.3.2` -> `0.3.3`. `0.3.2` is published and immutable.
- Both Git-consumed member manifests move to `tag = "botster-ui-contract-v0.3.3"`, and the root `[patch]` comment records the new tag.
- `@trybotster/hub-test-support` `0.1.39` -> `0.1.40`, with `@trybotster/ui-contract` dependency `0.3.3`.
- `CONFORMANCE_FIXTURE_REVISION` `44` -> `45`. `PROTOCOL_VERSION` stays `7`, because the added field is optional and an unchanged client still deserializes the package row.

### Fixtures

- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/botster-package.json` gains one emitted event and one notice reaction, plus a README paragraph. This is the published, product-free ABI fixture that Web and TUI conformance harnesses read.
- `examples/event-plane-producer/botster-package.json` gains `subject` and `notice` properties on the `sample.ready` schema and one `session`-scoped notice reaction. `plugin.lua` gains an emit path that sets both fields, so the Hub daemon test proves declaration, projection, subscription, and delivery in one live run.

## Repository ownership boundaries and cross-repository dependencies

- `botster-core` is untouched. Package event and notice policy lives on `HubPackageManifest`, per [[package event contracts live on HubPackageManifest not Core PackageManifest]].
- `botster-ui-contract` owns the renderer-neutral descriptor vocabulary and its shared shape validation, per [[botster package surface semantics live in ui contract while hub owns admission]].
- `botster-hub` owns admission, the reserved-owner rule, the emitted-event cross-check, and the sanitized projection.
- `botster-hub-client` owns the public `DaemonPackage` DTO, the generated TypeScript, and the conformance revision.
- Generic client reaction rendering stays with `botster-web` and `botster-tui`.

Cross-repository dependencies: none block this run. This ticket produces the contract that the downstream tickets consume.

Downstream consumers (no dependency edges to register against this run):

- `botster-project-pipelines`: package declaration and emitter.
- `botster-web`: `ticket_1787278327_274484`.
- `botster-tui`: `ticket_1787278327_199618`.

Consumer tickets after a Hub session-type eligibility parent: not applicable. This ticket has no session-type eligibility parent, and `blocking_dependencies` is empty.

## Assumptions and unknowns

- A1. `subject_scope` has exactly two values, `session` and `none`. Evidence: [[question opened clients subscribe with empty subjects]] shows an admitted event with no `subject` property, so a session-only vocabulary would make that downstream declaration unexpressible. `none` compiles to an empty subject set, which accepts every live event for the exact owner and name.
- A2. Severity vocabulary is `info`, `warning`, `error`. No existing Botster severity enum exists in `botster-ui-contract`; `UiVariant` is an emphasis token, not a severity.
- A3. TTL is `ttl_ms` bounded to `1_000..=60_000`. The field is a client display duration, not a delivery guarantee.
- A4. The descriptor key is `(owner, name)`. One admitted event carries at most one notice reaction.
- A5. `text_pointer` is one RFC 6901 segment, for example `/notice`. A nested pointer is rejected. This keeps the descriptor bounded and matches the ticket example. Plan Review can widen this to a bounded depth if a downstream declaration needs a nested payload.
- A6. `DaemonPackage` does not become `#[non_exhaustive]` in this run. The implementer measures the downstream source cost first, per [[public dto field additions are source breaking without non exhaustive]], and records it for the consumer tickets.
- U1. The exact npm coordinates `0.3.3` and `0.1.40` are unused. The implementer must confirm against the registry before selecting them, per [[Hub test support capability cutovers use a new unpublished package version]] and [[an unmerged run that publishes an npm coordinate burns it]].
- U2. The `botster-ui-contract-v0.3.3` tag does not exist until the maintainer runs `script/tag-ui-contract` on merged `main`. The root `[patch]` entry keeps this workspace resolvable in the meantime, per [[git-visible Hub member manifests must use the UI contract tag]].

## Affected surfaces and files

- `crates/botster-ui-contract/src/lib.rs` -- descriptor types, validation, schema and TypeScript declaration entries.
- `crates/botster-ui-contract/src/assets.rs` -- wire enums, JSON schema, conformance fixture entry.
- `crates/botster-ui-contract/tests/ui_contract_test.rs` -- validation unit tests.
- `crates/botster-ui-contract/tests/generated_assets_test.rs` -- fixture deserialization coverage.
- `packages/ui-contract/index.d.ts`, `schema.json`, `conformance-fixtures.json` -- regenerated.
- `packages/ui-contract/index.js`, `package.json`, `README.md` -- version and documentation.
- `src/packages.rs` -- `HubPackageEvents.notices` and admission rules.
- `src/client_api.rs` -- `HubClientPackage.notice_reactions` and `from_record`.
- `src/daemon_transport.rs` -- `daemon_package_from_client`.
- `crates/botster-hub-client/src/lib.rs` -- `DaemonPackage.notice_reactions`, `CONFORMANCE_FIXTURE_REVISION`.
- `crates/botster-hub-client/src/typescript.rs` -- import list and `DaemonPackage` interface.
- `crates/botster-hub-client/generated/daemon-protocol.ts` -- regenerated.
- `crates/botster-hub-client/Cargo.toml`, `crates/botster-hub-test-support/Cargo.toml`, `Cargo.toml` -- UI contract tag.
- `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/botster-package.json`, `README.md`.
- `crates/botster-hub-test-support/src/lib.rs` -- fixture assertions for the new field, where the contract matrix helper already checks package rows.
- `packages/hub-test-support/package.json`, `metadata.json`, `daemon-protocol.ts`, `README.md` -- regenerated and versioned.
- `examples/event-plane-producer/botster-package.json`, `plugin.lua`.
- `tests/hub_daemon_lifecycle/package_event_plane.rs` -- projection and session-subject delivery tests.
- `docs/client-protocol.md` -- record the revision bump and the new package field.

## Risks

- R1. Published npm bytes are immutable. Mutating `0.3.2` or `0.1.39` breaks registry trust. Mitigation: bump both versions and regenerate every checksum surface.
- R2. Adding a public `DaemonPackage` field is source-breaking for external Rust consumers that build the struct with a literal. Mitigation: run the scratch patch-redirect probe against `botster-tui` and record the exact cost.
- R3. Fixture byte changes propagate into `metadata.json` checksums and the support matrix. Mitigation: run both package sync checks and the generated-asset equality tests.
- R4. The admission cross-check could reject an already-installed package that declares notices without a matching emitted event. Mitigation: no first-party package declares notices today, and admission failure returns a typed error rather than dropping transport.
- R5. Scope creep into client reaction behavior. Mitigation: the plan ships declaration, admission, and projection only; the delivery test asserts unchanged event-plane behavior.
- R6. Two fixture packages change in one run. Mitigation: each has one distinct duty. The `plugin-contract-matrix` fixture proves the published client ABI; `event-plane-producer` proves live delivery. Neither depends on a product plugin, per [[event plane client proof uses library contract fixtures]].
- R7. The new Git tag does not exist until after merge. Mitigation: U2 records the maintainer step, and the root patch keeps the workspace building.

## Acceptance checks and tests

Contract and admission:

1. `cargo test -p botster-ui-contract` -- validation accepts a well-formed descriptor and rejects each of: empty name, wildcard name, wildcard owner, non-pointer text, multi-segment pointer, out-of-range `ttl_ms`, duplicate `(owner, name)`.
2. `cargo test -p botster-ui-contract --test generated_assets_test` -- the checked-in `index.d.ts`, `schema.json`, and `conformance-fixtures.json` match the generator, and the new fixture deserializes through the Rust authority.
3. Hub admission unit tests in `src/packages.rs`: a foreign `owner` is rejected; an unadmitted `name` is rejected; an emitted event without the `clients` audience is rejected; `subject_scope: session` without a schema `subject` property is rejected; a text pointer that names no declared string property is rejected; a valid declaration is admitted.

Projection and generated protocol:

4. A `client_api` test proves `HubClientPackage.notice_reactions` materializes the exact owner from the admitted package name.
5. A `botster-hub-client` TypeScript test proves the generated artifact contains `notice_reactions?: PackageNoticeReactionDescriptor[];` and the `@trybotster/ui-contract` import, and `generated_typescript_protocol_matches_checked_artifact` still passes.
6. A serde test proves the field is omitted when empty and round-trips when present, per [[generated typescript dtos must encode serde field optionality]].

Live Hub proof (production path, not scaffold):

7. A new test in `tests/hub_daemon_lifecycle/package_event_plane.rs` installs `examples/event-plane-producer` through the real Hub binary, calls the daemon package listing, and asserts the returned `DaemonPackage.notice_reactions` carries owner `event-plane-producer`, name `sample.ready`, `subject_scope: session`, `text_pointer: /notice`, the declared `ttl_ms`, and the declared severity. This proves the client-visible entry point, not only that the type exists.
8. The same test subscribes with `subjects: [<current session subject>]`, emits one matching and one non-matching event, and asserts exactly one delivery.
9. Existing named tests in `tests/hub_daemon_lifecycle/package_event_plane.rs` for subscribe, `package_event`, `event_gap`, reconnect, and queue limits pass unchanged. The implementer records the exact test names and results.

Package and downstream proof:

10. `node packages/ui-contract/test.mjs` and `node packages/hub-test-support/test.mjs` pass; `node packages/hub-test-support/scripts/sync-assets.mjs --check` reports no drift.
11. `metadata.json` reports `conformance_fixture_revision: 45`, `protocol_version: 7`, `package_version: 0.1.40`, `ui_contract.package_version: 0.3.3`, and a regenerated `daemon-protocol.ts` checksum.
12. Downstream source-cost probe per [[scratch cargo patch redirects measure downstream dto breakage]]: a scratch `botster-tui` worktree with a temporary `[patch."https://github.com/trybotster/botster-hub.git"]` redirect to this candidate checkout, a separate `CARGO_TARGET_DIR`, then `cargo check --workspace` and `cargo check --workspace --all-targets`. Record every failing literal. Remove the scratch worktree afterwards.

Repository gates:

13. Build `botster-session-worker` and then `botster-hub` with locked commands before the suite, per [[Hub suite runs prebuild the session worker before the locked test wrapper]].
14. `cargo fmt --all -- --check`.
15. `cargo clippy --workspace --all-targets --locked -- -D warnings`.
16. `./test.sh --locked`.
17. Record the Hub binary SHA and the lockfile-pinned Core worker SHA separately, per [[live hub proof records distinct hub and locked core binary provenance]].

## Worktree hygiene

- Tracked `.gitignore` is present and 53 bytes at plan time. No restore is required.
- The worktree path contains no `:`. No `CARGO_TARGET_DIR` override is required for this run.

## Vault gaps worth capturing

- The selected cutover coordinates: `@trybotster/ui-contract` `0.3.3`, `@trybotster/hub-test-support` `0.1.40`, conformance revision `45`, and the Hub commit that publishes them.
- A convention note for the notice reaction descriptor field set, the two-value `subject_scope` vocabulary, and the single-segment text pointer bound.
- The measured downstream source cost of the `DaemonPackage` field addition, and whether that cost justifies a later `#[non_exhaustive]` decision.
