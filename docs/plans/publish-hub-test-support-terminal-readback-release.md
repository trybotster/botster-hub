# Publish hub-test-support terminal readback release

## Context loaded

- Ticket `ticket_1783636761_760074`, run `run_1783639703_713662`, Plan step, gate prompt, artifacts, findings, questions, and prior answers were loaded through Project Pipelines. There are no prior artifacts, findings, questions, answers, or blocking dependencies for this run.
- Role and repo overlays: [[planner-playbook]], [[botster-planner-playbook]], [[botster-architecture]], [[cli-patterns]], and [[spa-patterns]].
- Package/protocol constraints: [[hub test support npm releases need external consumer smoke]], [[botster web generated protocol drift checks need explicit hub artifact paths]], [[generated typescript dtos must encode serde field optionality]], [[published capability matrices must derive enumerations from source]], [[adding a hub client feature constant is a three site change]], [[daemon event shape changes bump conformance fixture revision not protocol version]], and [[test script required for rust tests not cargo test]].
- Pipeline constraints: [[project pipeline orchestration belongs in a device-level botster plugin]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[plan agents must author vault context as wikilinks not home paths]], [[botster orchestration should spawn agents with explicit target ids]], and [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repository baseline is hub `main` after PR #128. The authoritative generated artifact at `crates/botster-hub-client/generated/daemon-protocol.ts` already contains `read_screen`, `capture_snapshot`, `DaemonReadScreen`, and `DaemonCaptureSnapshot`. The checked-in npm copy does not.
- `first_party_client_support_matrix()` already derives `required_features` and `supported_features` from the authoritative compatibility descriptors, including `terminal_readback`. The same Rust crate already owns `late_attach_history_conformance_fixture_json()` with positive history-before-live and no-history sequences. These existing source-backed values are the canonical release inputs; the npm layer must not duplicate their contents by hand.
- Registry and drift evidence captured during planning:
  - `npm view @trybotster/hub-test-support version dist-tags --json --cache <temporary-cache> --prefer-online` reports `0.1.2` and `latest=0.1.2`.
  - `node packages/hub-test-support/scripts/sync-assets.mjs --check` fails because `daemon-protocol.ts` and `metadata.json` are stale.
  - A repo search finds no npm-published `terminal_readback` token or late-attach fixture surface today.

Botster layers touched are the public hub-client protocol artifact, Rust hub test-support packaging adapter, Node test-support package, package documentation, and npm release boundary. No plugin, Lua core, TUI, React SPA, Rails relay, MCP, terminal runtime, or session-worker behavior changes are planned.

## Scope

1. Publish the already-landed terminal readback protocol through the generated Node package.
   - Regenerate `packages/hub-test-support/daemon-protocol.ts` from `botster_hub_client::daemon_protocol_typescript()` so it contains the request variants, response fields/kinds, and `DaemonReadScreen` / `DaemonCaptureSnapshot` DTOs from PR #128.
   - Keep `crates/botster-hub-client` as the only DTO source of truth. Do not hand-edit the package artifact.
2. Publish the existing source-derived compatibility/support descriptor.
   - Extend the Rust Node asset emitter to serialize the existing `first_party_client_support_matrix()` as a package asset rather than creating a second feature list.
   - Include that asset in generated metadata with an artifact path and checksum, and expose it through the package's main JavaScript/TypeScript API. Tests must prove both required and supported feature lists contain `terminal_readback`.
3. Publish the existing late-attach/history conformance fixture.
   - Extend the same Rust emitter to serialize `late_attach_history_conformance_fixture_json()` as a package asset.
   - Include the asset in metadata/checksum verification and expose a typed/readable package API. Preserve the source fixture's history-before-live ordering, renderable `data` with matching `bytes`, and separate no-history sequence without fabricated Snapshot/Scrollback events.
4. Cut the next patch release.
   - Bump `@trybotster/hub-test-support` and generated metadata/documentation from `0.1.2` to `0.1.3`, provided registry preflight still shows that version is unused.
   - Synchronize all Rust-emitted assets, update package tests and README usage, pack and test the exact tarball, then publish it publicly.
   - Verify the installed public registry coordinate from a clean external consumer, not from the workspace or a sibling checkout.
5. Leave a durable implementation report with the published coordinate, registry metadata, commands/results, and any deviations so the web follow-up can consume the release without local overrides.

## Non-scope

- No new hub runtime behavior, readback semantics, daemon request/response design, feature negotiation policy, protocol version, or conformance revision change. PR #128 already supplied the runtime and source DTO behavior.
- No hand-authored browser DTO mirror and no alternate package-local protocol generator.
- No changes to terminal history production, SessionIo/ClientWorker data-plane ownership, PTY sizing, readback retention, or late-attach runtime ordering.
- No botster-web dependency bump or active client wiring. That remains the follow-up ticket named in this ticket; this release only makes the registry dependency consumable and proves the downstream-shaped path.
- No unrelated package refactor, generic asset framework, release automation, optional configuration, plugin fixture redesign, or adjacent documentation cleanup.

## Assumptions and unknowns

- Assumption: `0.1.3` is the intended next version because the registry currently reports `0.1.2` as latest. Immediately before mutation or publish, re-run the exact-version lookup. If `0.1.3` exists or the tag has moved, stop and ask a human rather than silently choosing another version or overwriting release evidence.
- Assumption: publishing is explicitly in scope and the implementation agent has npm credentials for the public `@trybotster` scope. Authentication failure is a blocker to acceptance, not grounds to call a packed tarball complete.
- Assumption: the feature descriptor means the existing `FirstPartyClientSupportMatrix` compatibility evidence, not a new package-specific terminal-readback schema. It already derives feature enumeration from `DaemonCompatibility::current()` and `DaemonCompatibilityRequirement::current()` and is the local architectural source of truth.
- Assumption: the late-attach fixture means the existing Rust serde JSON scenario, published intact for Node consumers. No event-shape or revision bump is necessary unless implementation discovers that emitted JSON differs from the current Rust fixture.
- Assumption: the pipeline remains bound to target `tgt_7e208a0c76a44980a83b63af976b1f22` and the assigned ticket worktree. All commands and edits must remain in that worktree; repo artifacts use path-neutral references.
- Unknown: the npm publisher's current authentication/provenance requirements. Resolve with `npm whoami` and a dry-run/preflight before publish; do not commit credentials or add registry-specific secrets.
- Unknown: whether an actual botster-web checkout is available to the implementation/verification agent. The required hub-side proof is a clean external install with no sibling override; if web is available, additionally run its drift command against the installed dependency. The follow-up web dependency change remains out of scope.

## Affected surfaces and files

Expected required changes:

- `crates/botster-hub-test-support/examples/node_package_assets.rs`
  - Emit the source-derived first-party support matrix and late-attach fixture JSON beside the generated daemon protocol.
- `packages/hub-test-support/scripts/sync-assets.mjs`
  - Copy/check the new emitted JSON assets, calculate checksums, and place their descriptors in generated metadata.
- `packages/hub-test-support/package.json`
  - Bump to `0.1.3`; include/export the new assets only as needed for the documented main API.
- `packages/hub-test-support/daemon-protocol.ts`
  - Regenerated output containing terminal readback request/response DTOs.
- `packages/hub-test-support/metadata.json`
  - Regenerated `0.1.3` metadata, protocol checksum, source-derived support-matrix descriptor, and late-attach fixture descriptor.
- New generated package assets under `packages/hub-test-support/`, with stable descriptive names for the first-party support matrix and late-attach history fixture.
- `packages/hub-test-support/index.js` and `packages/hub-test-support/index.d.ts`
  - Minimal path/read APIs and types for the newly published assets; extend `verifyPackageAssets()` to cover their checksums.
- `packages/hub-test-support/test.mjs`
  - Assert terminal readback DTO tokens, source-derived feature membership, fixture ordering/no-history behavior, version, and asset verification.
- `packages/hub-test-support/README.md` and `docs/client-protocol.md`
  - Document `0.1.3`, the new package surfaces, registry-first consumption, and the unchanged runtime/data-plane semantics.
- A repo-visible implementation report under `docs/reports/` with publish and external-consumer evidence.

Only if required by test failure or source truth:

- `crates/botster-hub-test-support/src/lib.rs` for narrowly scoped assertions or a public serializable wrapper needed by the emitter. Do not change the existing feature list or fixture values merely to accommodate Node packaging.
- `crates/botster-hub-client/src/typescript.rs`, its checked artifact, and associated tests only if the authoritative generator is found incomplete relative to the already-defined Rust serde DTOs. Current inspection shows the DTOs are present, so no generator change is expected.

## Runtime and consumer path

The production release path is:

`botster-hub-client` Rust serde DTOs and compatibility descriptors -> `botster-hub-test-support` Rust asset emitter -> synchronized files in `@trybotster/hub-test-support` -> packed/published npm artifact -> clean downstream install -> botster-web protocol drift generation and later client wiring.

This ticket changes the registry-consumer path, not the running hub path. Success requires installing the published coordinate and observing the new DTOs/descriptors/fixture from `node_modules`; repository file existence alone is insufficient.

## Risks

- Stale generated assets: changing only `package.json` would publish 0.1.2-era protocol content. The sync check and installed-package token assertions are mandatory.
- Tautological feature metadata: a package-local literal could drift from the daemon descriptor. Serialize the existing support matrix and assert `terminal_readback` in both required and supported lists.
- Fixture drift: hand-authored JSON can diverge in field names, event order, byte counts, or no-history behavior. Emit the existing serde value and checksum it.
- Partial packaging: workspace tests can see unlisted files that npm omits. Inspect `npm pack --dry-run --json`, install the actual tarball, then repeat against the published coordinate.
- Version race or immutable-version conflict: the registry may change between Plan and Implement. Recheck `0.1.3` immediately before publish and escalate on collision.
- Irreversible bad publish: `npm publish` cannot be treated like a local test. Require clean tree, synchronized assets, package tests, Rust tests, tarball smoke, exact version preflight, and authenticated publisher before publishing.
- False downstream proof: a sibling path or `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` override can mask package omissions. The final consumer smoke must install from npm and run without sibling overrides.
- Cross-crate regression: the terminal feature constant changes both advertised and required features and the support-matrix snapshot. Run the repo wrapper at workspace scope, not only the hub-client crate.
- Checklist visibility: the standard vault checklist creation timed out during Plan. Preserve all checklist evidence in this document, gate evidence, and advancement request per the documented fallback.

## Acceptance checks and tests

Before publish:

1. Registry/auth preflight:
   - `npm whoami`
   - `npm view @trybotster/hub-test-support version dist-tags --json --cache <temporary-cache> --prefer-online`
   - `npm view @trybotster/hub-test-support@0.1.3 version --cache <temporary-cache> --prefer-online` must show the version is unused. Any conflicting result requires a human decision.
2. Generation and Node package checks:
   - Run `node packages/hub-test-support/scripts/sync-assets.mjs`, then `node packages/hub-test-support/scripts/sync-assets.mjs --check`.
   - Run `node packages/hub-test-support/test.mjs` (or `npm test` from the package directory).
   - Assert the generated protocol contains `read_screen`, `capture_snapshot`, `DaemonReadScreen`, and `DaemonCaptureSnapshot` with the expected request/response shapes.
   - Assert the emitted matrix contains `terminal_readback` in source-derived required and supported features.
   - Assert the emitted late-attach fixture has restored history before live output, `bytes === Buffer.byteLength(data)` for history events, and no fabricated history in the no-history sequence.
3. Rust contract checks through the repo wrapper:
   - `./test.sh -p botster-hub-client`
   - `./test.sh -p botster-hub-test-support`
   - `./test.sh` as the workspace-scoped gate required for feature-list/support-matrix changes. Record exact failures and prove any claimed baseline failure is unrelated; do not waive the suite generically.
4. Package-content proof:
   - Run `npm pack --dry-run --json` from `packages/hub-test-support` and confirm the generated protocol, metadata, support matrix, late-attach fixture, API, types, README, license, and plugin fixture are listed.
   - Create the real tarball with a temporary npm cache, install it into a clean temporary Node consumer, import the main package, assert version `0.1.3`, run `verifyPackageAssets()`, read both new JSON assets, and assert all terminal-readback/late-attach tokens and invariants.
   - Scan committed package/docs/report content for credentials, local home paths, sibling-checkout requirements, and stale `0.1.2` install instructions.

Publish and post-publish:

5. Publish exactly the verified tarball sources with `npm publish --access public` from `packages/hub-test-support` after all preconditions pass.
6. Verify registry state with `npm view @trybotster/hub-test-support@0.1.3 version dist.tarball dist.integrity license --json --cache <temporary-cache> --prefer-online` and confirm the latest tag disposition intended by the release.
7. In a second clean directory, install `@trybotster/hub-test-support@0.1.3` from the public registry with no file dependency, sibling checkout, or protocol override. Repeat the metadata, checksum, DTO token, feature descriptor, and late-attach fixture assertions against the installed package.
8. If botster-web is available, bump only a disposable verification checkout to `0.1.3` and run its protocol drift check without `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` or sibling overrides. Otherwise, record that the actual dependency bump is the named follow-up ticket and attach the clean external consumer proof that unblocks it.
9. Confirm the git diff contains only lines traceable to generated asset publication, package API/tests/docs, versioning, and release evidence. Commit the implementation/report and attach the published coordinate to Project Pipelines.

## Pipeline gates and artifacts

- Plan artifact: this document.
- Plan gate: context, bounded scope/non-scope, assumptions/unknowns, affected files, risks, acceptance commands, and vault-gap disposition.
- Implement artifact: committed package changes plus a report containing generation, Rust/Node, tarball, registry, and external-consumer evidence.
- Review/Verify must reject a workspace-only or tarball-only result as incomplete because the ticket explicitly requires a public npm release.
- The product decision ledger for this bounded release is: default version `0.1.3` while unused; no protocol/revision change; no web wiring; no alternate DTO source; ask a human on version collision, incompatible npm policy, fixture-shape divergence, or any need to waive the public-release acceptance.

## Vault workflow evidence and gaps worth capturing

- Notes read are listed under Context loaded. They constrain source ownership, source-derived enumerations, wrapper-based Rust testing, external npm consumer proof, pipeline artifacts, and path-neutral documentation.
- Convention conflicts: none. The plan uses existing Rust/package primitives, keeps the public protocol in `botster-hub-client`, avoids a new abstraction, and limits changes to the release/consumer boundary.
- Verification evidence already collected: registry latest is `0.1.2`; npm sync check currently fails on stale protocol and metadata; the Rust generated DTO and support/fixture sources are present; npm exports do not yet expose `terminal_readback` or late-attach fixture content.
- Checklist persistence: `project_pipelines_create_vault_checklist` returned `plugin worker invoke timeout`, so this document and gate evidence are the required fallback record.
- Durable capture candidate: if implementation establishes a reusable rule for publishing whole Rust conformance JSON assets through this npm adapter (including checksum/API conventions), capture that rule after the release. If the work only applies the already-documented external-consumer and source-derived-matrix rules, no new vault note is needed.
