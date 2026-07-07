# Publish updated hub-test-support artifact with navigation protocol DTOs

## Context loaded

- Pipeline context: ticket `ticket_1783383679_865031`, run `run_1783383694_756016`, active step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, reviews, open questions, or answers were present.
- Required role context: [[identity]], [[goals]], [[planner-playbook]], and [[botster-planner-playbook]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], and [[plan agents must author vault context as wikilinks not home paths]].
- Repo context inspected:
  - `crates/botster-hub-client/src/lib.rs`, `src/typescript.rs`, and `generated/daemon-protocol.ts` already contain `list_package_navigation`, `package_navigation`, `DaemonPackageNavigationEntry`, and `DaemonPackageNavigationSource`.
  - `packages/hub-test-support/daemon-protocol.ts` and `metadata.json` are synced to the current generated protocol and include navigation DTOs.
  - `packages/hub-test-support/package.json` and `metadata.json` still declare package version `0.1.0`.
  - `packages/hub-test-support/README.md` and `docs/client-protocol.md` still tell clients to pin `@trybotster/hub-test-support@0.1.0`.
  - `docs/reports/publish-installable-distribution-coordinate-for-hub-test-support-implement-report.md` confirms `0.1.0` was already published and is immutable.
  - `docs/reports/expose-admitted-package-navigation-registry-and-plugin-iframe-asset-urls-from-hub-implement-report.md` confirms the navigation DTO implementation synced the package assets but did not publish a new npm artifact.
- Registry context: `npm view @trybotster/hub-test-support versions --json --cache /private/tmp/botster-npm-cache` returned only `["0.1.0"]`; `npm view @trybotster/hub-test-support version dist.tarball dist.integrity license --cache /private/tmp/botster-npm-cache` returned public `0.1.0`.
- Checklist evidence: run checklist `checklist_1783383753_329884` was created after a plugin worker timeout and will be updated with vault, convention, verification, and capture evidence.

## Scope

- Publish or otherwise make available a new Node-consumable `@trybotster/hub-test-support` coordinate that includes the current generated `daemon-protocol.ts` navigation DTOs.
- Preferred path: bump the npm package to `0.1.1`, update package metadata/docs to that exact version, pack and verify the tarball from a clean non-hub consumer, then publish `@trybotster/hub-test-support@0.1.1` to npm.
- If `0.1.1` is already published by implementation time, choose the next patch version only after verifying the registry state and documenting the reason.
- If npm publication is unavailable, ask a blocking human question before switching to the ticket's fallback path. The fallback must be a repo-approved durable installable coordinate or hosted `.tgz` artifact with checksum/version metadata, not a local `/tmp` or sibling checkout dependency.
- Update package docs and client protocol docs so botster-web has one exact dependency coordinate to pin.
- Verify the actual downstream path from a clean temp Node consumer by installing the documented coordinate and grepping or importing the installed protocol artifact for navigation DTOs.

## Non-scope

- Do not change botster-web code, lockfiles, tests, or vendored generated protocol in this ticket.
- Do not change daemon protocol semantics, navigation routing, plugin iframe behavior, Rust DTO shape, or conformance fixture semantics unless implementation discovers the package assets are not actually synced.
- Do not introduce a root Node workspace, bundler, release framework, or broad packaging abstraction.
- Do not document `file:`, sibling checkout, Project Pipelines worktree, hand-reconstructed `/tmp`, or committed local hub copy dependencies as the botster-web path.
- Do not republish or mutate `0.1.0`; npm versions are immutable.

## Assumptions and unknowns

- Assumption: `0.1.1` is the smallest correct version because `0.1.0` exists publicly and lacks the navigation DTOs needed by botster-web.
- Assumption: the current checked package assets are the desired artifact contents, because repo grep shows navigation DTOs in both `crates/botster-hub-client/generated/daemon-protocol.ts` and `packages/hub-test-support/daemon-protocol.ts`.
- Assumption: npm public publication remains the approved distribution path, matching the prior `0.1.0` publication.
- Unknown: whether the current environment has npm auth, org permissions, 2FA, or provenance requirements. Implementation must verify with `npm whoami` and the publish command result, and ask a human if auth blocks publication.
- Unknown: whether the published `0.1.0` tarball can be cheaply inspected during implementation to prove it lacks navigation DTOs. This is useful evidence but not required to justify a new immutable version because the ticket already identifies `0.1.0` as stale and the registry confirms it is the only version.
- Worktree/target assumption: implementation runs in this assigned pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.

## Affected surfaces/files

- Botster layers touched: Node package distribution surface, Rust-generated client protocol artifact boundary, docs, and package verification.
- Likely package files:
  - `packages/hub-test-support/package.json`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/README.md`
  - `packages/hub-test-support/test.mjs`
  - `packages/hub-test-support/daemon-protocol.ts`
  - `packages/hub-test-support/index.js`
  - `packages/hub-test-support/index.d.ts`
  - `packages/hub-test-support/LICENSE`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
- Likely docs:
  - `docs/client-protocol.md`
  - this plan artifact
  - an implementation report under `docs/reports/` with the exact published coordinate, integrity/checksum, and clean-consumer proof.
- Rust surfaces should be touched only if `node packages/hub-test-support/scripts/sync-assets.mjs --check` proves the package assets are stale:
  - `crates/botster-hub-client/generated/daemon-protocol.ts`
  - `crates/botster-hub-test-support/src/lib.rs`
  - `crates/botster-hub-test-support/examples/node_package_assets.rs`

## Risks

- Publishing the unchanged `0.1.0` version is impossible and would keep botster-web blocked. Mitigation: bump package and metadata to `0.1.1` before packing.
- Docs can drift from package metadata. Mitigation: update both `packages/hub-test-support/README.md` and `docs/client-protocol.md`, and grep for lingering `@trybotster/hub-test-support@0.1.0` or `"@trybotster/hub-test-support": "0.1.0"` in docs/package surfaces.
- Local package tests can pass while the registry artifact is stale or missing files. Mitigation: verify an actual packed tarball before publish and the actual registry coordinate after publish from a clean non-hub consumer.
- The package may include the generated DTOs but metadata checksums may still reference an older artifact after a manual version edit. Mitigation: run the sync script after version changes and verify `metadata.package_version` matches `package.json`.
- npm auth, 2FA, or org permissions can block publish. Mitigation: ask a blocking human question rather than switching silently to an unapproved coordinate.
- Package contents may leak local paths, tokens, or PII through docs, metadata, npm scripts, or generated artifacts. Mitigation: run a targeted scan before gate submission.

## Acceptance checks/tests

- Source/package sync:
  - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - `node packages/hub-test-support/test.mjs`
  - Verify `packages/hub-test-support/package.json` and `packages/hub-test-support/metadata.json` agree on the new version.
  - `rg -n "DaemonPackageNavigationEntry|DaemonPackageNavigationSource|list_package_navigation|package_navigation" packages/hub-test-support/daemon-protocol.ts`
- Rust package boundary:
  - `./test.sh -p botster-hub-client`
  - `./test.sh -p botster-hub-test-support`
- Prepublish package proof:
  - `npm view @trybotster/hub-test-support versions --json --cache /private/tmp/botster-npm-cache`
  - `npm pack --dry-run --json --cache /private/tmp/botster-npm-cache` from `packages/hub-test-support`, with evidence that `LICENSE`, `README.md`, `package.json`, `metadata.json`, `daemon-protocol.ts`, JS API, declarations, and fixtures are included.
  - `npm pack --json --cache /private/tmp/botster-npm-cache` from `packages/hub-test-support`.
  - From a fresh temp directory outside the hub repo: `npm init -y`, `npm install --save-dev <packed-tgz>`, then a Node ESM smoke that imports `@trybotster/hub-test-support`, calls `readDaemonProtocolTypescript()`, calls `verifyPackageAssets()`, materializes the plugin contract matrix fixture, verifies installed `LICENSE`, and asserts the protocol contains `DaemonPackageNavigationEntry`, `DaemonPackageNavigationSource`, `list_package_navigation`, and `package_navigation`.
- Publish and registry proof:
  - Publish the verified package with the registry-required command, expected default `npm publish --access public`.
  - `npm view @trybotster/hub-test-support@0.1.1 dist.tarball dist.integrity license version --cache /private/tmp/botster-npm-cache`, or the actual selected version if `0.1.1` was unavailable.
  - From another fresh temp directory outside the hub repo: `npm init -y`, `npm install --save-dev @trybotster/hub-test-support@0.1.1`, then the same Node ESM smoke against the installed registry package.
- Docs and handoff:
  - Docs state the exact botster-web pin, expected default `"@trybotster/hub-test-support": "0.1.1"`.
  - `rg -n "@trybotster/hub-test-support@0\\.1\\.0|\"@trybotster/hub-test-support\": \"0\\.1\\.0\"" packages/hub-test-support docs README.md` returns no stale install guidance, except historical reports if intentionally excluded from the scan.
  - Targeted PII/secret scan over changed docs/package files finds no home paths, npm tokens, auth tokens, or email addresses.

## Vault gaps worth capturing

- Capture after implementation if this repeats a durable release rule: Node-consumable hub test-support artifacts must bump npm version whenever generated daemon protocol DTOs change after a published immutable version.
- Capture after implementation if there is a useful standard smoke: clean external consumers should assert specific new DTO names, not only generic `DaemonRequest` and checksum success, when publishing a protocol-unblocking artifact.
- No convention conflict found at plan time. The plan follows the external client boundary, generated artifact source-of-truth, minimal tooling, path-neutral artifact, explicit target/worktree, and repo-visible plan artifact conventions.
