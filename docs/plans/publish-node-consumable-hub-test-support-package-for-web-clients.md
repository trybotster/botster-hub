# Publish Node-consumable hub test-support package for web clients

## Context loaded

- Pipeline context: ticket `ticket_1783311702_290362`, run `run_1783311723_728343`, active step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, reviews, open questions, or answers were present.
- Required role context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]].
- Botster planning context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]].
- Repo context inspected:
  - `Cargo.toml` workspace members include `botster-hub-client` and `botster-hub-test-support`.
  - `crates/botster-hub-client/generated/daemon-protocol.ts` is the checked TypeScript protocol artifact generated from Rust serde DTOs.
  - `crates/botster-hub-client/src/lib.rs` exposes `daemon_protocol_typescript()` and tests exact equality with the checked generated artifact.
  - `crates/botster-hub-test-support/src/lib.rs` embeds/copies the plugin contract matrix fixture, exposes `daemon_protocol_typescript_artifact()`, and tests fixture/protocol parity.
  - `fixtures/plugins/plugin-contract-matrix` remains the hub-owned source fixture.
  - `docs/client-protocol.md` documents Rust crate consumption but not an npm dependency coordinate.
  - `find . -name package.json -o -name '*.mjs' -o -name '*.js' -o -name '*.ts'` found no Node package surface except the generated `.ts` protocol artifact.
- Existing baseline: the Rust dependency path is already solved; this ticket is the Node/npm distribution layer so `botster-web` can consume the same protocol and fixture assets as a declared Node dependency without `../botster-hub`.

## Scope

- Add a small npm package in this repo under a stable package name, preferably `@trybotster/hub-test-support`, that can be published or packed as a Node dependency.
- Keep the package as a generated/distribution wrapper over existing authoritative sources:
  - `crates/botster-hub-client/generated/daemon-protocol.ts`;
  - `fixtures/plugins/plugin-contract-matrix`;
  - compatibility metadata from `botster-hub-client` constants or the existing generated/test-support surfaces.
- Expose a Node API suitable for Vite/Ionic/browser-client tests:
  - protocol artifact path and contents, for drift checks or direct import/copy;
  - helper to materialize the plugin contract matrix fixture into a caller-owned temp directory;
  - metadata with protocol name, protocol version, conformance fixture revision, package version, artifact paths, and SHA-256 checksums for stale-artifact failures.
- Include TypeScript declarations for the Node API so botster-web can use it without handwritten local types.
- Add a no-dependency sync/check script that copies or regenerates package assets from the Rust-owned sources and writes checksum metadata.
- Add tests/checks proving:
  - package assets match the Rust/source artifacts byte-for-byte;
  - metadata checksums match package contents;
  - a Node consumer can import the package API and materialize the fixture without a sibling hub checkout;
  - package contents are included by `npm pack --dry-run` or an equivalent package-list check.
- Update docs with the exact dependency coordinate and example botster-web usage. Environment variables may remain documented only as local overrides, not the normal consumption path.

## Non-scope

- Do not change daemon request/response semantics, protocol versioning, session-worker protocol, plugin runtime behavior, or package manifest vocabulary.
- Do not edit an out-of-tree `botster-web` checkout in this ticket.
- Do not add broad JavaScript tooling, bundlers, TypeScript build steps, or npm dependencies unless implementation proves Node built-ins are insufficient.
- Do not make the npm package the source of truth for protocol DTOs or fixture files.
- Do not remove the existing Rust crate APIs; Node support is additive.

## Assumptions and unknowns

- Assumption: an in-repo package directory such as `packages/hub-test-support` is acceptable even though this Rust repo currently has no root `package.json`; a nested package avoids turning the whole repo into a Node workspace.
- Assumption: checked package assets are acceptable if a sync/check script and tests fail on drift against the Rust/source artifacts.
- Assumption: the package should be usable from both ESM and TypeScript-aware test code. Prefer a plain ESM API plus `.d.ts`; add CommonJS only if package tests or botster-web consumption requires it.
- Assumption: version metadata can start from `0.1.0` aligned with the Rust crate version unless release policy requires another npm version before publish.
- Unknown: the exact publish path and registry configuration. Implementation should prepare a package that can be packed/published, but should not attempt a real publish from the pipeline.
- Unknown: whether consumers want to import the generated TypeScript file directly or read it as text. Provide both a stable exported path and a content helper so botster-web can choose.
- Worktree/target assumption: implementation happens in this pipeline worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.

## Affected surfaces/files

- Botster layers touched: Rust hub-client artifact boundary, Rust hub test-support fixture boundary, Node package distribution surface, docs, package tests.
- Likely new package files:
  - `packages/hub-test-support/package.json`
  - `packages/hub-test-support/index.js`
  - `packages/hub-test-support/index.d.ts`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/daemon-protocol.ts`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/{README.md,botster-package.json,plugin.lua}`
  - `packages/hub-test-support/scripts/sync-assets.mjs`
  - `packages/hub-test-support/test.mjs`
- Existing sources/tests/docs likely touched:
  - `crates/botster-hub-client/generated/daemon-protocol.ts` only as source input, not semantic change.
  - `fixtures/plugins/plugin-contract-matrix/**` only as source input unless docs need cross-link wording.
  - `docs/client-protocol.md`
  - `crates/botster-hub-test-support/fixtures/plugin-contract-matrix/README.md` or root fixture README for Node usage note.
  - possibly `README.md` if the repo already has a client-support/package section appropriate for the dependency coordinate.

## Risks

- Second-source-of-truth drift: checked npm assets can diverge from Rust/source artifacts. Mitigate with one sync/check script, byte-for-byte tests, and checksum metadata validation.
- Package export mistakes: Node `exports` can hide assets from consumers or from `npm pack`. Prove import, path resolution, materialization, and pack file inclusion.
- Over-tooling risk: adding a root Node workspace or build stack would be out of scale. Keep the package self-contained and Node-built-in only.
- Stale metadata risk: version/checksum fields can pass if tests only read `metadata.json`. Tests must recompute hashes from package files and compare to source artifacts.
- Consumer-path regression: docs can accidentally keep `../botster-hub` as the normal botster-web path. The normal docs must use the declared npm dependency; any local path/env var is override-only.
- Runtime proof risk: this is intentionally a distribution-artifact ticket. The production/user path to prove is Node package import plus helper materialization, not hub daemon runtime behavior.
- PII/path leakage risk in metadata, docs, and fixtures. Use relative artifact paths, synthetic `example.invalid` fixture values, and no local home paths.

## Acceptance checks/tests

- Rust artifact/source parity remains green:
  - `./test.sh -p botster-hub-client`
  - `./test.sh -p botster-hub-test-support`
- Node package checks:
  - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - `node packages/hub-test-support/test.mjs`
  - `npm pack --dry-run --json` from `packages/hub-test-support`, or an equivalent command that proves the protocol file, fixture files, metadata, API, declarations, README/license, and package manifest are included.
- Optional live hub regression if implementation touches fixture source or Rust conformance code:
  - `./test.sh --test hub_daemon_lifecycle_test daemon_plugin_contract_matrix_fixture_exercises_public_package_contracts`
- Docs/PII/path scan:
  - `rg -n "../botster-hub|/U[s]ers/[^/]+|/h[o]me/[^/]+|BOTSTER_[A-Z_]*=.*(token|secret|key)|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+" docs/client-protocol.md crates/botster-hub-test-support/fixtures/plugin-contract-matrix/README.md packages/hub-test-support`
  - Any `../botster-hub` hit must be obsolete-context or local-override wording, not the normal Node consumer path.
- Success criteria:
  - A clean Node fixture test imports `@trybotster/hub-test-support`, reads protocol metadata/content, materializes a plugin fixture into a temp directory, and verifies the copied files without any sibling checkout path.
  - Generated package artifacts fail checks when the Rust generated protocol or source fixture changes without resyncing.
  - Documentation gives botster-web the exact npm coordinate and example import/API usage.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/publish-node-consumable-hub-test-support-package-for-web-clients.md`.
- Plan gate should attach this artifact path plus checklist evidence.
- Implement gate should report the public package name, changed files, API names, sync/check command results, Node import/materialization proof, `npm pack` inclusion evidence, Rust parity tests, docs updates, and PII scan result.
- Review should reject package assets that are hand-maintained without drift checks, docs that require `../botster-hub` for normal use, unwired Node API code, missing `npm pack` proof, missing TypeScript declarations, or metadata that does not fail clearly on stale artifacts.

## Vault gaps worth capturing

- Capture after implementation if this becomes a durable Botster convention: Node-consumable client test-support packages should be thin generated wrappers over Rust hub-client/test-support sources, guarded by sync checks and checksums.
- Capture if the final package export shape becomes the standard pattern for distributing generated TypeScript protocol artifacts to web clients.
- No convention conflict found at plan time. The plan follows the hub-client external boundary, source-derived fixture/support-matrix guidance, minimal-dependency preference, path-neutral artifact rule, and repo-visible plan artifact discipline.
