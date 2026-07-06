# Publish installable distribution coordinate for @trybotster/hub-test-support

## Context loaded

- Pipeline context: ticket `ticket_1783357942_899405`, run `run_1783357972_245255`, active step `botster_plan`, gate `botster_plan_gate`. No prior artifacts, findings, reviews, open questions, or answers were present.
- Orchestrator clarification: plan the full installability chain, including concrete dependency coordinate, registry/tarball decision, install proof outside the hub repo, import/materialize proof, protocol artifact proof, installed-tarball license proof, version/checksum/staleness story, docs, and botster-web handoff. Do not rely on sibling paths, Project Pipelines paths, unpublished guesses, or root `github:trybotster/botster-hub` specs.
- Required role context: [[identity]], [[goals]], [[planner-playbook]], [[botster-planner-playbook]].
- Botster context: [[botster-architecture]], [[cli-patterns]], [[spa-patterns]], [[project pipeline orchestration belongs in a device-level botster plugin]], [[project pipelines needs an operator workbench not more primitives]], [[project pipelines ui contract belongs in the plugin readme]], [[botster orchestration should spawn agents with explicit target ids]], [[botster orchestration prompts must bind agents to explicit worktrees]], [[plan steps need reviewable plan artifacts]], [[project pipelines checklist worker timeouts require artifact evidence fallback]], [[test script required for rust tests not cargo test]], and [[external client hub tests use subprocess spawned hub test support]].
- Checklist evidence: both `project_pipelines_create_vault_checklist` and `project_pipelines_create_checklist` timed out in the plugin worker, so checklist evidence is preserved in this plan and should also be attached to the Plan gate per [[project pipelines checklist worker timeouts require artifact evidence fallback]].
- Repo context inspected:
  - `packages/hub-test-support/package.json` defines `@trybotster/hub-test-support@0.1.0`, ESM exports, `.d.ts`, package files, and local sync/test scripts.
  - `packages/hub-test-support/README.md` currently documents `npm install --save-dev @trybotster/hub-test-support`, but no real registry coordinate exists yet.
  - `packages/hub-test-support/index.js` exposes metadata, protocol reading, checksum verification, and fixture materialization helpers.
  - `packages/hub-test-support/test.mjs` proves import/materialization when the package is linked locally, but not installability from an external coordinate.
  - `packages/hub-test-support/scripts/sync-assets.mjs` generates package assets from Rust-owned sources and checksum metadata.
  - `docs/client-protocol.md` documents the package as the normal Node client path, so docs currently overstate installability.
  - `npm view @trybotster/hub-test-support version` returned npm `E404` from this worktree on 2026-07-06, confirming the registry coordinate is not currently published to the public npm registry.

## Scope

- Chosen distribution path: publish `@trybotster/hub-test-support@0.1.0` to the npm registry as the source-of-truth coordinate for botster-web and CI.
- Make the existing package publishable without creating a second source of truth:
  - keep `daemon-protocol.ts`, fixture files, and metadata generated from Rust hub/test-support sources;
  - preserve the existing sync/check workflow and checksum metadata;
  - fix package metadata so the installed tarball resolves license information from inside the tarball.
- Add or update docs with the exact dependency spec botster-web should use:
  - `devDependencies["@trybotster/hub-test-support"] = "0.1.0"` after publish, or the exact lockfile-resolved form npm writes;
  - registry/auth setup if the package is private or scoped registry auth is required;
  - a short handoff note for botster-web explaining the package-lock update and smoke command to run there.
- Prove end to end from a clean non-hub fixture:
  - `npm install @trybotster/hub-test-support@0.1.0` succeeds without `../botster-hub`;
  - a Node smoke imports the package, reads the daemon protocol artifact, verifies package assets, and materializes the plugin contract matrix fixture;
  - the installed tarball contains package metadata, README, license file, protocol artifact, metadata, fixture files, JS API, and declarations.

## Non-scope

- Do not create a root Node workspace, bundler, or broader JavaScript toolchain in this Rust repo.
- Do not change daemon protocol semantics, protocol versioning, plugin fixture behavior, hub runtime policy, or botster-web source code in this repo.
- Do not use root `github:trybotster/botster-hub#<sha>` or guessed npm git-subdirectory syntax as the final coordinate unless npm install from a clean non-hub repo is proven and documented; this plan selects npm publish instead.
- Do not fall back to `../botster-hub`, Project Pipelines worktree paths, or local `file:` dependencies as the normal botster-web path.
- Do not publish a tarball manually as an untracked local artifact. If npm credentials are unavailable, ask a blocking human question before switching to the ticket's acceptable durable hosted `.tgz` release-asset path.

## Assumptions and unknowns

- Assumption: the intended first coordinate is public npm registry `@trybotster/hub-test-support@0.1.0`, because the package already uses that scoped npm name and the ticket lists npm publish as preferred.
- Assumption: `@trybotster` npm org/package publish rights are available through an npm token or logged-in publisher in the implementation environment. If not, implementation must ask a human for registry/auth direction rather than silently selecting a different coordinate.
- Assumption: version `0.1.0` is still the first publish version. Before publish, implementation must run `npm view @trybotster/hub-test-support@0.1.0 version`; if it unexpectedly exists, ask a human whether to reuse, deprecate, or bump.
- Assumption: package visibility should be public unless project policy requires private scoped packages. If private, docs must include `.npmrc`/CI token setup and the clean install proof must include that setup without leaking token values.
- Unknown: whether the npm org has two-factor or provenance requirements. The implementer should document any publish command variant used, such as `npm publish --access public` or `npm publish --provenance --access public`.
- Unknown: whether botster-web's CI uses npm, pnpm, or another client. The deliverable docs should still state the npm package spec and mention the exact package-lock/dependency change expected for npm-based botster-web until another client is confirmed.
- Worktree/target assumption: implementation happens in this run's assigned worktree for target `tgt_7e208a0c76a44980a83b63af976b1f22`.

## Affected surfaces/files

- Botster layers touched: Node package distribution surface, Rust-generated client protocol artifact boundary, Rust hub test-support fixture boundary, docs, and CI/package verification commands.
- Likely package files:
  - `packages/hub-test-support/package.json`
  - `packages/hub-test-support/README.md`
  - `packages/hub-test-support/index.js`
  - `packages/hub-test-support/index.d.ts`
  - `packages/hub-test-support/metadata.json`
  - `packages/hub-test-support/daemon-protocol.ts`
  - `packages/hub-test-support/fixtures/plugin-contract-matrix/**`
  - `packages/hub-test-support/scripts/sync-assets.mjs`
  - `packages/hub-test-support/test.mjs`
  - `packages/hub-test-support/LICENSE`
  - `packages/hub-test-support/package.json` `files[]`, which must include `LICENSE`
- Likely docs:
  - `docs/client-protocol.md`
  - `packages/hub-test-support/README.md`
  - possibly root `README.md` if it has the appropriate client-support section.
- Optional release artifacts outside git, only if npm publish is blocked and a human approves the durable `.tgz` fallback:
  - a GitHub Release asset URL;
  - SHA-256 checksum;
  - docs replacing the npm version spec with the exact `https://...tgz` npm install coordinate.

## Risks

- Registry/auth risk: the plan depends on npm publish rights. Mitigation: fail early with `npm whoami`, `npm access`, and `npm view`; ask a human before switching distribution strategy.
- Publish irreversibility risk: npm registry versions are immutable; a defective `0.1.0` cannot be fixed by re-publishing the same version. Mitigation: make publish the final irreversible step, first produce the actual packed tarball and install/smoke that tarball from a clean non-hub fixture. If a defect is discovered only after publish, treat the fix as a `0.1.x` version bump, not a re-publish.
- License risk: `license: "SEE LICENSE IN ../../LICENSE"` does not resolve from the installed tarball unless the license file is included. Mitigation: include a package-local `LICENSE`, add `LICENSE` to package `files[]`, and prove `node_modules/@trybotster/hub-test-support/LICENSE` exists after both tarball and registry installs.
- False installability risk: local package tests can pass while registry installs fail. Mitigation: create fresh temp non-hub npm projects and install first from the locally packed tarball before publish, then from the documented registry coordinate after publish.
- Stale asset risk: published package can lag Rust protocol or fixture sources. Mitigation: run sync check before publish, keep checksum metadata, verify `verifyPackageAssets()`, and document version/checksum expectations.
- Coordinate ambiguity risk: a GitHub repo root spec will keep failing because the repo root has no `package.json`. Mitigation: docs must state the npm package coordinate as normal path and explicitly demote local overrides to development-only.
- Private registry risk: docs can omit required CI auth and cause botster-web CI failures. Mitigation: if private, include scoped registry and token environment instructions with no secret values.
- PII risk: npm package contents, metadata, docs, and smoke scripts must not include local worktree paths, user home paths, tokens, or emails.

## Acceptance checks/tests

- Prepublish source/package checks:
  - `node packages/hub-test-support/scripts/sync-assets.mjs --check`
  - `node packages/hub-test-support/test.mjs`
  - `npm pack --dry-run --json` from `packages/hub-test-support`, with evidence that files include the API, declarations, protocol, metadata, fixture files, README, package manifest, and `LICENSE`.
  - `npm pack --json` from `packages/hub-test-support` to produce the actual `.tgz` that would be published.
  - In a fresh temp non-hub project, run `npm init -y`, `npm install --save-dev <path-to-packed-.tgz>`, and the full Node ESM smoke against the installed package before publishing. The smoke must import `@trybotster/hub-test-support`, call `readDaemonProtocolTypescript()`, assert the protocol contains `DaemonRequest` and `DaemonCompatibility`, call `verifyPackageAssets()`, materialize the plugin contract matrix fixture, read `botster-package.json` and `plugin.lua`, and prove `node_modules/@trybotster/hub-test-support/LICENSE` exists.
  - `./test.sh -p botster-hub-client`
  - `./test.sh -p botster-hub-test-support`
- Registry/publish checks:
  - `npm view @trybotster/hub-test-support@0.1.0 version` before publish should return 404 or a human-approved existing state.
  - Only after the local packed-tarball install proof passes, run `npm publish --access public` from `packages/hub-test-support`, or the documented private/provenance variant required by the registry.
  - `npm view @trybotster/hub-test-support@0.1.0 dist.tarball dist.integrity license version` after publish.
- Registry install confirmation:
  - create a temp directory outside the hub repo;
  - run `npm init -y`;
  - run `npm install --save-dev @trybotster/hub-test-support@0.1.0`;
  - re-run the same Node ESM smoke used for the local packed tarball;
  - inspect the installed package under `node_modules/@trybotster/hub-test-support` and prove `LICENSE` exists.
- Docs/handoff checks:
  - docs state the exact dependency spec and registry/auth setup;
  - docs include the botster-web handoff: add dev dependency, update package-lock from the clean registry coordinate, run the import/materialize smoke or matching botster-web CI check;
  - `rg -n "../botster-hub|Project Pipelines|/Users/|/home/|npm_[A-Za-z0-9]|token|secret" docs/client-protocol.md packages/hub-test-support/README.md README.md` has no normal-path dependency on local hub/worktree paths and no secrets.

## Pipeline gates and artifacts

- Plan artifact: `docs/plans/publish-installable-distribution-coordinate-for-hub-test-support.md`.
- Plan gate evidence should attach this plan path, the loaded vault notes, the npm `E404` observation, and the checklist-timeout fallback.
- Implement gate should attach:
  - package coordinate and version;
  - registry visibility/auth decision;
  - actual packed tarball path and pre-publish install-from-tarball smoke proof;
  - publish command and post-publish registry `npm view` proof;
  - clean non-hub registry install proof;
  - Node import/materialization/protocol proof;
  - installed tarball and registry package license proof;
  - package file inclusion proof;
  - sync/Rust test proof;
  - docs and botster-web handoff proof;
  - PII scan result.
- Plan Review should reject any plan or implementation that leaves the package only locally packable, uses a sibling path as the documented dependency, omits registry/auth details, omits installed-tarball license proof, or cannot prove the botster-web dependency coordinate from a clean non-hub project.

## Vault gaps worth capturing

- Capture after implementation if npm publish becomes the durable convention: Botster Node-consumable test-support packages should publish a real registry coordinate, include tarball-local license metadata, and prove clean external install before client repos depend on them.
- Capture after implementation if the version/checksum/staleness workflow becomes the standard: generated Node assets from Rust protocol/fixture sources require checksum metadata plus prepublish sync checks.
- No convention conflict found at plan time. The plan follows the external-client boundary, generated source-of-truth rule, minimal tooling preference, path-neutral artifact rule, and repo-visible plan artifact discipline.
