# Plan: verify published @trybotster/ui-contract 0.3.3 and @trybotster/hub-test-support 0.1.41

Ticket: `ticket_1787351279_697528`
Run: `run_1787351732_490500`
Plan revision: 4. This run is now post-publication verification.
Verification artifact: `artifact_1787357846_301112`.

## Target and charter

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through
  `list_spawn_targets`, not from the working directory.
- Repository playbook: [[botster-hub-playbook]].

## Context loaded

Vault notes read:
- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-architecture]]
- [[botster-hub-playbook]]
- [[Core types-only npm releases use human public publish and clean install proof]]
- [[hub test support npm releases need external consumer smoke]]
- [[an unmerged run that publishes an npm coordinate burns it]]
- [[conformance fixture revisions must be unique per published content]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub generated protocol changes are a four site release chain]]
- [[closed dependency tickets signal merged source not a consumable release]]

Not loaded, with reasons:
- [[botster runtime teardown lenses]]: the runtime-teardown class does not apply.
- [[hotwire-app-planner-playbook]]: `botster-hub` is a Rust workspace, not a
  Hotwire Rails app.
- [[project-pipelines-playbook]]: no Project Pipelines package path is in scope.

## State: both coordinates are published

Jason published both packages on 2026-08-21. The registry is now the authority,
and npm versions are immutable. This run therefore verifies a completed
release. It does not plan one.

Published coordinates:
- `@trybotster/ui-contract@0.3.3`
- `@trybotster/hub-test-support@0.1.41`

Registry version lists end at those coordinates.
`@trybotster/hub-test-support@0.1.40` returns 404 and stays permanently
unpublished, as the amended ticket requires.

## Scope and non-scope

In scope:
1. Prove the published bytes equal the intended merged source at `e950f4f`.
2. Prove a clean registry install satisfies every ticket acceptance criterion.
3. Record that proof as the durable run artifact.

Out of scope:
- Any publish command. Both coordinates exist and are immutable. Do not
  publish, republish, force, or deprecate anything in this run.
- Any source, version, or metadata edit.
- The `test.mjs` literal repair. That is `ticket_1787353310_106098`.
- Any `botster-web` consumer change. That is `ticket_1787278327_274484`.

There are no remaining credential, authorization, publish-order, or
pending-publish steps. Those assumptions are removed, because the action they
guarded is complete.

## Repository ownership boundaries and cross-repo dependencies

`botster-hub` owns both npm packages, their generated assets, and their
publication. Site 3 of the four-site release chain in [[hub generated protocol
changes are a four site release chain]] is now complete.

Cross-repository seams:
- `botster-web` (`tgt_40abcf71ccf049f4ac0c99953a799869`) owns site 4 in
  `ticket_1787278327_274484`. It is now unblocked and should pin
  `@trybotster/hub-test-support@0.1.41` at conformance revision 46 and
  `@trybotster/ui-contract@0.3.3`.
- `botster-core` supplies protocol fixtures through the pinned crate source.
  This run changes no Core pin.
- `ticket_1787349524_364728` publishes the Git tag `botster-ui-contract-v0.3.3`
  for Rust consumers. Neither ticket blocks the other.

`ticket_1787353310_106098` repairs five stale literals in
`packages/hub-test-support/test.mjs` on `origin/main`. It is **not** a blocking
dependency of this release ticket. `test.mjs` is absent from `package.json`
`files[]`, so it never entered either tarball, and the integrity comparison
below proves the published bytes are already correct without it. That repair
keeps `main` green for future work; it cannot change a published artifact.

## Assumptions and unknowns

Assumptions:
- The registry is the authority for what shipped. Local trees are evidence
  about intent, not about delivery.

Unknowns: none material to this ticket. The release is complete and verified.

## Affected surfaces and files

No repository file changes. This run produces verification evidence only.

Surfaces read:
- The installed `@trybotster/ui-contract@0.3.3` package root.
- The installed `@trybotster/hub-test-support@0.1.41` package root.
- `packages/ui-contract` and `packages/hub-test-support` at `e950f4f`, used
  only to pack comparison tarballs.

Botster layers touched: packages and npm distribution only.

## Risks

1. A stale-tree publish, where the registry carries pre-change bytes under a
   correct-looking version. [[hub generated protocol changes are a four site
   release chain]] records that exact incident for `0.1.17`. Retired by the
   integrity comparison below, which matched both coordinates exactly.
2. A false proof from a workspace link, a `file:` dependency, or a local
   tarball. Retired by installing the registry coordinates into an empty
   directory with no link and no local artifact.
3. A metadata-only proof that never exercises the package API. Retired by
   entering through `metadata`, `verifyPackageAssets()`,
   `readDaemonProtocolTypescript()`, and
   `materializePluginContractMatrixFixture()`.
4. Resolving `./package.json`, which neither `exports` map exposes. Retired by
   resolving each exported root entrypoint with
   `path.dirname(require_.resolve(name))`.
5. Revision reuse across concurrent branches, per [[conformance fixture
   revisions must be unique per published content]]. Retired because published
   revision 46 is strictly above published revision 44 in `0.1.39`, and no
   other published coordinate claims 46.

## Acceptance checks and tests, all executed

### 1. Registry coordinates resolve

- `npm view @trybotster/ui-contract versions` ends at `0.3.3`.
- `npm view @trybotster/hub-test-support versions` ends at `0.1.41`.
- `npm view @trybotster/hub-test-support@0.1.40 version` returns 404.

### 2. Published bytes equal the intended merged source

Packed both packages from `origin/main` at `e950f4f`, then compared the
registry `dist.integrity` with the local tarball digest:

| coordinate | registry `dist.integrity` | local pack from `e950f4f` | equal |
|---|---|---|---|
| `@trybotster/ui-contract@0.3.3` | `sha512-+c34Bd5pnELt/HYaKEK5nI1oF1GeRgTejOCIAeAcSF8FCg2wkmJsIac5DLBGgOlWebfyLPQYAmtNAQWbe73eEw==` | same | yes |
| `@trybotster/hub-test-support@0.1.41` | `sha512-LXH9DscSoDvNytbkmhUsiwGXwcAYb93d6/hu2L/PViuQ5Xg8UelWFS8TVItl/KwRGZt90Qyy868Xb7C0zEuC5w==` | same | yes |

This retires the stale-tree publish risk directly, rather than inferring
correctness from self-consistent published metadata.

### 3. Clean registry install in an empty directory

Method: empty temporary directory outside the repository, `npm init -y`, then
`npm install` of each registry coordinate. No workspace link, no `file:`
dependency, no packed tarball.

- `@trybotster/ui-contract@0.3.3` installs.
- `@trybotster/hub-test-support@0.1.41` installs and resolves
  `@trybotster/ui-contract@0.3.3` **transitively from the registry**. The
  installed `node_modules/@trybotster/ui-contract/package.json` reports
  `0.3.3`.

That transitive resolution is the production path this ticket exists to
deliver, and the exact path `ticket_1787278327_274484` was blocked on.

### 4. Public package API assertions

One ESM script, entering only through exported entrypoints:

```js
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import {
  resolveNoticeText,
  NOTICE_TEXT_MAX_BYTES,
} from "@trybotster/ui-contract";
import {
  metadata,
  readDaemonProtocolTypescript,
  daemonProtocolTypescriptPath,
  materializePluginContractMatrixFixture,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

const require_ = createRequire(import.meta.url);
// Neither package exports "./package.json", so resolving that subpath throws
// ERR_PACKAGE_PATH_NOT_EXPORTED. Resolve the exported root entrypoint instead;
// each package main file sits at its package root.
const packageRoot = (name) => path.dirname(require_.resolve(name));

assert.equal(typeof resolveNoticeText, "function");
assert.equal(NOTICE_TEXT_MAX_BYTES, 512);

const uiTypes = fs.readFileSync(
  path.join(packageRoot("@trybotster/ui-contract"), "index.d.ts"),
  "utf8",
);
assert.match(uiTypes, /PackageNoticeReactionDescriptor/);
assert.match(uiTypes, /PackageNoticeReactionDeclaration/);

assert.equal(metadata.package_version, "0.1.41");
assert.equal(metadata.ui_contract.package_version, "0.3.3");
assert.equal(metadata.protocol_version, 7);
assert.equal(metadata.conformance_fixture_revision, 46);

verifyPackageAssets();

const protocol = readDaemonProtocolTypescript();
assert.match(
  protocol,
  /notice_reactions\?: PackageNoticeReactionDescriptor\[\];/,
);
assert.match(protocol, /export interface DaemonPackage /);

const digest = crypto
  .createHash("sha256")
  .update(fs.readFileSync(daemonProtocolTypescriptPath()))
  .digest("hex");
assert.equal(digest, metadata.daemon_protocol.sha256);
assert.equal(
  digest,
  "14121c4b1aa15f0728040b7ab3cc0189bf7720dc3159d994926d54e0251c5996",
);

const dest = fs.mkdtempSync(path.join(os.tmpdir(), "hts-smoke-"));
const fixtureDir = materializePluginContractMatrixFixture(dest);
assert.ok(fs.existsSync(path.join(fixtureDir, "botster-package.json")));
assert.ok(fs.existsSync(path.join(fixtureDir, "plugin.lua")));

for (const name of ["@trybotster/ui-contract", "@trybotster/hub-test-support"]) {
  assert.ok(fs.existsSync(path.join(packageRoot(name), "LICENSE")));
}

console.log("clean consumer smoke passed");
```

Result against the published registry coordinates: `clean consumer smoke
passed`, exit 0. Every assertion executed.

`materializePluginContractMatrixFixture(destination)` returns `destination`
joined with `metadata.plugin_contract_matrix.artifact_path`, currently
`fixtures/plugin-contract-matrix`, so the script asserts against the returned
path rather than against `destination`.

### 5. Ticket acceptance mapping

| Amended ticket criterion | Status |
|---|---|
| `npm view` resolves both coordinates | passed, check 1 |
| ui-contract exports `resolveNoticeText`, `NOTICE_TEXT_MAX_BYTES` 512, both descriptor declarations | passed, check 4 |
| hub-test-support ships `notice_reactions?: PackageNoticeReactionDescriptor[]` on `DaemonPackage` | passed, check 4 |
| metadata reports 0.1.41, ui-contract 0.3.3, protocol 7, revision 46 | passed, check 4 |
| published `daemon-protocol.ts` sha256 equals published `metadata.json` | passed, check 4 |
| clean consumer proves the package API path, not only file contents | passed, check 4 |
| clean install from the registry, not a workspace link or local tarball | passed, check 3 |

## Vault gaps worth capturing

1. A note recording `@trybotster/ui-contract@0.3.3` and
   `@trybotster/hub-test-support@0.1.41` at conformance revision 46 as the
   published package-owned notice-reaction cutover, and recording that `0.1.40`
   at revision 45 was allocated and then skipped, mirroring [[hub test support
   0 1 39 revision 44 is the web package event dto cutover]].
2. A note stating that comparing registry `dist.integrity` against a tarball
   packed from the intended commit is the direct retirement of the stale-tree
   publish risk, stronger than trusting self-consistent published metadata.
3. A note stating that `ui-contract` and `hub-test-support` publish as an
   ordered pair, because `hub-test-support` pins `ui-contract` exactly and
   imports it at runtime.
4. A note stating that a scoped package with an `exports` map does not export
   `./package.json`, so a consumer smoke must resolve the exported root
   entrypoint.
5. A note stating that a hub-test-support version bump must update the package
   test literals as well as the shipped metadata and fixtures, because
   `test.mjs` does not ship and its staleness surfaces only at release time.
6. A note stating that ancestor containment is not release source identity.
