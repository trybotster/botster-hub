# Plan: publish @trybotster/hub-test-support 0.1.42 with the terminal reservation DTO

Ticket: `ticket_1788280618_295967`
Run: `run_1788280945_468802`
Plan revision: 1.

## Target and charter

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`, resolved through
  `list_spawn_targets`, not from the process working directory.
- Repository playbook: [[botster-hub-playbook]].

## Context loaded

Role and repository playbooks:
- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster-hub-playbook]]

Targeted atomic notes:
- [[Core types-only npm releases use human public publish and clean install proof]]
- [[hub test support npm releases need external consumer smoke]]
- [[hub generated protocol changes are a four site release chain]]
- [[registry integrity compared against a pack of the intended commit retires stale tree publish risk]]
- [[an unmerged run that publishes an npm coordinate burns it]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[conformance fixture revisions must be unique per published content]]
- [[Hub test support version bumps must update the Node mirror test literals]]
- [[clean consumer smokes resolve exported root entrypoints not package json]]
- [[published package owned notice reaction cutover is ui contract 0 3 3 and hub test support 0 1 41]]

Repository context read:
- `packages/hub-test-support/package.json`, `metadata.json`, `README.md`,
  `test.mjs`, `daemon-protocol.ts`, `scripts/sync-assets.mjs`.
- `packages/ui-contract/package.json`.
- `crates/botster-hub-client/generated/daemon-protocol.ts`.
- Prior art:
  `docs/plans/verify-published-ui-contract-0-3-3-and-hub-test-support-0-1-41.md`.

Not loaded, with reasons:
- [[botster runtime teardown lenses]]: `teardown_class_applies` is false. This
  run publishes an existing package artifact. It changes no WebRTC or peer
  lifecycle, no `SessionIo` or `ClientWorker` teardown, no multi-peer
  ownership, and no runtime code. The terminal reservation DTO is already
  merged Hub source; this ticket ships bytes, not behavior.
- [[project-pipelines-playbook]]: no Project Pipelines package or plugin path
  is in scope.
- [[hotwire-app-planner-playbook]]: `botster-hub` is a Rust workspace.

## Verified starting state at commit `b4020a9`

The worktree HEAD equals `origin/main` at
`b4020a976010f4ec495c89efd6ea66271e02712f`. The working tree is clean.

Repository package state:
- `packages/hub-test-support/package.json` version `0.1.42`.
- `metadata.json`: `protocol_version` 8, `conformance_fixture_revision` 47,
  `ui_contract.package_version` `0.3.3`.
- `metadata.json` `daemon_protocol.sha256` is
  `8940d99b2e1035b77a9ce94fae8597d246490e5d9673ab084cff8ff04749989a`.
- `shasum -a 256 packages/hub-test-support/daemon-protocol.ts` equals that
  value, and equals the digest of
  `crates/botster-hub-client/generated/daemon-protocol.ts`. Sites 1 and 2 of
  the four-site chain are already synchronized.
- `daemon-protocol.ts` declares `export interface DaemonTerminalReservation`
  with `session_id`, `subscription_id`, `generation`, `peer_generation`,
  `label`, and `expires_in_seconds`. `DaemonResponse` carries
  `terminal_reservation?: DaemonTerminalReservation | null`.
  `DaemonResponseKind` includes `"terminal_reservation"`.
- `grep mode_gated_input packages/hub-test-support/daemon-protocol.ts` returns
  no match. The removed request is absent.

Gate commands already pass on this commit, run from
`packages/hub-test-support` after `npm install --no-save`:
- `npm run check` prints `hub test-support package assets are current`.
- `npm test` prints
  `hub test-support package import and fixture materialization passed`.
  The five `test.mjs` literals required by
  [[Hub test support version bumps must update the Node mirror test literals]]
  already read `0.1.42` and revision 47.

Registry state on 2026-09-01:
- `npm view @trybotster/hub-test-support version` returns `0.1.41`.
- The published version list ends at `0.1.41`. `0.1.42` is unpublished, so no
  coordinate is burned under
  [[an unmerged run that publishes an npm coordinate burns it]].
- `npm view @trybotster/ui-contract version` returns `0.3.3`, which equals the
  exact dependency pin. No second package publication is required, and the
  ordered-pair constraint from
  [[published package owned notice reaction cutover is ui contract 0 3 3 and hub test support 0 1 41]]
  is already satisfied.
- `npm whoami` in the agent shell returns `401 Unauthorized`. The agent cannot
  publish.

## Scope and non-scope

In scope:
1. Publish `@trybotster/hub-test-support@0.1.42` from commit `b4020a9` through
   the human credentialed `npm publish --access public` path.
2. Prove that the published bytes equal a package packed from `b4020a9`.
3. Prove the coordinate from a clean external consumer that installs from the
   registry, not from a workspace link or a local tarball.
4. Record the exact published Hub commit and the resulting baseline in durable
   run artifacts.

Out of scope:
- Any change to package source, version, metadata, fixtures, or `test.mjs`.
  The merged bytes are the artifact under release. If a defect appears, this
  run stops and reports; it does not silently repair and publish different
  bytes under `0.1.42`.
- Any npm token committed to the repository, and any new GitHub Actions
  publish workflow. [[Core types-only npm releases use human public publish and clean install proof]]
  forbids both as release side effects.
- `botster-web` vendoring, its exact pin, its drift gate, and its README
  metadata claims. That is site 4 and belongs to `ticket_1787600676_914408`.
- Republishing, deprecating, or unpublishing `0.1.41` or any earlier
  coordinate.
- Any Rust source, Core pin, or runtime change.

## Repository ownership boundaries and cross-repo dependencies

`botster-hub` owns the emitter, the generated `daemon-protocol.ts`, the
`packages/hub-test-support` mirror, and the npm publication. This ticket is
site 3 of [[hub generated protocol changes are a four site release chain]].
Sites 1 and 2 are already merged at `b4020a9`.

Cross-repository seams:
- `botster-web` (`tgt_40abcf71ccf049f4ac0c99953a799869`) owns site 4 in
  `ticket_1787600676_914408`. It stays blocked until `0.1.42` resolves from the
  registry. Web may implement against a local artifact through
  `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL`, but it must not merge on that basis.
  No dependency registration is added here, because this Hub ticket is itself
  the dependency that the Web ticket names.
- `botster-core` supplies protocol fixtures through the pinned crate source.
  This run changes no Core pin.
- `@trybotster/ui-contract@0.3.3` is already published and already pinned. No
  ordered second publish is required.

The human operator owns the credentialed publish action. The Implement agent
owns preparation, the ask, and every verification that follows.

## Assumptions and unknowns

Assumptions:
1. The merged bytes at `b4020a9` are the intended release content. Every
   repository gate passes on that commit, so no pre-publish source change is
   expected.
2. The operator publishes from a clean checkout of `b4020a9`, not from an
   ambient working tree. The integrity comparison below detects a violation of
   this assumption before the run claims success.
3. npm remains reachable from the agent shell for read-only `npm view`,
   `npm pack`, and `npm install`. Only the publish action needs credentials.

Unknowns:
1. The publish timing depends on the human operator. The Implement agent must
   call `project_pipelines_ask_human` and wait, rather than treat a prepared
   coordinate as a shipped one.
2. Whether the operator publishes from a stale tree. This is the known failure
   shape recorded for `0.1.17`. Check 2 retires it and must run before any
   completion claim.

## Affected surfaces and files

No package or product source file changes. This run adds run artifacts only:
- `docs/plans/publish-hub-test-support-0-1-42-terminal-reservation.md` (this
  plan).
- `docs/reports/publish-hub-test-support-0-1-42-terminal-reservation-implement.md`
  (Implement report, to be written).
- `docs/reports/publish-hub-test-support-0-1-42-terminal-reservation-evidence.json`
  (machine-readable evidence, to be written).

Surfaces read or exercised:
- `packages/hub-test-support` at `b4020a9`, used only to pack the comparison
  tarball.
- The installed `@trybotster/hub-test-support@0.1.42` package root in a clean
  external directory.

Botster layers touched: packages and npm distribution only.

## Risks

1. **Stale-tree publish.** The registry carries pre-change bytes under a
   correct-looking version, with self-consistent metadata, as happened for
   `0.1.17`. Retired by check 2, the `dist.integrity` comparison against a pack
   of `b4020a9`.
2. **False proof from a workspace link, a `file:` dependency, or a local
   tarball.** Retired by check 3, which installs the registry coordinate into
   an empty directory outside the repository.
3. **Metadata-only proof that never exercises the package API.** Retired by
   check 4, which enters through `metadata`, `verifyPackageAssets()`,
   `readDaemonProtocolTypescript()`, and
   `materializePluginContractMatrixFixture()`.
4. **Resolving `./package.json`,** which the `exports` map does not expose.
   Retired by resolving the exported root with
   `path.dirname(require_.resolve(name))`, per
   [[clean consumer smokes resolve exported root entrypoints not package json]].
5. **Revision collision.** Revision 47 must name exactly one published content.
   Retired by check 1: published revisions end at 46 in `0.1.41`, and 47 is
   strictly above every published revision.
6. **Immutability after a bad publish.** npm versions cannot be repaired in
   place. If check 2 or check 4 fails, the recovery is a new unused version and
   a fresh conformance revision, per
   [[an unmerged run that publishes an npm coordinate burns it]] and
   [[Hub test support capability cutovers use a new unpublished package version]].
   The run must not attempt to overwrite `0.1.42`.
7. **A human publish that never happens.** The Implement agent must not report
   success from a prepared local tree. Publication is proven only by registry
   reads.

## Acceptance checks and tests

Run every check from a colon-free path. No cargo gate is required in this run,
so `CARGO_TARGET_DIR` stays unset.

### 0. Pre-publish repository proof at `b4020a9`

From `packages/hub-test-support`, after `npm install --no-save`:
- `npm run check` passes.
- `npm test` passes.
- `git rev-parse HEAD` equals `b4020a976010f4ec495c89efd6ea66271e02712f`.
- `git status --porcelain` is empty.

Record the four results. Already observed green during Plan; Implement repeats
them as the release-time record.

### 1. Registry preconditions

- `npm view @trybotster/hub-test-support@0.1.42 version` returns a 404 before
  publication.
- `npm view @trybotster/hub-test-support versions` ends at `0.1.41`.
- `npm view @trybotster/ui-contract version` returns `0.3.3`.

### 2. Human publish, then registry integrity against a pack of `b4020a9`

Preparation, before the ask:

```sh
scratch=$(mktemp -d)
git archive b4020a9 packages/hub-test-support | tar -x -C "$scratch"
cd "$scratch/packages/hub-test-support" && npm pack --json
```

Record the packed `integrity` SHA-512 value.

Ask the human operator, through `project_pipelines_ask_human`, to run this
from a clean checkout of `b4020a9`:

```sh
cd packages/hub-test-support
npm publish --access public
```

The operator uses a credentialed shell. The run adds no npm token to the
repository and adds no publish workflow.

After the operator confirms publication:
- `npm view @trybotster/hub-test-support@0.1.42 dist.integrity` must equal the
  packed integrity value from `b4020a9`.
- A mismatch fails the ticket. Do not republish `0.1.42`. Report and stop.

### 3. Clean external install

In an empty temporary directory outside the repository:
- `npm init -y`.
- `npm install @trybotster/hub-test-support@0.1.42`.
- Assert no workspace link, no `file:` dependency, and no local tarball.
- Assert `node_modules/@trybotster/ui-contract/package.json` reports `0.3.3`,
  resolved transitively from the registry.

### 4. Installed consumer smoke

One ESM script in that clean directory, entering only through exported
entrypoints:

```js
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { createRequire } from "node:module";
import {
  metadata,
  readDaemonProtocolTypescript,
  daemonProtocolTypescriptPath,
  materializePluginContractMatrixFixture,
  verifyPackageAssets,
} from "@trybotster/hub-test-support";

const require_ = createRequire(import.meta.url);
// The exports map has no "./package.json" key. Resolve the exported root.
const packageRoot = (name) => path.dirname(require_.resolve(name));

assert.equal(metadata.package_version, "0.1.42");
assert.equal(metadata.protocol_version, 8);
assert.equal(metadata.conformance_fixture_revision, 47);
assert.equal(metadata.ui_contract.package_version, "0.3.3");
assert.equal(
  metadata.daemon_protocol.sha256,
  "8940d99b2e1035b77a9ce94fae8597d246490e5d9673ab084cff8ff04749989a",
);

// verifyPackageAssets() returns { ok, failures }. It does not throw.
const assets = verifyPackageAssets();
assert.deepEqual(assets.failures, []);
assert.equal(assets.ok, true);

const protocol = readDaemonProtocolTypescript();
assert.match(protocol, /export interface DaemonTerminalReservation \{/);
for (const field of [
  "session_id",
  "subscription_id",
  "generation",
  "peer_generation",
  "label",
  "expires_in_seconds",
]) {
  assert.ok(protocol.includes(field), field);
}
assert.match(
  protocol,
  /terminal_reservation\?: DaemonTerminalReservation \| null;/,
);
assert.match(protocol, /\| "terminal_reservation"/);
// The removed request must be absent from the published bytes.
assert.ok(!protocol.includes("mode_gated_input"));

const digest = crypto
  .createHash("sha256")
  .update(fs.readFileSync(daemonProtocolTypescriptPath()))
  .digest("hex");
assert.equal(digest, metadata.daemon_protocol.sha256);

const dest = fs.mkdtempSync(path.join(os.tmpdir(), "hts-0142-smoke-"));
const fixtureDir = materializePluginContractMatrixFixture(dest);
assert.ok(fs.existsSync(path.join(fixtureDir, "botster-package.json")));
assert.ok(fs.existsSync(path.join(fixtureDir, "plugin.lua")));

assert.ok(
  fs.existsSync(
    path.join(packageRoot("@trybotster/hub-test-support"), "LICENSE"),
  ),
);

console.log("clean consumer smoke passed");
```

The script must exit 0 and print `clean consumer smoke passed`.

The `mode_gated_input` absence assertion is the delta proof against published
`0.1.41`, which still contains that request. Implement should also record the
`0.1.41` positive control: a `readDaemonProtocolTypescript()` read of the
installed `0.1.41` package that does contain `mode_gated_input`. Without that
control, the absence assertion could pass against an empty or wrong file.

### 5. Downstream unblock statement

Record in the Implement report:
- The exact published Hub commit `b4020a976010f4ec495c89efd6ea66271e02712f`.
- The coordinate `@trybotster/hub-test-support@0.1.42`.
- Protocol version 8, conformance fixture revision 47,
  `@trybotster/ui-contract@0.3.3`.
- The `daemon-protocol.ts` SHA-256
  `8940d99b2e1035b77a9ce94fae8597d246490e5d9673ab084cff8ff04749989a`.
- The registry `dist.integrity` value.

`botster-web` `ticket_1787600676_914408` consumes these five facts for its
pin, its drift gate, and its README metadata claim.

### 6. Ticket acceptance mapping

| Ticket criterion | Check |
|---|---|
| Publish `0.1.42` through the human public publish path | 2 |
| Published `daemon-protocol.ts` and metadata sha256 equal repository artifacts | 2 and 4 |
| Prove the coordinate with an external clean install, not a workspace link | 3 and 4 |
| Record the exact Hub commit published | 0 and 5 |

## Runtime-teardown class

- `teardown_class_applies`: false.
- Reason: this run publishes an already-merged package artifact. It changes no
  peer lifecycle, no session or client teardown, no ownership identity, and no
  runtime code path. The terminal reservation DTO is a shipped type
  declaration, not a live runtime change in this ticket.

## Production-path proof for this ticket

This ticket is intentionally a distribution change, not a code change. The
production path it delivers is registry resolution: a consumer outside the
workspace must install `@trybotster/hub-test-support@0.1.42` from npm and read
the terminal reservation DTO from the installed package. Checks 3 and 4 are
that proof. A local `npm pack` or a workspace link is not.

## Vault gaps worth capturing

1. A note recording `@trybotster/hub-test-support@0.1.42` at protocol version
   8 and conformance revision 47 as the published terminal reservation
   coordinate, with the Hub commit `b4020a9` and the `daemon-protocol.ts`
   SHA-256, in the same shape as
   [[published package owned notice reaction cutover is ui contract 0 3 3 and hub test support 0 1 41]].
2. A note stating that a removal-shaped protocol release needs a positive
   control on the prior published coordinate. An absence assertion alone can
   pass against a wrong or empty file.
3. A note stating that a Hub support release can require no paired
   `ui-contract` publish when the exact pin already resolves from the registry,
   so the ordered-pair rule applies only when the pinned version changes.
