# Hub test-support 0.1.11 daemon protocol release preparation

## Scope and identity

- Target repository: `trybotster/botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Preparation ticket: `ticket_1784912420_977096`
- Publication ticket: `ticket_1784916883_931144`
- Pipeline run: `run_1784912505_507667`
- Preparation branch: `project-pipelines/ticket_1784912420_977096`
- Preparation base: `origin/main` at
  `0484ca8653d3b77679d5c8d4600742e99f1c7c91`
- Prepared npm coordinate: `@trybotster/hub-test-support@0.1.11`
- Protocol identity: `botster-hub-daemon-v1`, protocol version `3`,
  conformance fixture revision `18`

The public registry preflight returned npm `latest` and `dist-tags.latest` as
`0.1.10`. Source preflight found protocol version `2` and conformance fixture
revision `17`, so `0.1.11` / `3` / `18` were unused next identities.

This branch did **not** run `npm publish`. Publication remains owned by
`ticket_1784916883_931144`; after merge, the product orchestrator starts that
ticket's run pinned to the exact merge commit.

## Generated path and changed behavior

The authoritative compatibility constants in
`crates/botster-hub-client/src/lib.rs` were advanced, and the npm package
version was advanced in `packages/hub-test-support/package.json`. Assets were
then regenerated with:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs
```

The generator ran the Rust `botster-hub-test-support` emitter. No generated
asset was hand-edited. The resulting npm `daemon-protocol.ts` includes:

```text
{ type: "refresh_local_packages" }
```

The package test now asserts that exact token as well as package version
`0.1.11`, protocol version `3`, and conformance revision `18`.

## Hashes and packed contents

```text
39e9202bd333584be077e1d1ef5c3fa31a9409996607cb4c01471c103e263980  crates/botster-hub-client/generated/daemon-protocol.ts
39e9202bd333584be077e1d1ef5c3fa31a9409996607cb4c01471c103e263980  packages/hub-test-support/daemon-protocol.ts
7f61c1e2a6ef3eaf8a71c593659e6637c86943ea0d0d9c711641caf0f7c464ff  packages/hub-test-support/metadata.json
2c3c429b4558895d113c38d050389c1c48371d1ad982b50a59ce3c36743ed145  trybotster-hub-test-support-0.1.11.tgz
```

`npm pack --dry-run` and `npm pack` reported 15 files, package size 15.0 kB,
unpacked size 66.7 kB, npm shasum
`b62aba983289eb2651c4ed19e609332712a5d3a7`, and filename
`trybotster-hub-test-support-0.1.11.tgz`.

## Verification

The following gates passed:

```sh
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh -p botster-hub-client
./test.sh -p botster-hub-test-support
node packages/hub-test-support/test.mjs
cargo fmt --all -- --check
./test.sh
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
npm pack --dry-run
npm pack
```

The full wrapper retained real-Hub proof through
`tests/hub_daemon_lifecycle_test.rs`: 95 lifecycle tests passed and the one
documented larger local adversarial test remained explicitly ignored. All
other repository suites and doctests passed.

An initial diagnostic run launched both focused Rust suites, generator check,
and the Node package test concurrently. Two pre-existing timing-sensitive
hub-test-support lifecycle assertions failed in that artificially competing
run. The exact repository-owned command was immediately rerun alone and passed
all 32 unit tests and 3 doctests. The later full default-concurrency
`./test.sh` run also passed, so no failure waiver is claimed.

## Clean external tarball consumer

A fresh temporary Node project outside the checkout installed the exact packed
tarball:

```sh
npm init -y
npm install /path/to/trybotster-hub-test-support-0.1.11.tgz
```

It imported `@trybotster/hub-test-support`, called
`verifyPackageAssets()`, materialized the plugin contract matrix fixture, and
asserted the prepared identity and request token. Its result was:

```json
{
  "package_version": "0.1.11",
  "protocol_version": 3,
  "conformance_fixture_revision": 18,
  "daemon_protocol_sha256": "39e9202bd333584be077e1d1ef5c3fa31a9409996607cb4c01471c103e263980",
  "refresh_local_packages": true,
  "verifyPackageAssets": true,
  "fixture_materialized": true
}
```

This is prepublication tarball proof only. It is not public-registry consumer
proof.

## Ownership and routing

- `botster-hub-client` remains the sole owner of daemon request vocabulary and
  compatibility constants.
- Hub's Rust-backed test-support emitter and npm sync path remain the sole
  package asset source.
- No `botster-core`, botster-web, botster-tui, Project Pipelines package, or
  Project Pipelines plugin source was changed.
- Existing Web consumer work remains ticket `ticket_1784912421_508855` on the
  botster-web target. It now depends on publication ticket
  `ticket_1784916883_931144`. Final integration ticket
  `ticket_1784854143_789468` also depends on the publication ticket. No
  duplicate consumer ticket was created.
- No sibling checkout or
  `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` override was used as evidence.

The implementation follows the approved code plan. The sequencing contract was
clarified by human answer `question_1784914707_524559`: this ticket is
preparation-only and may auto-close with its PR; publication ticket
`ticket_1784916883_931144` was created before merge, and the product
orchestrator starts its exact-merge-SHA run after this ticket closes.

## Post-merge publication commands and residual risk

From the clean publication-ticket checkout pinned to the exact merge commit:

```sh
git rev-parse HEAD origin/main
git status --short
npm view @trybotster/hub-test-support version dist-tags --json
node packages/hub-test-support/scripts/sync-assets.mjs --check
./test.sh -p botster-hub-client
./test.sh -p botster-hub-test-support
node packages/hub-test-support/test.mjs
npm publish --access public
npm view @trybotster/hub-test-support@0.1.11 version dist.tarball dist.integrity --json
```

The publication run must also capture real Hub `up`/status output, install the public
coordinate in a new external consumer, repeat the identity/token/fixture
assertions, and prove the installed public protocol hash equals the
authoritative merge-commit hash.

Unverified behavior is intentionally limited to work that cannot happen before
merge: exact merge-SHA identity, npm authentication/2FA, public publication,
registry propagation, live daemon identity capture, public-coordinate
consumer proof, and final Web dependency routing.

The vault covers source ownership, cold-cut identity, collision avoidance,
strict Rust gates, and external npm consumer proof. It does not yet contain the
approved reusable rule that PR-gated immutable releases prepare on a branch,
publish from a merge-SHA-pinned child, and keep artifact-coupled downstream
integration blocked on that child. That guidance should be captured in the
knowledge-vault target rather than by broadening this Hub worktree.
