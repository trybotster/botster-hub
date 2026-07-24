# `@trybotster/hub-test-support` 0.1.3 release

## Outcome

`@trybotster/hub-test-support@0.1.3` is published on the public npm registry.
The published tarball contains the generated terminal-readback daemon DTOs,
the source-derived first-party client support matrix, and the source-derived
late-attach history conformance fixture.

Registry coordinate:

- Package: `@trybotster/hub-test-support`
- Version: `0.1.3`
- Dist-tag: `latest`
- Tarball: `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.3.tgz`
- SHA-1: `2d83f6e167043b1605cf952527fc75b4cfbc2b6d`
- Integrity: `sha512-MzDrcB13cDfT+QZ8yoJ+zf+0pCfcUI5VHMYEC7U8e2e1LhQx4x8vLKFRSfrMPBT9n52eZBXYVebLYxZM4ABQOw==`

The locally generated tarball has the same SHA-1 and integrity as the public
registry artifact.

## Assumptions and decisions

- The new package assets use the fixed filenames
  `first-party-client-support-matrix.json` and
  `late-attach-history-conformance-fixture.json` everywhere: the Rust emitter,
  sync/checksum pipeline, package exports and file list, JavaScript API,
  declarations, tests, and docs.
- Rust serde DTOs and `botster-hub-test-support` helpers remain authoritative.
  The npm package does not introduce a second feature list, fixture, or protocol
  generator.
- The existing conformance fixture revision and protocol version are unchanged;
  this release packages source behavior already merged on hub main.
- Publishing the matrix makes `terminal_readback` both supported and required.
  `botster-hub-client::current_feature_list()` currently feeds both descriptors,
  so downstream compatibility checks must implement the feature. Separating
  required from supported is a separate client-contract change.
- The ticket intentionally changes the registry-consumer path, not the running
  hub, SessionIo/ClientWorker data plane, or terminal history production path.

## Files changed

- `crates/botster-hub-test-support/examples/node_package_assets.rs`
- `crates/botster-hub-test-support/src/lib.rs`
- `packages/hub-test-support/package.json`
- `packages/hub-test-support/scripts/sync-assets.mjs`
- `packages/hub-test-support/index.js`
- `packages/hub-test-support/index.d.ts`
- `packages/hub-test-support/test.mjs`
- `packages/hub-test-support/daemon-protocol.ts`
- `packages/hub-test-support/metadata.json`
- `packages/hub-test-support/first-party-client-support-matrix.json`
- `packages/hub-test-support/late-attach-history-conformance-fixture.json`
- `packages/hub-test-support/README.md`
- `docs/client-protocol.md`
- `docs/reports/hub-test-support-0.1.3-release.md`

## Verification

All commands ran from the ticket worktree unless a disposable directory is
named explicitly.

- `cargo fmt --all -- --check` — passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` —
  passed after the complete npm-copy guard was added.
- `node packages/hub-test-support/scripts/sync-assets.mjs` — generated current
  npm assets from Rust sources.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` — passed.
- `npm test` in `packages/hub-test-support` — passed. It asserts version 0.1.3,
  package checksums, both JSON export subpaths, `read_screen`,
  `capture_snapshot`, `DaemonReadScreen`, `DaemonCaptureSnapshot`, required and
  supported `terminal_readback`, restored-history-before-live ordering, UTF-8
  byte counts, and the no-history sequence.
- `./test.sh -p botster-hub-client` — passed: 32 unit tests and 4 doc tests.
- `./test.sh -p botster-hub-test-support` — passed: 24 unit tests and 3 doc
  tests, including source-equality guards for all three generated npm package
  copies. Each new JSON guard was also shown to fail under deliberate asset
  corruption before the checked asset was restored.
- `./test.sh` — passed with no failures, including 73 library tests, 14 hub
  capability tests, 21 client API tests, 87 daemon lifecycle tests, 1 local
  production runtime test, 18 Lua runtime tests, 6 MCP tests, 6 plugin lifecycle tests, 7
  hub runtime tests, and doc tests.
- `npm pack --dry-run --json` — passed and listed all 12 expected package files.
- `npm pack --json` — produced SHA-1 and integrity identical to the public
  registry artifact.
- `npm view @trybotster/hub-test-support@0.1.3 name version dist.tarball
  dist.integrity dist.shasum license dist-tags --json --prefer-online` — passed;
  version 0.1.3 is public and `latest`.
- Clean registry consumer under `/tmp`, installed with
  `npm install @trybotster/hub-test-support@0.1.3 --prefer-online` and no
  `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` — passed metadata, checksums, DTO tokens,
  support-matrix membership, both export subpaths, late-attach ordering, byte
  counts, and no-history assertions from `node_modules`.
- Disposable botster-web clone — changed only `package.json`,
  `package-lock.json`, and its generated daemon protocol, then ran
  `env -u BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL node
  scripts/check-daemon-protocol-drift.mjs` — passed. The actual botster-web
  ticket worktree was not modified.
- `git diff --check` — passed.

## Plan deviations

- This agent's `npm whoami` preflight returned `E401`, so it asked the human
  through Project Pipelines rather than weakening the release acceptance. The
  human responder published and independently verified 0.1.3, then instructed
  the agent to continue with registry-consumer verification. Consequently, the
  agent did not personally observe publisher authentication or a clean git
  tree at the instant of publish. The published artifact's SHA-1 and integrity
  exactly match the locally generated and tested tarball, which proves the
  released bytes.
- The approved plan did not pin asset filenames. Plan Review required fixed
  names and export-subpath resolution proof; both were added before editing and
  carried through implementation.
- Plan Review requested durable npm-copy staleness guards. Three narrow Rust
  tests beside the existing checked-generated-file test now compare the daemon
  protocol, support matrix, and late-attach fixture package copies to their
  authoritative generated values.

## Residual risk and unverified behavior

- Publisher identity and provenance policy were verified by the human
  responder, not by this agent's npm session.
- No running-hub behavior changed in this ticket. Existing full-suite tests
  cover readback and late-attach runtime paths; the new work proves packaging
  and downstream consumption.
- The real botster-web branch was not changed. Its exact follow-up diff was
  proven in a disposable clone, but the dependency/generated-file update still
  belongs to the named web ticket.
- `terminal_readback` remains required as well as supported. This is existing
  hub-main behavior now made visible to npm consumers, not a new package-local
  decision.
- The Rust suite guards the three source-generated npm copies. `metadata.json`
  remains guarded transitively by package checksum verification and
  `sync-assets.mjs --check`, which are part of the release verification path.

## Vault guidance disposition

Applied `implementer-playbook`, `botster-implementer-playbook`, the Botster
architecture/CLI/SPA maps, source-derived support-matrix guidance, the external
npm consumer smoke rule, the Rust test-wrapper rule, artifact/git-state rules,
and the manual-only verification warning. No convention conflict was found.
Review exposed one missing explicit vault rule: every checked-in published copy
of a generated artifact needs a source-equality guard in the generating crate's
ordinary test suite. Existing source-derivation and automatic-signal notes each
covered part of that rule but did not state the per-copy obligation. Review also
confirmed that a Project Pipelines checklist creation timeout can be a
client-side false negative, so agents should re-list checklists before using the
artifact-only fallback. Both capture candidates are recorded in the durable
Project Pipelines checklists; no vault file was written from this repo-scoped
implementation step.
