# Hub test support 0.1.9 session lifecycle release

## Run and routing

- Ticket: `ticket_1784752212_173295`
- Run: `run_1784764657_297391`
- Target repository: `botster-hub`
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`
- Implementation commit: `713be1c`
- Assumption: the human-owned `npm publish --access public` command was run
  from this run worktree after all prepublication gates passed. Registry
  inspection and a clean public consumer independently verified its result.

## Guidance applied

- `[[implementer-playbook]]`, `[[botster-implementer-playbook]]`, and the exact
  repository charter `[[botster-hub-playbook]]`.
- Runtime/package overlays: `[[botster-runtime-reviewer-playbook]]` and
  `[[botster-package-reviewer-playbook]]`.
- Architecture and task notes: `[[botster-architecture]]`, `[[cli-patterns]]`,
  `[[spa-patterns]]`, `[[botster hub is a first party host profile over core]]`,
  `[[botster data plane bypasses the hub through session and client actors]]`,
  `[[botster local client api lives over hubruntime not raw core routers]]`,
  `[[botster hub events use bounded priority lanes instead of unbounded queue fuses]]`,
  `[[botster hub client crate is the external client boundary]]`,
  `[[external client hub tests use subprocess spawned hub test support]]`,
  `[[botster first party client support matrices belong in hub test support]]`,
  `[[published capability matrices must derive enumerations from source]]`,
  `[[daemon event shape changes bump conformance fixture revision not protocol version]]`,
  `[[conformance fixture revisions must be unique per published content]]`,
  `[[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]]`,
  `[[published fixture readmes are part of the shipped contract]]`,
  `[[generated typescript dtos must encode serde field optionality]]`,
  `[[hub test support npm releases need external consumer smoke]]`,
  `[[closed dependency tickets signal merged source not a consumable release]]`,
  and `[[production runtime generated data dirs use short tmp paths for unix sockets]]`.
- `[[project-pipelines-playbook]]` was intentionally not applied because no
  Project Pipelines package, plugin, or workflow-policy path changed.
- Convention conflicts or waivers: none.

## Implementation

The Hub test-support crate now publishes a typed
`SessionLifecycleSubscriptionConformanceScenario`, stable JSON fixture, and
`run_session_lifecycle_subscription_conformance` runner. The runner uses only
`botster-hub-client` requests and DTOs against an `IsolatedHub`, and proves the
real HubDaemon -> HubRuntime -> CoreDaemon -> session-worker path for the
authoritative snapshot, ordered upsert/patch/remove deltas, independent
subscribers, natural exit, disconnect cleanup, and fresh-subscription snapshot.

The existing production transport overflow seam now derives its expected
`subscriber_overflow` vocabulary and close policy from the same typed fixture.
No production transport behavior or queue capacity changed. Generated Node
assets export the fixture, checksum it, and bind the support matrix to the Rust
runner and JSON helper.

Files changed:

- `README.md`
- `crates/botster-hub-test-support/src/lib.rs`
- `crates/botster-hub-test-support/examples/node_package_assets.rs`
- `tests/hub_daemon_lifecycle_test.rs`
- `src/daemon_transport.rs` (tests only)
- `packages/hub-test-support/package.json`
- `packages/hub-test-support/index.js`
- `packages/hub-test-support/index.d.ts`
- `packages/hub-test-support/test.mjs`
- `packages/hub-test-support/scripts/sync-assets.mjs`
- `packages/hub-test-support/README.md`
- `packages/hub-test-support/metadata.json` (generated)
- `packages/hub-test-support/first-party-client-support-matrix.json` (generated)
- `packages/hub-test-support/session-lifecycle-subscription-conformance-fixture.json` (generated)
- `docs/client-protocol.md`
- `docs/plans/publish-session-lifecycle-subscription-conformance.md`
- `docs/reports/hub-test-support-0.1.9-session-lifecycle-release.md`

## Ownership and dependencies

- Preserved: `botster-core` remains lifecycle/baseline/cursor authority;
  `botster-hub-client` remains sole DTO/socket-helper authority; Hub owns host
  topology, bounded delivery, test support, package generation, and release.
- Preserved: terminal bytes and history remain on SessionIo/ClientWorker paths;
  no data-plane payload was moved into Hub.
- The registered Hub dependency `ticket_1784752211_764368` was already merged
  on the run base. Core dependency `ticket_1784752211_142730` remains consumed
  through the Hub lockfile.
- No cross-repository files changed. Web ticket `ticket_1784752211_333506` and
  TUI ticket `ticket_1784752212_275852` remain separately routed downstream
  upgrades.
- Deviations from the approved plan: none. The synthetic runtime session id was
  shortened after a real test hit the documented macOS Unix-socket pathname
  limit; this is fixture normalization, not a contract or scope change.

## Verification

Passed commands:

- `npm view @trybotster/hub-test-support@0.1.9 ... --prefer-online` preflight:
  candidate unused before publish; `npm whoami` returned `tonksthebear`.
- `./test.sh -p botster-hub-client`: 40 unit and 4 doc tests.
- `./test.sh -p botster-hub-test-support`: 32 unit and 3 doc tests.
- `./test.sh --test hub_daemon_lifecycle_test --no-run`.
- `./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_pushes_snapshot_ordered_deltas_and_fresh_reconnect -- --exact --nocapture`.
- `./test.sh --test hub_daemon_lifecycle_test session_entity_subscription_recovers_after_terminal_disconnect_with_pending_egress -- --exact --nocapture`.
- `./test.sh --lib daemon_transport::tests::entity_overflow_requires_empty_snapshot_resync_and_failed_delivery_disconnects -- --exact --nocapture`.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check`.
- `npm test` in `packages/hub-test-support`.
- `cargo fmt --all -- --check`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `./test.sh`: complete suite passed; the one documented large local
  adversarial test remained ignored.
- `git diff --check` and the committed docs/package local-path, PII, token, and
  private-key marker scan.
- `npm pack --dry-run --json` and `npm pack --json`; exact tarball installed in
  a clean project and all metadata, checksum, DTO-token, fixture-order,
  reconnect, overflow, and support-matrix assertions passed.

## Published artifact and downstream proof

- Coordinate: `@trybotster/hub-test-support@0.1.9`
- Tarball:
  `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.9.tgz`
- Integrity:
  `sha512-l8521hb0K2KszUM9io3T6U+K1EiLiTSpV2Fq+wS3CSJ/Bh4tF1HH/8YESlEyIsx/sdwG7dLpmHJpwSJzIU+VRw==`
- SHA-1: `73972b8769f22160d56194c27dd5354b08670443`
- Protocol version: `2`
- Conformance fixture revision: `16`
- Fixture asset:
  `session-lifecycle-subscription-conformance-fixture.json`
- Fixture SHA-256 from published metadata:
  `2cdf948471d71d8ac6cfdb924cc75d2916b22164c54c6bbd26f4e5aaa12ba845`
- `latest` dist-tag after publication: `0.1.9`

A second clean project installed the exact public coordinate with
`npm install @trybotster/hub-test-support@0.1.9 --save-exact --prefer-online`.
Its lockfile resolved the public tarball and the integrity above. Installed
package assertions repeated the full checksum, generated DTO-token,
snapshot/upsert/patch/remove order, normalized sequence, overflow, reconnect,
and support-matrix proof.

Web upgrade:

```sh
npm install --save-dev --save-exact @trybotster/hub-test-support@0.1.9
```

Web should import `readSessionLifecycleSubscriptionConformanceFixture()` and
must discard prior-generation frames without adding polling/list-refresh
fallbacks. TUI should consume Hub commit `713be1c` and the typed
`session_lifecycle_subscription_conformance_scenario` /
`run_session_lifecycle_subscription_conformance` surface with the same rule.

## Residual risk and vault disposition

- Unverified behavior: downstream Web/TUI dependency upgrades are intentionally
  deferred to their separately routed tickets; this run proves the shared
  consumable contract, not their renderer integration.
- Residual release risk: npm publication is immutable; registry coordinate,
  integrity, installed content, protocol version, and fixture revision were all
  reverified after publication.
- Missing vault guidance: none. The only implementation discovery, Unix socket
  pathname pressure from generated identifiers, is already covered by
  `[[production runtime generated data dirs use short tmp paths for unix sockets]]`.
- Durable knowledge captured: no new vault note; the repository fixture,
  protocol docs, package README, plan, and this report are the durable task
  artifacts.
