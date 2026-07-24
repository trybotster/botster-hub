# Publish session lifecycle subscription conformance

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket: `ticket_1784752212_173295`; run `run_1784764657_297391`.
- Repository routing was resolved from the admitted Botster spawn-target record,
  not from the process working directory. The assigned worktree is at
  `origin/main` merge `0ac4dfb` (PR #158), so the closed Hub contract dependency
  is present on this run's base.
- Role and repository playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], and [[botster-hub-playbook]].
- Surface guidance: [[botster-runtime-reviewer-playbook]] for the real daemon /
  session-worker harness and [[botster-package-reviewer-playbook]] for the npm
  test-support package. [[project-pipelines-playbook]] is intentionally not
  loaded because no Project Pipelines package, plugin, engine, surface, or
  workflow-policy file is in scope.
- Architecture maps and target notes: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[botster hub is a first party host profile over core]],
  [[botster hub gravity must be watched before it becomes the new monolith]],
  [[botster data plane bypasses the hub through session and client actors]],
  [[botster local client api lives over hubruntime not raw core routers]],
  [[botster hub events use bounded priority lanes instead of unbounded queue fuses]],
  [[botster hub client crate is the external client boundary]],
  [[external client hub tests use subprocess spawned hub test support]],
  [[botster session worker requires explicit build in production runtime launchers]],
  [[botster first party client support matrices belong in hub test support]],
  [[published capability matrices must derive enumerations from source]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  [[conformance fixture revisions must be unique per published content]],
  [[shared conformance fixtures that contradict the core contract teach clients the wrong state machine]],
  [[published fixture readmes are part of the shipped contract]],
  [[generated typescript dtos must encode serde field optionality]],
  [[hub test support npm releases need external consumer smoke]], and
  [[closed dependency tickets signal merged source not a consumable release]].
- Repository evidence inspected: root/package READMEs, the prior subscribed
  session plan, public client protocol, client/test-support crates, production
  daemon transport, real daemon lifecycle tests, generated Node asset pipeline,
  prior npm release plans/reports, `test.sh`, and the loaded-daemon workflow.
- Registry evidence at Plan time: public latest is
  `@trybotster/hub-test-support@0.1.8` with integrity
  `sha512-tOdnofJe9fvX0HSZ21RnPiYh34ORbNpQ3CIqw3fqiugpMcwlc90ZUR0L2UEmINNXsozzzz8S77Qm1LBUrf0o/g==`.
  Its packed `metadata.json` is protocol version 1 / conformance revision 15.
  Main already carries protocol version 2 / revision 16, generated session
  entity DTOs, a support-matrix claim, and real runtime tests, but no reusable
  session-lifecycle fixture/runner and no published artifact containing them.

## Scope

1. Add one reusable, typed session lifecycle subscription conformance surface
   to `botster-hub-test-support`.
   - A public runner starts from an `IsolatedHub` and uses only
     `botster-hub-client` requests/helpers against the spawned real
     `botster-hub` and `botster-session-worker` binaries.
   - It explicitly subscribes to `session`, records the authoritative baseline,
     observes spawn/upsert, lifecycle patches including natural exit, explicit
     remove, independent concurrent delivery, disconnect cleanup, and a fresh
     reconnect subscription/snapshot.
   - It returns a stable report of contract observations rather than exposing
     timing-dependent raw process state.
2. Add one versioned, source-derived JSON fixture for Web and other non-Rust
   consumers.
   - Serialize a Rust `SessionLifecycleSubscriptionConformanceScenario` (or the
     smallest equivalent existing naming pattern) into a normalized fixture.
   - Include the frame vocabulary/order, strictly increasing sequence
     relationships, fresh-subscription generation boundary, and overflow/loss
     contract: a resync is a new authoritative snapshot carrying
     `resync_reason: "subscriber_overflow"`; later deltas cannot precede it;
     failed snapshot delivery closes the subscription rather than concealing a
     gap.
   - Normalize only nondeterministic timestamps/sequences needed for stable
     bytes. Keep actual public `DaemonEntityFrame` serde shapes as the source;
     do not invent client-specific event names or a second DTO model.
3. Bind the fixture to production behavior with Hub-owned tests.
   - The existing real-daemon lifecycle regression should call the new runner
     or compare its observations with the fixture's lifecycle contract.
   - The existing production transport overflow/resync unit seam should assert
     the exact fixture reason/order/close policy. Do not add a public or
     test-only daemon queue-capacity flag solely to force socket backpressure.
   - Add source-equality guards so the checked npm fixture cannot drift from the
     Rust serializer, just like the existing generated protocol/support assets.
4. Publish the same contract through `@trybotster/hub-test-support`.
   - Emit, checksum, export, type, test, and document the new fixture through
     the existing `node_package_assets` and `sync-assets.mjs` pipeline.
   - Regenerate/check the daemon protocol artifact and support matrix from Rust.
     Do not hand-edit generated DTOs, fixture JSON, support JSON, or metadata.
   - Update the support matrix's `session_entities` evidence to name the new
     reusable runner and fixture instead of only an in-repo regression name.
5. Cut the next public patch release through the established npm path.
   - Plan-time default is package `0.1.9`, because public `0.1.8` is immutable
     and still contains protocol 1 / revision 15. Recheck that exact version
     immediately before mutation and again immediately before publish.
   - Preserve protocol 2 / conformance revision 16 only if registry and branch
     preflight confirm revision 16 has no competing published meaning and the
     new fixture describes the already-merged revision-16 contract. Otherwise
     stop and ask the human before allocating a different revision.
   - Publish only after local source, full runtime, tarball, and clean-consumer
     gates pass. Then verify the actual public coordinate and repeat the smoke
     from a second clean registry consumer.
6. Persist a release report containing package coordinate, tarball URL,
   integrity/SHA, protocol and fixture revision, fixture checksum/content
   assertions, commands/results, consumer upgrade instructions, and deviations.

## Non-scope

- No redesign or semantic expansion of the merged Hub session subscription,
  CoreDaemon lifecycle source, session projection, bounded queue, remove
  policy, or reconnect behavior.
- No new lifecycle authority, process watcher, polling loop, `list_sessions`
  fallback, duplicate session event model, or subscribe-time global hydration.
- No movement of terminal bytes/history/files into Hub; SessionIo/ClientWorker
  data-plane ownership remains unchanged.
- No generic fixture framework, release automation project, new dependency, or
  public test-only daemon configuration.
- No direct edits to `botster-core`, `botster-web`, or `botster-tui`. Their
  consumer upgrades stay in separately routed tickets.
- No Project Pipelines package/plugin/workflow changes.
- No unrelated cleanup of the open low-severity predecessor findings about
  optional-field clearing or observation-only drain batches unless the new
  test proves they block this ticket's contract.

## Ownership boundaries and cross-repository dependencies

- `botster-core` remains authoritative for lifecycle identity, ordering,
  baseline/cursor semantics, and loss signals. Its prerequisite ticket
  `ticket_1784752211_142730` is closed and consumed through the Hub lockfile;
  this run must not copy or alter Core behavior.
- `botster-hub` owns the real host topology, sanitized projection, connection
  lifetime, bounded delivery/resync policy, test-support crate, generated
  package, release evidence, and public registry publication.
- The in-repository `botster-hub-client` crate remains the sole public DTO and
  socket-helper authority. Rust and JSON conformance surfaces must serialize
  its types rather than mirror them.
- `botster-web` ticket `ticket_1784752211_333506` and `botster-tui` ticket
  `ticket_1784752212_275852` are downstream consumers, not prerequisites. The
  release report must give both the same fixture revision and upgrade
  instructions without adding client-specific lifecycle semantics.
- The current ticket's registered dependency
  `ticket_1784752211_764368` is closed and merged on this worktree base. No new
  cross-repository prerequisite is known. If the fixture exposes a missing Core
  or Hub runtime behavior, stop and register a dependency against the owning
  repository target rather than broadening this release run.

## Product decision ledger, assumptions, and unknowns

- Default: extend the existing `IsolatedHubBuilder`/conformance-report pattern
  and existing Node asset generator; add no parallel harness or generator.
- Default: publish one normalized contract consumed by Rust and Node surfaces;
  renderer-specific expectations remain in Web/TUI.
- Default: the merged contract's `resync_reason` field is the overflow/loss
  diagnostic this artifact must expose. Do not invent a second diagnostic DTO
  unless production already emits one.
- Default: package `0.1.9`, protocol 2, conformance revision 16. These are
  preflight assumptions, not authorization to collide with registry state.
- Follow-up acceptable: Web/TUI dependency and lockfile upgrades after this
  release; this Hub run only proves that both consumption shapes exist and are
  byte/semantic equivalents.
- Ask the human before proceeding if `0.1.9` exists, revision 16 has acquired a
  different published meaning, npm authentication/provenance blocks publish,
  public runtime behavior cannot produce the documented lifecycle, or meeting
  acceptance requires changing runtime semantics rather than test support.
- Unknown until implementation: the narrowest stable report field names and
  normalization needed to compare real timestamps/sequences. Prefer relational
  assertions and existing public DTO serialization over hard-coded clocks.
- Unknown until prepublish: publisher authentication/2FA and current `latest`
  dist-tag. Do not commit credentials, add `.npmrc`, or weaken publication into
  a local tarball-only result.
- No convention conflict or requested waiver is known.

## Affected surfaces and likely files

- `docs/plans/publish-session-lifecycle-subscription-conformance.md` — this
  reviewable plan; update it if an accepted implementation decision changes
  scope or acceptance.
- `crates/botster-hub-test-support/src/lib.rs` — typed scenario/report, live
  conformance runner, serialization helper, support-matrix evidence, and
  source-equality tests.
- `crates/botster-hub-test-support/examples/node_package_assets.rs` — emit the
  source-derived session lifecycle fixture and metadata inputs.
- `tests/hub_daemon_lifecycle_test.rs` — real HubDaemon/CoreDaemon/worker proof
  through the reusable runner; preserve existing adjacent terminal regressions.
- `src/daemon_transport.rs` tests only — bind existing overflow/resync/close
  behavior to the published fixture vocabulary. Production code is not an
  expected change.
- `packages/hub-test-support/scripts/sync-assets.mjs` — generate/copy/checksum
  the new asset and remove stale copies through the established pipeline.
- `packages/hub-test-support/package.json`, `index.js`, `index.d.ts`,
  `test.mjs`, `README.md` — version, export/file list, public read/path helper,
  declarations, package assertions, and upgrade instructions.
- `packages/hub-test-support/session-lifecycle-subscription-conformance-fixture.json`,
  `metadata.json`, `first-party-client-support-matrix.json`, and
  `daemon-protocol.ts` — generated/checksummed outputs, never hand-authored.
- `docs/client-protocol.md` and root `README.md` — replace the deferred-release
  disclaimer with the published coordinate and reusable lifecycle fixture/
  runner semantics.
- `docs/reports/hub-test-support-0.1.9-session-lifecycle-release.md` (or the
  exact next-version filename resolved at implementation) — durable registry
  and downstream proof.
- `crates/botster-hub-client/src/lib.rs` and its authoritative generated
  TypeScript should change only if collision preflight requires a new
  conformance revision; no new protocol surface is expected.

## Risks and mitigations

- **Fixture teaches a client-only state machine:** serialize public client DTOs
  and compare the normalized fixture with observations from the real spawned
  topology. Reject bespoke Web/TUI event names.
- **Runtime claim is only static JSON:** require a focused real-daemon test that
  calls the reusable runner, not merely serde/unit/package checks.
- **Overflow is either flaky or unproven:** keep deterministic queue failure
  coverage at the production transport seam and bind it to the same published
  reason/order/close contract. Do not create optional runtime tuning just for a
  test or rely on OS socket-buffer exhaustion.
- **Fresh reconnect accidentally accepts stale generation frames:** the runner
  must drop the old connection, prove its subscription resource is released,
  use a new id, and require a new authoritative snapshot before later deltas.
- **Harness leaks subprocesses:** retain explicit hub/worker paths, bounded
  reads/readiness, EOF-draining reader behavior, explicit shutdown, and
  kill/wait fallback.
- **Generated copies drift:** every new checked npm copy gets an ordinary Rust
  source-equality test plus `sync-assets --check` and checksum verification.
- **Package version or fixture revision collision:** preflight registry and
  current main immediately before editing/publish; stop rather than silently
  renumbering or overwriting the meaning recorded in this plan.
- **Same local version describes unpublished new bytes:** bump the package to a
  new immutable coordinate before publication; never republish `0.1.8`.
- **Partial npm tarball:** inspect packed files and install the exact tarball in
  a clean consumer before publish, then install the public coordinate after.
- **Authentication turns release into scaffold-only work:** publication is
  explicit acceptance. Ask the human through Project Pipelines and wait; do not
  mark a packed-only result complete.
- **Downstream clients diverge:** expose the same source-derived semantics as a
  Rust typed API and npm JSON asset and assert their equality. Client-specific
  behavior remains downstream.
- **Adjacent predecessor findings become relevant:** if the live runner exposes
  stale optional fields, lost observation-only output, or terminal starvation,
  treat that as a real blocker/finding rather than a pre-existing-failure
  waiver.

## Acceptance checks and downstream proof

1. Registry/version preflight:
   - `npm view @trybotster/hub-test-support version dist-tags --json --prefer-online`.
   - Exact-version lookup for `0.1.9` (or the human-approved replacement) must
     show it is unused; `npm whoami`/publish provenance must be available.
   - Pack and inspect the latest public metadata to confirm the newest published
     fixture meaning before retaining revision 16.
2. Focused client/test-support tests through the repository wrapper:
   - `./test.sh -p botster-hub-client` proves public session entity serde,
     helper, compatibility, and authoritative TypeScript generation remain
     coherent.
   - `./test.sh -p botster-hub-test-support` proves the typed fixture/report,
     source-derived support matrix, serialization, npm-copy equality, and
     example/doc compilation.
3. Real runtime path:
   - A focused `./test.sh --test hub_daemon_lifecycle_test <new-conformance-test> -- --exact --nocapture`
     must invoke the reusable runner against
     `HubDaemon -> HubRuntime -> CoreDaemon -> botster-session-worker` and prove
     explicit snapshot, ordered spawn/lifecycle/remove deltas, concurrent
     isolation, disconnect cleanup, and fresh reconnect snapshot.
   - Existing terminal-disconnect/pending-egress regression stays green so the
     conformance work does not mask data-plane starvation.
4. Overflow/resync:
   - Focused `src/daemon_transport.rs` tests prove queue overflow causes a
     snapshot with `subscriber_overflow` before later deltas, empty snapshot
     resync is valid, failed snapshot delivery closes/fails the subscription,
     and a healthy subscriber is unaffected.
   - The published fixture and runner report name those same semantics; no
     silent delta gap is representable as success.
5. Generated Node assets:
   - Run the sync command, then
     `node packages/hub-test-support/scripts/sync-assets.mjs --check` and
     `npm test` in `packages/hub-test-support`.
   - Assert metadata package version, protocol 2, chosen fixture revision, new
     asset checksum/export, `verifyPackageAssets()`, support-matrix feature and
     runner/fixture evidence, and generated DTO tokens for subscribe,
     snapshot/upsert/patch/remove, `resync_reason`, and session rows.
6. Repository gates required by the Hub charter:
   - `cargo fmt --all -- --check`.
   - `./test.sh`.
   - `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
   - `git diff --check` plus a PII/local-path/token scan over committed
     package/docs/report artifacts.
7. Exact tarball proof before publish:
   - `npm pack --dry-run --json`, then `npm pack --json` from the package root.
   - Install that tarball into a clean external Node project with no sibling
     checkout, `file:` dependency, or protocol override. Assert metadata,
     integrity-visible files, checksum verification, generated DTO tokens,
     lifecycle fixture content/order/resync semantics, and support matrix.
8. Publish and public registry proof:
   - Publish through `npm publish --access public` only after all prior gates.
   - Record `npm view <exact-coordinate> version dist.tarball dist.integrity dist.shasum license dist-tags --json --prefer-online`.
   - Install the exact public coordinate in a second clean consumer and repeat
     every assertion against `node_modules`; local tarball success alone is not
     acceptance.
9. Shared downstream contract proof:
   - Rust tests compare the typed scenario/report with the serialized npm
     fixture so TUI and Web cannot receive different lifecycle semantics.
   - The release report gives Web the exact npm pin/update command and fixture
     import, and TUI the exact Hub commit plus typed helper/report names. It
     explicitly says both must discard prior-generation frames and must not
     add polling/list-refresh fallbacks.
10. Durable pipeline artifacts:
    - Attach this plan, the implementation/release report, published coordinate,
      integrity/SHA, fixture revision/checksum, exact commands/results, and
      consumer upgrade instructions to the run before advancement.
    - Any deviation from the defaults above must be reflected in this plan and
      the report, not only in free-text gate evidence.

## Vault gaps worth capturing

- Candidate after implementation: a reusable conformance artifact should pair
  a normalized source-derived fixture with a real subprocess runner when raw
  lifecycle values contain clocks/sequences that cannot be stable fixture
  literals.
- Candidate after implementation: deterministic overflow conformance can bind a
  production queue seam to a published resync scenario without adding a
  test-only public capacity knob.
- Capture either only if implementation confirms it as a repeatable rule not
  already covered by the loaded fixture, runtime, and package notes. Otherwise
  record `nil` with that reason in the run's vault checklist.
