# Publish current daemon protocol in hub test support

## Target and context loaded

- Target repository: `botster-hub` (`trybotster/botster-hub`).
- Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`.
- Pipeline ticket: `ticket_1784912420_977096`; run
  `run_1784912505_507667`.
- Target routing was resolved from the admitted Botster spawn-target registry,
  not inferred from the process working directory. At Plan time, this worktree,
  local `main`, and local `origin/main` all resolve to
  `0484ca8653d3b77679d5c8d4600742e99f1c7c91`, which contains `af5fb28`.
- Role and repository playbooks: [[planner-playbook]],
  [[botster-planner-playbook]], and [[botster-hub-playbook]].
- Additional ownership and surface guidance:
  [[botster-hub-client-playbook]], [[botster-runtime-reviewer-playbook]], and
  [[botster-package-reviewer-playbook]]. The in-repository
  `botster-hub-client` crate owns the external protocol and compatibility
  identity; Hub owns the generated test-support package and publication path.
  [[project-pipelines-playbook]] is intentionally not loaded because this
  ticket changes no Project Pipelines package/plugin path or workflow policy.
- Architecture maps and targeted notes: [[botster-architecture]],
  [[cli-patterns]], [[spa-patterns]],
  [[botster hub is a first party host profile over core]],
  [[botster hub client crate is the external client boundary]],
  [[botster hub client compatibility descriptors belong in client crate]],
  [[daemon event shape changes bump conformance fixture revision not protocol version]],
  [[conformance fixture revisions must be unique per published content]],
  [[generated typescript dtos must encode serde field optionality]],
  [[hub test support npm releases need external consumer smoke]],
  [[botster web generated protocol drift checks need explicit hub artifact paths]],
  and [[rust repo strict lints must be verified before dismissing warnings]].
- Repository evidence inspected: root and package READMEs,
  `docs/client-protocol.md`, prior package release plans and commits,
  `crates/botster-hub-client`, `crates/botster-hub-test-support`,
  `packages/hub-test-support`, the Rust-backed Node asset generator,
  `test.sh`, and repository CI.
- Plan-time registry evidence: public npm `latest` is
  `@trybotster/hub-test-support@0.1.10`.
- Plan-time drift evidence: the authoritative generated protocol contains
  `{ type: "refresh_local_packages" }` and hashes to
  `39e9202bd333584be077e1d1ef5c3fa31a9409996607cb4c01471c103e263980`.
  The checked npm package copy lacks that request and hashes to
  `67676b59e05f8273a0061f9905fb66a94ca7afde0fc4f27691d35d67d6df95d8`;
  `metadata.json` records the stale hash.
- Human decision `question_1784912650_818083`: adding
  `refresh_local_packages` changes request vocabulary, so cold-cut
  `PROTOCOL_VERSION`, `CONFORMANCE_FIXTURE_REVISION`, and the immutable npm
  package version together. Do not preserve protocol-v2 parsing, dual fixtures,
  or compatibility aliases.
- Human decision `question_1784913639_626654`: this PR prepares and verifies the
  release but must not publish an immutable npm version from an unmerged
  branch. After the reviewed PR merges, create a Hub-targeted post-merge
  publication child ticket/run pinned to that exact merge commit. The child
  owns publication, human 2FA coordination, registry install/proof,
  live-daemon identity capture, and durable final release evidence.

## Scope

1. Recheck exact repository and registry identity immediately before allocating
   versions. If npm `latest` remains `0.1.10` and main remains at revision 17,
   use package `0.1.11`, protocol version `3`, and conformance revision `18`.
   If any identity is already used, stop and reallocate strictly above the
   latest published/current meaning rather than colliding.
2. Advance `PROTOCOL_VERSION` and
   `CONFORMANCE_FIXTURE_REVISION` in the authoritative
   `botster-hub-client` crate. Keep the change cold-turkey: one current request
   vocabulary and compatibility descriptor, with no v2 parser or versioned
   parallel DTO.
3. Bump the npm package to the next unused immutable version and update
   package/docs/tests that intentionally name the coordinate and compatibility
   identity.
4. Run the repository-supported generator:
   `node packages/hub-test-support/scripts/sync-assets.mjs`. It must derive the
   npm protocol, support matrix, fixtures, and metadata from the Rust
   `botster-hub-test-support` emitter; generated assets are not hand-edited.
5. Add a package-level assertion for the exact
   `refresh_local_packages` generated token so the ticket's required request
   cannot disappear while broader asset checks stay green.
6. In this PR run, verify the complete packed tarball in a clean temporary
   consumer, record exact release commands, and persist a release-preparation
   report. Do not run `npm publish` from the branch.
7. After this reviewed PR merges, create a post-merge publication child ticket
   and run against the `botster-hub` target, pinned to the exact merge commit.
   That child rechecks registry uniqueness, captures live Hub identity, runs
   `npm publish --access public`, waits for human 2FA/publication confirmation
   when required, installs the registry coordinate externally, and proves all
   hashes and identities.
8. Make final integration, including existing Web consumer ticket
   `ticket_1784912421_508855`, depend on the publication child rather than
   treating the preparation PR as a consumable release.

## Non-scope

- No behavioral redesign of `RefreshLocalPackages`, package refresh, `up`, or
  daemon routing already merged in `af5fb28`.
- No v2 compatibility parser, dual protocol fixture, deprecated alias, feature
  negotiation workaround, or unpublished local-package substitute.
- No new generator, release framework, dependency, optional configuration, or
  adjacent package/runtime refactor.
- No sibling checkout or
  `BOTSTER_HUB_CLIENT_DAEMON_PROTOCOL` override as publication or consumer
  evidence.
- No branch-head publication or byte-identical-tree assumption. Only the exact
  merge commit may be the publication source.
- No direct botster-web or botster-tui edits. Package-coordinate, lockfile,
  pinned-literal, or vendored-protocol updates remain in separately routed
  consumer tickets.
- No Project Pipelines package, plugin, schema, tool, prompt, or workflow-policy
  change.

## Ownership boundaries and cross-repository dependencies

- `botster-hub-client`, maintained here as the externally consumed client
  crate, owns `DaemonRequest`, compatibility constants/descriptors, and the
  authoritative generated TypeScript protocol.
- `botster-hub` owns the Rust-backed Node asset pipeline, npm package contents,
  release documentation, and public registry publication. This PR run owns
  release preparation; a Hub-targeted post-merge child owns publication and
  final registry proof from the exact merge commit.
- `botster-hub-test-support` must derive compatibility values from
  `botster_hub_client`; it must not introduce duplicate version literals as a
  second source of truth.
- `botster-core` owns no changed surface for this publication and is not a
  prerequisite.
- Botster Web and TUI are downstream consumers, not silently included in this
  run. The external clean-Node smoke is required producer proof. Existing
  botster-web ticket `ticket_1784912421_508855`, routed to target
  `tgt_40abcf71ccf049f4ac0c99953a799869`, already depends on this Hub ticket
  and consumes the published coordinate, hashes, and conformance identity. Once
  the publication child exists, make Web's final integration depend on that
  child so a merged preparation PR alone cannot unblock it. Do not create a
  duplicate Web ticket. Route a separate botster-tui ticket to target
  `tgt_c3d470bab78549df920a41e8fb0e58d8` only if its repository still has pinned
  literals, vendored protocol drift, or missing release consumption without
  existing ticket coverage.

## Assumptions and unknowns

- Expected identities are npm `0.1.11`, protocol `3`, and conformance revision
  `18`, based on main and public registry evidence at Plan time. They are
  preflight defaults, not permission to reuse an occupied identity.
- Publication must be generated from and attributed to one exact merged Hub
  SHA. The current PR run does not publish. Its merge triggers creation of a
  post-merge Hub child run pinned to that merge commit; the child verifies a
  clean checkout and exact `origin/main` identity before `npm publish`.
- The repository-supported generator may update every fixture carrying the
  conformance identity even when its semantic body is otherwise unchanged.
  Those derived changes are in scope; unrelated manual fixture edits are not.
- npm authentication, provenance, 2FA, and the current dist-tag remain
  prepublish unknowns owned by the post-merge child. The child provides the
  exact command from its clean merge checkout, waits for human publication
  confirmation when 2FA requires it, and independently verifies the registry
  artifact. A tarball-only result is not publication.
- The compatibility helper accepts a daemon when its protocol and conformance
  values are greater than or equal to the client's minimum
  (`crates/botster-hub-client/src/lib.rs`), so a v2/revision-17 client does not
  reject a v3/revision-18 daemon merely because the daemon is newer. The
  downstream work is updating pinned metadata assertions, package coordinates,
  lockfiles, vendored protocol copies, and drift checks. Web coverage already
  belongs to `ticket_1784912421_508855`; any uncovered TUI work must be routed
  separately.
- No convention waiver remains. The protocol-version conflict was resolved by
  the human in favor of advancing `PROTOCOL_VERSION`; publication sequencing
  was resolved in favor of a merge-pinned child run, never branch publication.

## Implementation sequence

1. Recheck `origin/main`, working-tree cleanliness, npm `latest`, and current
   compatibility constants; record the results.
2. Advance the two Rust compatibility constants and the npm version, then
   update intentional coordinate/version assertions and protocol documentation.
3. Regenerate all package assets through `sync-assets.mjs`; inspect the diff to
   ensure every generated line traces to the version identities or current
   daemon protocol.
4. Add/assert the exact `refresh_local_packages` token in the Node package
   test. Run focused Rust, generation, and Node checks.
5. Run full format, repository test-wrapper, strict workspace lint, whitespace,
   and package-content gates.
6. Pack and install the tarball in a clean consumer outside the repository.
   Verify assets, token, identities, and hashes, and commit the preparation
   report. Do not publish from this branch.
7. Complete Review and Verify, merge the PR, then create a Hub-targeted
   publication child ticket/run whose base is pinned to that exact merge SHA.
   Register the final Web integration dependency against the child.
8. In the clean child checkout, rerun identity/generation/package gates, capture
   real Hub status, publish (waiting for human 2FA confirmation if necessary),
   install the public coordinate in a fresh consumer, verify registry metadata,
   and attach durable final release evidence.

## Affected surfaces and likely files

- `docs/plans/publish-current-daemon-protocol-in-hub-test-support.md` — this
  reviewable plan.
- `crates/botster-hub-client/src/lib.rs` — protocol and conformance constants;
  the already-merged request remains authoritative.
- `crates/botster-hub-client/generated/daemon-protocol.ts` — authoritative
  generated artifact to regenerate/check; its request token already differs
  from the npm copy.
- `packages/hub-test-support/package.json` — next unused npm version.
- `packages/hub-test-support/test.mjs` — package/protocol/conformance assertions
  and explicit `refresh_local_packages` proof.
- `packages/hub-test-support/README.md`, `README.md`, and
  `docs/client-protocol.md` — current coordinate and compatibility claims.
- Generated by `packages/hub-test-support/scripts/sync-assets.mjs`:
  `packages/hub-test-support/daemon-protocol.ts`, `metadata.json`,
  `first-party-client-support-matrix.json`, and conformance fixtures whose
  revision fields change. Do not edit them manually.
- `docs/reports/hub-test-support-<version>-daemon-protocol-release-preparation.md`
  — this PR's generated diff, local gates, tarball smoke, hashes, and exact
  post-merge commands. The publication child attaches final registry evidence
  as a durable Project Pipelines report keyed to the exact merge SHA; any
  repo-visible post-publication report is a later evidence commit and is not
  the published source revision.
- `Cargo.lock` and package export code are not expected to change; include them
  only if the existing generator legitimately requires it and explain why.

## Risks and mitigations

- **Version identity lies about request vocabulary:** bump protocol and
  conformance identities together and assert the exact request token from the
  installed public package.
- **Package version/revision collision:** query public npm and current main
  immediately before editing and again before publish; never reuse immutable
  coordinates or published conformance meanings.
- **Generated copy remains stale:** use only the Rust-backed sync path, run
  check mode, compare byte hashes, and require metadata SHA equality.
- **Local workspace masks missing tarball files:** inspect `npm pack --dry-run`,
  install the tarball outside the repo, then repeat from the public registry.
- **Wrong source revision is published:** require clean exact merged
  `origin/main` identity in the post-merge child and record it beside all
  hashes; never publish from the preparation branch.
- **Partial publication or auth failure:** do not mark the ticket complete from
  generation or packing alone. The child waits for human 2FA/publication
  confirmation, then independently verifies the public coordinate.
- **Downstream repositories retain protocol/conformance literals or stale
  vendored DTOs:** the minimum-version handshake remains additive, but literal
  assertions and copied artifacts can drift. Feed the release evidence into
  existing Web ticket `ticket_1784912421_508855` and route only uncovered TUI
  work separately; do not add v2 compatibility.
- **Documentation claims drift:** update only current package/protocol claims;
  preserve historical release notes and prior plan/report evidence.

## Acceptance checks and downstream proof

### Preparation PR gates

1. Preflight records:
   `git rev-parse HEAD origin/main`,
   `git status --short`, and
   `npm view @trybotster/hub-test-support version dist-tags --json`.
2. Generation:
   `node packages/hub-test-support/scripts/sync-assets.mjs`, followed by
   `node packages/hub-test-support/scripts/sync-assets.mjs --check`.
3. Focused source/asset tests:
   `./test.sh -p botster-hub-client`,
   `./test.sh -p botster-hub-test-support`, and
   `node packages/hub-test-support/test.mjs`.
4. Full repository gates:
   `cargo fmt --all -- --check`,
   `./test.sh`,
   `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and
   `git diff --check`.
5. `./test.sh` must retain
   `tests/hub_daemon_lifecycle_test.rs` coverage that the real `up` path reports
   protocol version from `botster_hub_client::PROTOCOL_VERSION`; final captured
   live identity output belongs to the merge-pinned publication child.
6. Package-content gate from `packages/hub-test-support`:
   `npm pack --dry-run`, then `npm pack`; record the tarball filename and hash.
7. Clean prepublish consumer outside the checkout installs that tarball, imports
   `@trybotster/hub-test-support`, calls `verifyPackageAssets()`, and asserts:
   expected package version, protocol 3, conformance revision 18,
   `refresh_local_packages`, and installed protocol SHA equality with metadata.
8. Commit a preparation report with all commands/results, hashes, tarball
   contents, the exact intended publish command, and the explicit statement
   that no npm publication occurred from the branch.

### Post-merge publication child gates

9. After Review/Verify and merge, create a child ticket/run on target
   `tgt_7e208a0c76a44980a83b63af976b1f22`, pinned to the exact merge commit.
   Require `git rev-parse HEAD origin/main` equality and a clean checkout.
10. Recheck npm uniqueness, rerun generator check/focused package tests, and
    capture the real Hub `up`/status output showing
    `protocol=botster-hub-daemon-v1`, `protocol_version=3`, and
    `conformance_fixture_revision=18`.
11. From that clean merge checkout, provide and run
    `npm publish --access public`. If npm requires human 2FA, ask the human,
    wait for publication confirmation, then independently query the registry.
12. A fresh external consumer installs the exact public coordinate with no
   sibling checkout, override variable, or local tarball. It repeats
   `verifyPackageAssets()`, identity assertions, fixture materialization, and
   the generated request-token assertion.
13. Compute SHA-256 for:
   `crates/botster-hub-client/generated/daemon-protocol.ts`, the installed
   public package's daemon protocol, and its `metadata.daemon_protocol.sha256`.
   All three must match at the recorded Hub SHA.
14. Prove published `metadata.conformance_fixture_revision` equals
    `botster_hub_client::CONFORMANCE_FIXTURE_REVISION` through the Rust-derived
    sync/check tests plus the public consumer's exact numeric assertion.
15. Record
    `npm view @trybotster/hub-test-support@<version> version dist.tarball dist.integrity --json`,
    publish output, merge SHA, hashes, and external smoke output in a durable
    child-run release artifact.
16. Trace the real shipped path in the child report:
    `DaemonRequest`/compatibility constants in `botster-hub-client` ->
    real Hub `up`/status output -> `botster-hub-test-support` Rust emitter ->
    `sync-assets.mjs` ->
    packed/published npm files -> clean external import. Generated file
    existence alone is not sufficient.
17. Make existing Web consumer ticket `ticket_1784912421_508855` depend on the
    publication child before final integration advances. Do not treat this
    preparation PR or its tarball smoke as the consumable release.

## Pipeline artifacts and gates

- Keep this plan committed and update it if implementation changes the
  identity allocation, file boundary, publication sequence, or executable
  checks.
- This run's Implementation evidence must include both human decisions,
  generated diff, focused/full gate output, tarball contents/hash, clean local
  tarball consumer output, preparation report, and exact post-merge commands.
  It must explicitly attest that no npm publication occurred from the branch.
- The post-merge publication child owns exact merge SHA, live-daemon output,
  publish command/result, human 2FA coordination, npm registry metadata, clean
  public-consumer output, hashes, and durable final release evidence.
- Review and Verify must load Hub, hub-client, runtime, and package surfaces;
  reject stale generated bytes, version aliases, branch-only publication,
  missing strict gates, or a preparation report that claims registry
  publication. Final integration remains blocked until the child crosses the
  public npm boundary and updates downstream dependency routing.

## Vault gaps worth capturing

- Existing notes already cover protocol-versus-conformance identity,
  publication collisions, Rust-derived assets, strict gates, and external npm
  consumer proof.
- Capture the approved reusable rule after implementation: PR-gated immutable
  package releases prepare and verify on the branch, then publish from a child
  run pinned to the exact merge commit; downstream integration depends on that
  publication child, not the preparation PR. Keep one-off
  SHA/version/integrity values in release artifacts rather than the atomic note.
