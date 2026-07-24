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
6. Verify the complete packed tarball in a clean temporary consumer before
   publication, then publish with `npm publish --access public` from
   `packages/hub-test-support`.
7. Install the public registry coordinate in a second clean consumer and prove:
   package metadata/version, protocol 3, conformance revision 18, asset
   verification, request token presence, and exact daemon protocol SHA-256
   equality with the authoritative generated file at the recorded Hub SHA.
8. Persist an implementation/release report with the package coordinate, exact
   Hub source SHA, generated and installed hashes, conformance identity,
   tarball/integrity, publish command/result, external-consumer command/result,
   and any downstream compatibility findings.

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
- No direct botster-web or botster-tui edits. Incompatible merged consumers
  require separately routed tickets against their own repository targets.
- No Project Pipelines package, plugin, schema, tool, prompt, or workflow-policy
  change.

## Ownership boundaries and cross-repository dependencies

- `botster-hub-client`, maintained here as the externally consumed client
  crate, owns `DaemonRequest`, compatibility constants/descriptors, and the
  authoritative generated TypeScript protocol.
- `botster-hub` owns the Rust-backed Node asset pipeline, npm package contents,
  release documentation, and public registry publication.
- `botster-hub-test-support` must derive compatibility values from
  `botster_hub_client`; it must not introduce duplicate version literals as a
  second source of truth.
- `botster-core` owns no changed surface for this publication and is not a
  prerequisite.
- Botster Web and TUI are downstream consumers, not silently included in this
  run. The external clean-Node smoke is required producer proof. If exact
  merged Web or TUI rejects protocol 3, create repository-owned consumer
  tickets against `botster-web` target
  `tgt_40abcf71ccf049f4ac0c99953a799869` or `botster-tui` target
  `tgt_c3d470bab78549df920a41e8fb0e58d8`, register dependencies there, and do
  not weaken Hub's version contract.

## Assumptions and unknowns

- Expected identities are npm `0.1.11`, protocol `3`, and conformance revision
  `18`, based on main and public registry evidence at Plan time. They are
  preflight defaults, not permission to reuse an occupied identity.
- Publication must be generated from and attributed to one exact merged Hub
  SHA. Before `npm publish`, verify the release source is the intended
  `origin/main` commit and clean. If the PR pipeline cannot provide a
  post-merge execution point, stop and ask the human rather than publishing a
  branch-only build and calling it main.
- The repository-supported generator may update every fixture carrying the
  conformance identity even when its semantic body is otherwise unchanged.
  Those derived changes are in scope; unrelated manual fixture edits are not.
- npm authentication, provenance, 2FA, and the current dist-tag remain
  prepublish unknowns. Authentication failure blocks acceptance; a tarball-only
  result is not publication.
- The exact downstream compatibility impact is unknown until the new
  descriptor is exercised. Consumer failures belong to separately routed
  repositories, as directed by the human answer.
- No convention waiver remains. The protocol-version conflict was resolved by
  the human in favor of advancing `PROTOCOL_VERSION`.

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
   Verify assets, token, identities, and hashes before publication.
7. From the exact clean merged Hub SHA, recheck registry uniqueness and publish
   the public package.
8. Install the public coordinate in a fresh second consumer, repeat all
   identity/hash/token assertions, query npm tarball/integrity metadata, and
   write the release report.

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
- `docs/reports/hub-test-support-<version>-daemon-protocol-release.md` — final
  publication and external-consumer evidence, with the exact version resolved
  at implementation.
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
  `origin/main` identity at publication and record it beside all hashes.
- **Partial publication or auth failure:** do not mark the ticket complete from
  generation or packing alone; ask the human if public publish cannot finish.
- **Downstream clients reject protocol 3:** preserve the honest contract and
  route Web/TUI updates to their repository targets instead of adding v2
  compatibility.
- **Documentation claims drift:** update only current package/protocol claims;
  preserve historical release notes and prior plan/report evidence.

## Acceptance checks and downstream proof

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
5. Package-content gate from `packages/hub-test-support`:
   `npm pack --dry-run`, then `npm pack`; record the tarball filename and hash.
6. Clean prepublish consumer outside the checkout installs that tarball, imports
   `@trybotster/hub-test-support`, calls `verifyPackageAssets()`, and asserts:
   expected package version, protocol 3, conformance revision 18,
   `refresh_local_packages`, and installed protocol SHA equality with metadata.
7. Publication runs only after exact merged-main identity is rechecked:
   `npm publish --access public`.
8. A fresh external consumer installs the exact public coordinate with no
   sibling checkout, override variable, or local tarball. It repeats
   `verifyPackageAssets()`, identity assertions, fixture materialization, and
   the generated request-token assertion.
9. Compute SHA-256 for:
   `crates/botster-hub-client/generated/daemon-protocol.ts`, the installed
   public package's daemon protocol, and its `metadata.daemon_protocol.sha256`.
   All three must match at the recorded Hub SHA.
10. Prove published `metadata.conformance_fixture_revision` equals
    `botster_hub_client::CONFORMANCE_FIXTURE_REVISION` through the Rust-derived
    sync/check tests plus the public consumer's exact numeric assertion.
11. Record
    `npm view @trybotster/hub-test-support@<version> version dist.tarball dist.integrity --json`,
    publish output, source SHA, hashes, and external smoke output in the
    implementation/release report.
12. Trace the real shipped path in the report:
    `DaemonRequest`/compatibility constants in `botster-hub-client` ->
    `botster-hub-test-support` Rust emitter -> `sync-assets.mjs` ->
    packed/published npm files -> clean external import. Generated file
    existence alone is not sufficient.

## Pipeline artifacts and gates

- Keep this plan committed and update it if implementation changes the
  identity allocation, file boundary, publication sequence, or executable
  checks.
- Implementation evidence must include the human protocol-version decision,
  exact merged source SHA, generated diff, focused/full gate output, tarball
  contents/hash, publish command/result, npm registry metadata, and clean public
  consumer output.
- Review and Verify must load Hub, hub-client, runtime, and package surfaces;
  reject stale generated bytes, version aliases, branch-only publication,
  unpublished-local consumption, missing strict gates, or evidence that does
  not cross the public npm boundary.

## Vault gaps worth capturing

- Existing notes already cover protocol-versus-conformance identity,
  publication collisions, Rust-derived assets, strict gates, and external npm
  consumer proof.
- Capture a new atomic note only if implementation establishes a reusable rule
  for sequencing public package publication from an exact merged-main SHA in a
  PR-gated pipeline. Otherwise record the one-off SHA/version/integrity only in
  the release report and Project Pipelines artifacts.
