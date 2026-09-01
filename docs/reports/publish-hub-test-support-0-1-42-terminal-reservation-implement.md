# Implementation report: publish hub-test-support 0.1.42

Ticket: `ticket_1788280618_295967`
Run: `run_1788280945_468802`
Target repository: `trybotster/botster-hub`
Target id: `tgt_7e208a0c76a44980a83b63af976b1f22`

## Result

The operator published `@trybotster/hub-test-support@0.1.42` through
`botster-hub/script/publish-npm-packages`.

The published artifact matches the package packed from Hub commit
`b4020a976010f4ec495c89efd6ea66271e02712f`.

The public registry reports this integrity:

`sha512-Sorc7CFj7K5E4YZ+2wYJm5ONprALZ5fn0YiByJfNdIRwrUBhXMPZNTJUKQ0NEAcS5dvT/4qWnLUq0AVk2+YvHg==`

The public tarball is:

`https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.42.tgz`

The operator's original npm console output was not available to this run.
Fresh public registry evidence and the clean consumer proof replace that
missing transcript. The run did not request a second publication.

## Guidance applied

Repository playbook:

- [[botster-hub-playbook]]

Role and workflow playbooks:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[project-pipelines-playbook]]

Targeted notes:

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

The Implement overlay's required Botster architecture, CLI, SPA, plugin,
client-copy, MCP-routing, and test-wrapper notes were also loaded. They did
not add a product constraint to this distribution-only change.

## Files changed

- `docs/reports/publish-hub-test-support-0-1-42-terminal-reservation-implement.md`
- `docs/reports/publish-hub-test-support-0-1-42-terminal-reservation-evidence.json`

No package source, generated source, fixture, metadata, or runtime file changed.

## Ownership boundaries

Hub owns the generated protocol mirror, support package, and npm publication.
This run changed only Hub-owned release evidence.

The run did not change Botster Web. Botster Web owns the consumer pin, vendored
protocol, drift gate, and documentation update in
`ticket_1787600676_914408`.

The run did not change Botster Core or `@trybotster/ui-contract`.
The support package continues to use the published exact dependency
`@trybotster/ui-contract@0.3.3`.

## Assumptions

- The operator used the repository publish script from the intended Hub source.
- Registry integrity equality proves that the published package bytes match
  the package packed from commit `b4020a9`.
- The clean install used npm registry URLs and no workspace link, `file:`
  dependency, or local tarball.

## Deviations from plan

The operator used `botster-hub/script/publish-npm-packages` instead of the
plan's direct `npm publish --access public` command. The script completed the
same human credentialed public publish path.

The original operator console output was unavailable. The run records this
gap and uses fresh registry evidence, integrity equality, and clean installs.

No package content or acceptance check changed.

## Verification

Pre-publish repository proof:

- `npm install --no-save`: passed.
- `npm run check`: passed with `hub test-support package assets are current`.
- `npm test`: passed with
  `hub test-support package import and fixture materialization passed`.
- `git diff --name-only b4020a9..HEAD`: only the Plan artifact existed before
  this report.
- `git status --porcelain`: empty before report creation.

Registry precondition proof:

- `npm view @trybotster/hub-test-support@0.1.42 version`: returned `404` before publication.
- `npm view @trybotster/hub-test-support versions --json`: ended at `0.1.41` before publication.
- `npm view @trybotster/ui-contract version`: returned `0.3.3`.

Intended commit package proof:

- `git archive b4020a9 packages/hub-test-support | tar -x -C <scratch>`: passed.
- `npm pack --json`: produced package integrity
  `sha512-Sorc7CFj7K5E4YZ+2wYJm5ONprALZ5fn0YiByJfNdIRwrUBhXMPZNTJUKQ0NEAcS5dvT/4qWnLUq0AVk2+YvHg==`.

Post-publish registry proof:

- `npm view @trybotster/hub-test-support@0.1.42 version dist.integrity dist.tarball --json`: passed.
- The registry integrity equals the intended commit package integrity.
- The published versions list now ends at `0.1.42`.

External clean consumer proof:

- A new directory under `/private/tmp` ran `npm init -y`.
- `npm install @trybotster/hub-test-support@0.1.42`: passed.
- `npm ls @trybotster/hub-test-support @trybotster/ui-contract --all`: resolved
  `0.1.42` and transitive UI Contract `0.3.3`.
- The lockfile resolved the support package from the public registry tarball.
- The lockfile did not mark the package as a link.
- The installed package root was inside the temporary consumer's `node_modules`.
- The ESM consumer smoke printed `clean consumer smoke passed`.

The installed consumer asserted:

- package version `0.1.42`;
- protocol version `8`;
- conformance fixture revision `47`;
- UI Contract version `0.3.3`;
- daemon protocol SHA-256
  `8940d99b2e1035b77a9ce94fae8597d246490e5d9673ab084cff8ff04749989a`;
- `verifyPackageAssets()` returned `{ ok: true, failures: [] }`;
- `DaemonTerminalReservation` and all six fields exist;
- `terminal_reservation` exists in `DaemonResponse` and `DaemonResponseKind`;
- `mode_gated_input` is absent;
- the installed daemon protocol file digest matches metadata;
- plugin contract matrix files materialize;
- the package includes `LICENSE`.

Prior-version positive control:

- A separate clean directory installed `@trybotster/hub-test-support@0.1.41`.
- The installed artifact reports protocol version `7` and revision `46`.
- The installed artifact contains `mode_gated_input`.
- The installed artifact does not contain `DaemonTerminalReservation`.

This positive control proves that the removal assertion distinguishes the new
registry artifact from the prior published artifact.

## Production-path proof

This ticket changes npm distribution, not runtime code. The production path is
an external consumer that resolves the public registry coordinate and reads the
installed package exports. The clean consumer exercised that path.

## Downstream unblock facts

- Hub commit: `b4020a976010f4ec495c89efd6ea66271e02712f`.
- Coordinate: `@trybotster/hub-test-support@0.1.42`.
- Protocol version: `8`.
- Conformance fixture revision: `47`.
- UI Contract coordinate: `@trybotster/ui-contract@0.3.3`.
- Daemon protocol SHA-256:
  `8940d99b2e1035b77a9ce94fae8597d246490e5d9673ab084cff8ff04749989a`.
- Registry integrity:
  `sha512-Sorc7CFj7K5E4YZ+2wYJm5ONprALZ5fn0YiByJfNdIRwrUBhXMPZNTJUKQ0NEAcS5dvT/4qWnLUq0AVk2+YvHg==`.

These facts unblock Botster Web `ticket_1787600676_914408`.

## Residual risk and unverified behavior

- The operator console output is unavailable.
- The run did not execute Botster Web's downstream drift gate because that work
  belongs to the separately routed Web ticket.
- The run did not test runtime terminal reservation behavior. This ticket only
  publishes the already-merged DTO artifact.

Runtime teardown does not apply.

## Missing vault guidance

The vault does not yet record these durable facts:

1. Hub Test Support `0.1.42` is the terminal reservation DTO baseline at
   protocol version `8` and revision `47` from Hub commit `b4020a9`.
2. A removal-shaped package smoke needs a positive control against the prior
   published coordinate.
3. A support package release does not need a paired UI Contract publish when
   its exact UI Contract pin already exists in the registry.

This run did not write outside its routed Hub worktree. The implementation
checklist records these three capture candidates for a later vault pipeline.
