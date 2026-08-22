# Implement report: verify published ui-contract 0.3.3 and hub-test-support 0.1.41

Ticket: `ticket_1787351279_697528`
Run: `run_1787351732_490500`
Step: `botster_stack_implement` / `run_step_1787362110_315500`
Plan: revision 5, `docs/plans/verify-published-ui-contract-0-3-3-and-hub-test-support-0-1-41.md`
Prior Implement commit: `affb85b`
Review return: `review_1787362097_377433` (`changes_required`)
`teardown_class_applies`: no

## Review-return corrections

`review_1787362097_377433` approved the product proof and returned two artifact
findings. This pass changes only committed pipeline documents.

| Finding | Fix |
|---|---|
| `finding_1787362097_868213` | Four wiki-links in the plan spanned a newline. Each wiki-link now sits on one line. A resolver then checked every committed wiki-link title against the matching vault note filename. Zero remaining multiline titles. Zero missing files. |
| `finding_1787362097_665686` | The plan said "No repository file changes." Plan revision 5 now says "No package or product source files change. This run adds only the plan and verification artifacts." and lists the three paths. |

No package source, version, metadata, or publish command changed.

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Worktree | pipeline-provided ticket worktree |
| Branch | `project-pipelines/ticket_1787351279_697528` |
| Proved source commit | `e950f4f0d5d1d7953eb5d9f378330ea044b0be1c` |
| Ticket branch after fast-forward | `08298d9d2e631e962b00e2578e1840783dc18010` (`origin/main`) |
| Pipeline | `botster_stack_delivery` |
| `merge_policy` | `direct` |

Independent resolution: `project_pipelines_current_context` ticket/run `target_id` and `list_spawn_targets` both map `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing. Implementation stayed in this run worktree.

## Repository playbook and other playbooks/notes applied

Loaded before any edit:

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-architecture]]
- [[botster-hub-playbook]]
- [[cli-patterns]]

Targeted atomic notes:

- [[Core types-only npm releases use human public publish and clean install proof]]
- [[hub test support npm releases need external consumer smoke]]
- [[an unmerged run that publishes an npm coordinate burns it]]
- [[conformance fixture revisions must be unique per published content]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[hub generated protocol changes are a four site release chain]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[Hub test support version bumps must update the Node mirror test literals]]
- [[client notice reactions belong to package declarations not client constants]]
- [[implementation artifacts must match actual git state]]
- [[implementation steps must persist report artifacts for review]]
- [[implement gate must verify committed work and pr link before review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[botster review and verify must scan all committed artifacts for pii]]
- [[implementation reports separate merge cleanup from feature behavior]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[plan steps need reviewable plan artifacts]]
- [[zero diff already delivered pipeline runs need terminal disposition]]
- [[test script required for rust tests not cargo test]]
- [[verify must recheck resolved findings against the live worktree]]

Not loaded, with reasons:

- [[project-pipelines-playbook]]: no Project Pipelines package or plugin path is in scope.
- [[botster runtime teardown lenses]]: the approved plan is not runtime-teardown class.
- [[hotwire-app-implementer-playbook]]: `botster-hub` is a Rust workspace, not a Hotwire Rails app.

## Constraints applied before edits

- Work only in the routed `botster-hub` run worktree.
- Do not publish, republish, force, or deprecate any npm coordinate.
- Do not edit package source, versions, or metadata.
- Treat the registry as the authority for what shipped.
- Pack comparison tarballs from `e950f4f`, not from the ticket worktree.
- Prove the public package API from a clean registry install, not a workspace link or local tarball.
- Do not open a pull request. `merge_policy` is `direct`.
- Do not run Hub `./test.sh`. This ticket does not change Rust or daemon source.

## Feature behavior implemented

None in this repository. Jason already published both coordinates. This Implement step re-ran the post-publication proof and recorded it.

Site 3 of [[hub generated protocol changes are a four site release chain]] is complete.

## Merge or rebase cleanup

The ticket branch started at `12e0cc6`. That commit still carries unpublished-looking `packages/hub-test-support` version `0.1.40`.

`origin/main` already contains:

- `e950f4f`, the proved publish source
- `08298d9`, the closed `ticket_1787353310_106098` repair of five `test.mjs` literals

I fast-forwarded the ticket branch to `origin/main` at `08298d9` so this run does not sit behind the published source. That is git hygiene, not a package edit.

`git diff e950f4f 08298d9 -- packages/ui-contract packages/hub-test-support` lists only `packages/hub-test-support/test.mjs`. That file is absent from `package.json` `files[]`, so it cannot change published tarball bytes.

## Files changed

Pipeline handoff only:

- `docs/plans/verify-published-ui-contract-0-3-3-and-hub-test-support-0-1-41.md` — Plan revision 5. Unwrapped four wiki-links. Replaced the false "No repository file changes" sentence. Listed the three committed artifact paths.
- `docs/reports/verify-published-ui-contract-0-3-3-and-hub-test-support-0-1-41-implement.md` — this report, including the Review-return corrections
- `docs/reports/verify-published-ui-contract-0-3-3-and-hub-test-support-0-1-41-evidence.json` — registry, integrity, and smoke evidence; product bytes unchanged

Not changed: package source, `package.json` versions, `metadata.json`, generated protocol, Rust, or publish scripts.

## Ownership boundaries preserved

| Boundary | Owner | This ticket |
| --- | --- | --- |
| `@trybotster/ui-contract` and `@trybotster/hub-test-support` npm publication | botster-hub | verified site 3; no republish |
| Generated TypeScript emitter | botster-hub-client inside this repo | unread as a change surface |
| botster-web pin of these coordinates | botster-web | separately routed |
| `botster-ui-contract-v0.3.3` Git tag | botster-hub, other ticket | not this run |
| `test.mjs` literal repair | botster-hub, closed ticket | not this run |

## Cross-repo dependencies or separately routed work

- `botster-web` ticket `ticket_1787278327_274484` owns site 4. It can now pin `@trybotster/hub-test-support@0.1.41` at conformance revision 46 and `@trybotster/ui-contract@0.3.3`. This run does not edit that repository.
- `ticket_1787349524_364728` publishes the Git tag `botster-ui-contract-v0.3.3` for Rust consumers. Neither ticket blocks the other.
- `ticket_1787353310_106098` repaired five stale `test.mjs` literals. It is closed. `dependency_1787353605_290826` is closed. The repair does not change published bytes.
- `botster-core` still supplies protocol fixtures through the pinned crate source. This run does not change the Core pin.

## Deviations from plan

None in product scope.

Git hygiene from the first Implement pass: the ticket branch fast-forwarded from `12e0cc6` to `origin/main` `08298d9` so the verification documents land on current main rather than on the pre-publish `0.1.40` tree.

Review-return plan resync: Plan revision 5 now names the three committed artifact paths. That matches `origin/main...HEAD`. This follows [[implementation deviations must resync committed plan acceptance checks]].

No publish command ran. No source, version, or metadata edit ran.

## Tests and downstream proof run

Independent Implement proof, not a copy of the Plan artifact.

### 1. Registry coordinates

- `npm view @trybotster/ui-contract versions` ends at `0.3.3`.
- `npm view @trybotster/hub-test-support versions` ends at `0.1.41`. The list skips `0.1.40`.
- `npm view @trybotster/hub-test-support@0.1.40 version` returns npm `E404`.

### 2. Published bytes equal `e950f4f`

Packed both packages from `e950f4f` with `git archive`, then compared local `npm pack` integrity with registry `dist.integrity`.

| coordinate | registry `dist.integrity` | local pack from `e950f4f` | equal |
|---|---|---|---|
| `@trybotster/ui-contract@0.3.3` | `sha512-+c34Bd5pnELt/HYaKEK5nI1oF1GeRgTejOCIAeAcSF8FCg2wkmJsIac5DLBGgOlWebfyLPQYAmtNAQWbe73eEw==` | same | yes |
| `@trybotster/hub-test-support@0.1.41` | `sha512-LXH9DscSoDvNytbkmhUsiwGXwcAYb93d6/hu2L/PViuQ5Xg8UelWFS8TVItl/KwRGZt90Qyy868Xb7C0zEuC5w==` | same | yes |

The hub-test-support pack lists 16 files and does not include `test.mjs`.

### 3. Clean registry install

Method: empty temporary directory outside the repository, `npm init -y`, then `npm install` of registry coordinates. No workspace link. No `file:` dependency. No packed tarball.

- Direct install of `@trybotster/ui-contract@0.3.3` and `@trybotster/hub-test-support@0.1.41` succeeds.
- Install of `@trybotster/hub-test-support@0.1.41` alone resolves `@trybotster/ui-contract@0.3.3` transitively from the registry. The installed `package.json` reports `0.3.3`.

That transitive resolution is the production path this ticket exists to deliver.

### 4. Public package API

The Plan revision 4 ESM script ran against the transitive registry install during the first Implement pass. Review independently confirmed that product proof. This Review-return pass did not rerun npm, because no package file changed.

Result: `clean consumer smoke passed`, exit 0.

Assertions executed:

- `resolveNoticeText` is a function
- `NOTICE_TEXT_MAX_BYTES` equals 512
- installed `index.d.ts` declares `PackageNoticeReactionDescriptor` and `PackageNoticeReactionDeclaration`
- `metadata.package_version` equals `0.1.41`
- `metadata.ui_contract.package_version` equals `0.3.3`
- `metadata.protocol_version` equals 7
- `metadata.conformance_fixture_revision` equals 46
- `verifyPackageAssets()` passes
- `readDaemonProtocolTypescript()` matches `notice_reactions?: PackageNoticeReactionDescriptor[]` and `export interface DaemonPackage`
- sha256 of installed `daemon-protocol.ts` equals `metadata.daemon_protocol.sha256` and equals `14121c4b1aa15f0728040b7ab3cc0189bf7720dc3159d994926d54e0251c5996`
- `materializePluginContractMatrixFixture` returns a directory containing `botster-package.json` and `plugin.lua`
- `LICENSE` exists in each installed package root

The script resolved each exported root entrypoint with `path.dirname(require_.resolve(name))`. It did not resolve `./package.json`.

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

`./test.sh` was not run. No Rust or daemon path changed.

Repository `npm test` for `packages/hub-test-support` was not run in this Review-return pass. Review already reported both package tests green. This pass changes only pipeline documents.

Wiki-link resolver: every wiki-link in the three committed artifacts was extracted with a multiline matcher. A title that contains a newline fails. Each remaining title must exist as a vault note file whose name equals that title. Result after the Plan revision 5 edits: zero newline-broken titles, zero missing files. The sentence "No repository file changes" is absent from the plan.

## Unverified behavior or residual risk

- npm versions are immutable. This run cannot repair a future stale-tree publish of a new coordinate. The integrity comparison retires that risk for `0.3.3` and `0.1.41` only.
- Site 4 remains on `botster-web`. This run does not prove that consumer pin.
- The Git tag `botster-ui-contract-v0.3.3` is a separate ticket. This run does not prove Cargo consumers.
- I did not re-publish, so I did not re-prove the human credentialed publish path. The registry bytes and the clean install are the remaining production path.

## Missing vault guidance discovered

The Plan listed these gaps. This Implement step did not capture them. They belong in the knowledge vault pipeline, not in this hub ticket:

1. Record `@trybotster/ui-contract@0.3.3` and `@trybotster/hub-test-support@0.1.41` at conformance revision 46 as the published package-owned notice-reaction cutover, and record that `0.1.40` at revision 45 was allocated and then skipped.
2. Comparing registry `dist.integrity` against a tarball packed from the intended commit is the direct retirement of the stale-tree publish risk.
3. `ui-contract` and `hub-test-support` publish as an ordered pair, because `hub-test-support` pins `ui-contract` exactly and imports it at runtime.
4. A scoped package with an `exports` map does not export `./package.json`, so a consumer smoke must resolve the exported root entrypoint.
5. A hub-test-support version bump must update the package test literals as well as the shipped metadata and fixtures. Closed `ticket_1787353310_106098` and [[Hub test support version bumps must update the Node mirror test literals]] now cover the literal half.
6. Ancestor containment is not release source identity.

No convention conflict was found. The loaded notes agree that this run must not publish and must prove the installed registry packages.

## Assumptions

- The registry is the authority for what shipped. Local trees are evidence about intent, not about delivery.
- `merge_policy: direct` means this run must not open a pull request.
- Fast-forwarding onto `origin/main` is required so a later direct merge does not rewind published source.
