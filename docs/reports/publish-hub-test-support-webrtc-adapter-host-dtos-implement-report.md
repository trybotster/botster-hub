# Implement report: publish hub-test-support with WebRTC adapter host DTOs

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Ticket | `ticket_1786723348_522242` |
| Run | `run_1786724333_797392` |
| Worktree | pipeline-provided ticket worktree |
| Branch | `project-pipelines/ticket_1786723348_522242` |
| Authoritative HEAD at plan time | `24517f4879a6effdd87eacddbb4b40aca13104c1` (`origin/main`) |
| Published tree SHA | `e1999a08c328577ccbccf939719f69b4d5495456` |
| Plan | `docs/plans/publish-hub-test-support-webrtc-adapter-host-dtos.md` revision 2 |
| `teardown_class_applies` | no |

Independent target resolution via `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster playbooks compose role with changed surface overlays]]
- [[hub generated protocol changes are a four site release chain]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[hub test support npm releases need external consumer smoke]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[ready then history cutover uses Hub test support version 0.1.33]]
- [[conformance fixture revisions must be unique per published content]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[published fixture readmes are part of the shipped contract]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub is a first party host profile over core]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[implementation artifacts must match actual git state]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[implementation deviations must resync committed plan acceptance checks]]
- [[pipeline artifacts should use path neutral worktree references]]

Not loaded: [[project-pipelines-playbook]] (this ticket does not change Project Pipelines package or plugin paths). Not loaded: [[botster runtime teardown lenses]] (this ticket is not runtime-teardown class).

There is no implement-stage packages overlay. [[botster-package-reviewer-playbook]] is the packages-surface reviewer overlay and was read for surface awareness only.

## Constraints applied before edits

- Work only in the routed `botster-hub` run worktree.
- Publish already-synced host-plane DTO tokens. Do not change terminal adapter policy or inspect terminal bodies.
- Pack the README-corrected tree before any occupancy decision. Accept occupied `0.1.35` only when `dist.integrity` equals that corrected tarball.
- Keep `protocol_version` 7 and `@trybotster/ui-contract@0.3.2`.
- Merge directly into `main`. Do not create a pull request (`merge_policy: direct`).
- Do not run Hub `./test.sh` unless Rust or daemon source changes.

## Files changed

Feature behavior:

- `packages/hub-test-support/README.md` — prior published coordinate is now `0.1.33` at protocol 7 / revision 38. Removed the stale claim that `0.1.32` is the prior coordinate at the same protocol and revision.

Pipeline handoff:

- `docs/plans/publish-hub-test-support-webrtc-adapter-host-dtos.md` — approved revision 2 plan (Plan step)
- `docs/reports/publish-hub-test-support-webrtc-adapter-host-dtos-implement-report.md` — this report
- `docs/reports/publish-hub-test-support-webrtc-adapter-host-dtos-release-evidence.json` — coordinate, integrity, and SHA evidence

Not changed: `packages/hub-test-support/package.json`, `metadata.json`, `test.mjs`, Rust adapter/daemon/emitter files. Occupied `0.1.35` did not exist, so no version reallocation was required. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passed on the publish tree.

## Ownership boundaries preserved

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host control DTOs and npm `@trybotster/hub-test-support` | botster-hub | published site 3 |
| Generated TypeScript emitter | botster-hub-client inside this repo | consumed already-synced bytes |
| Core terminal protocol package | botster-core | sibling `ticket_1786723347_177328`; not absorbed |
| Web pin and vendored protocol | botster-web | downstream `ticket_1786661008_897067`; already registered |
| WebRTC adapter-close host event | botster-hub runtime | sibling `ticket_1786724303_284888`; not absorbed |

No Rust adapter, daemon, or emitter files were edited. Hub remains content-blind. Terminal-body opacity is unchanged.

## Cross-repo dependencies or separately routed work

No new cross-repo dependency was required. Downstream Web ticket `ticket_1786661008_897067` already depends on this Hub ticket. After this run merges, that consumer can pin `@trybotster/hub-test-support@0.1.35`.

Sibling work stays out of this run:

- Core `ticket_1786723347_177328` — publish `@trybotster/terminal-protocol`
- Hub `ticket_1786724303_284888` — emit `TerminalSubscriptionClosed` on WebRTC

## Deviations from plan

None. Occupancy used the README-corrected tarball. `0.1.35` was unused, so that coordinate was published. `npm whoami` succeeded as the existing `trybotster` publisher account; no human auth question was required.

## Published artifact

| Field | Value |
| --- | --- |
| Coordinate | `@trybotster/hub-test-support@0.1.35` |
| Registry latest after publish | `0.1.35` |
| `dist.integrity` | `sha512-KIUC4JKaeZHGtUz6wq0P1vS1ByepFShIETVdiSDvvVeYpBbLihCCb41I4ptRmDLMu3nmRqWIMQYCLcA59SXKOQ==` |
| `dist.tarball` | `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.35.tgz` |
| Hub SHA packed and published | `e1999a08c328577ccbccf939719f69b4d5495456` |
| `protocol_version` | 7 |
| `conformance_fixture_revision` | 40 |
| UI contract | `@trybotster/ui-contract@0.3.2` |
| ui-contract published integrity | `sha512-lWzx8j2Z+OQhRAhURWpqUSLQyAMFycOOV6MgFAfMHRgU/27bkyc0v95komBGdJjv5IWjMRWJHlgmX7GlXGWR0Q==` |

`script/publish-npm-packages` skipped `@trybotster/ui-contract@0.3.2` after the packed tarball integrity matched the already-published coordinate.

## Tests and downstream proof run

```sh
node packages/hub-test-support/scripts/sync-assets.mjs --check
# hub test-support package assets are current

node packages/hub-test-support/test.mjs
# hub test-support package import and fixture materialization passed

script/publish-npm-packages --dry-run
# Dry run passed for @trybotster/ui-contract@0.3.2 and @trybotster/hub-test-support@0.1.35
# Packed hub-test-support integrity matched the pre-occupancy corrected tarball.

npm view @trybotster/hub-test-support versions --json --prefer-online
# latest published before publish: 0.1.33; 0.1.34 and 0.1.35 unused

npm whoami --prefer-online
# existing trybotster publisher account

script/publish-npm-packages
# ui-contract 0.3.2 skipped on matching integrity
# + @trybotster/hub-test-support@0.1.35

npm view @trybotster/hub-test-support version --prefer-online
# 0.1.35

npm view @trybotster/hub-test-support@0.1.35 dist.integrity --prefer-online
# sha512-KIUC4JKaeZHGtUz6wq0P1vS1ByepFShIETVdiSDvvVeYpBbLihCCb41I4ptRmDLMu3nmRqWIMQYCLcA59SXKOQ==
```

External smoke: clean temporary npm project, then
`npm install @trybotster/hub-test-support@0.1.35 --prefer-online`.

Installed-package assertions:

- `metadata.package_version === "0.1.35"`
- `metadata.protocol_version === 7`
- `metadata.conformance_fixture_revision === 40`
- `metadata.ui_contract.package_version === "0.3.2"`
- `verifyPackageAssets()` succeeded
- `readDaemonProtocolTypescript()` contains `DaemonLocalWebrtcDeliveryKind`, `"daemon_terminal_frame"`, optional `DaemonHello.terminal_compatibility`, and optional `DaemonHelloAck.terminal_compatibility`
- installed package files contain `webrtc_terminal_adapter` and `terminal_subscription_closed`
- installed README names `0.1.33` as the prior published coordinate at protocol 7 / revision 38
- installed README does not claim `0.1.32` is the prior coordinate at the same protocol and revision

Did not run Hub `./test.sh`. No Rust or daemon source changed.

## Unverified behavior or residual risk

- `0.1.35` is now immutable. Recovery from a later content defect requires a new unused patch.
- Downstream Web still has to pin and vendor this coordinate. That remains `ticket_1786661008_897067`.
- WebRTC still does not emit `TerminalSubscriptionClosed`. That remains `ticket_1786724303_284888`. The published DTO and support-matrix token exist; the runtime event on WebRTC does not.
- Registry visibility was confirmed immediately after publish. Later CDN replica lag is not expected after `npm view` already returned the coordinate.

## Missing vault guidance discovered

None required to implement this ticket. The four-site chain, unpublished-version cutover, README-as-contract, and external-smoke notes already covered the work.

Optional durable capture after publish: inbox note `webrtc adapter host DTO cutover uses Hub test support version 0.1.35`.
