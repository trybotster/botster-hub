# Implement report: publish hub-test-support with daemon_event close negotiation

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Ticket | `ticket_1786730686_674642` |
| Run | `run_1786734357_656172` |
| Worktree | pipeline-provided ticket worktree |
| Branch | `project-pipelines/ticket_1786730686_674642` |
| Authoritative HEAD at plan time | `b11ff3d0aeac5d5a46ea069067be7d945a0f8798` (`origin/main`) |
| Published tree SHA | `924ea9790ec42c1ea2ed18622bcdbd489a532c34` |
| Plan | `docs/plans/publish-hub-test-support-daemon-event-close-negotiation.md` revision 2 |
| `teardown_class_applies` | no |

Independent target resolution via `list_spawn_targets` maps `tgt_7e208a0c76a44980a83b63af976b1f22` to `trybotster/botster-hub`. The approved plan used the same routing.

## Repository playbook and other playbooks/notes applied

- [[implementer-playbook]]
- [[botster-implementer-playbook]]
- [[botster-hub-playbook]]
- [[botster hub is a first party host profile over core]]
- [[hub generated protocol changes are a four site release chain]]
- [[closed dependency tickets signal merged source not a consumable release]]
- [[hub test support npm releases need external consumer smoke]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[WebRTC adapter host DTO cutover uses Hub test support version 0.1.35]]
- [[Protocol 7 gates WebRTC daemon events on close-event Hello negotiation]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[published fixture readmes are part of the shipped contract]]
- [[implement gate must verify committed work and pr link before review]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[project pipelines mcp create calls can time out after committing]]
- [[project pipelines checklist worker timeouts require artifact evidence fallback]]

Not loaded: [[project-pipelines-playbook]] (this ticket does not change Project Pipelines package or plugin paths). Not loaded for implementation: [[botster runtime teardown lenses]] (this ticket is not runtime-teardown class).

## Constraints applied before edits

- Work only in the routed `botster-hub` run worktree.
- Publish already-synced host-plane tokens. Do not change terminal adapter policy or inspect terminal bodies.
- Commit the README-corrected tree before any `script/publish-npm-packages` invocation.
- Keep `protocol_version` 7, conformance fixture revision 41, and `@trybotster/ui-contract@0.3.2`.
- Keep `terminal_subscription_closed` optional in default `required_features`.
- Merge directly into `main`. Do not create a pull request (`merge_policy: direct`).
- Do not run Hub `./test.sh` unless Rust or daemon source changes.

## Files changed

Feature behavior:

- `packages/hub-test-support/README.md` — prior published coordinate is now `0.1.35` at protocol 7 / revision 40. Added the protocol-7 Hello `required_features` / `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` / `for_webrtc_terminal_subscription_closed()` contract before Hub sends `daemon_event`. Removed the stale claim that `0.1.33` is the prior coordinate at protocol 7 / revision 38.

Pipeline handoff:

- `docs/plans/publish-hub-test-support-daemon-event-close-negotiation.md` — approved revision 2 plan (Plan step)
- `docs/reports/publish-hub-test-support-daemon-event-close-negotiation-implement-report.md` — this report
- `docs/reports/publish-hub-test-support-daemon-event-close-negotiation-release-evidence.json` — coordinate, integrity, and SHA evidence

Not changed: `packages/hub-test-support/package.json`, `metadata.json`, `test.mjs`, Rust adapter/daemon/emitter files. Occupied `0.1.36` did not exist, so no version reallocation was required. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passed on the publish tree.

## Ownership boundaries preserved

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host control DTOs and npm `@trybotster/hub-test-support` | botster-hub | published site 3 |
| Generated TypeScript emitter | botster-hub-client inside this repo | consumed already-synced bytes |
| WebRTC close-event runtime emission | botster-hub runtime | closed parent `ticket_1786724303_284888`; not absorbed |
| Web pin and vendored protocol | botster-web `tgt_40abcf71ccf049f4ac0c99953a799869` | downstream `ticket_1786661008_897067`; already registered |

No Rust adapter, daemon, or emitter files were edited. Hub remains content-blind. Terminal-body opacity is unchanged.

## Cross-repo dependencies or separately routed work

No new cross-repo dependency was required. Downstream Web ticket `ticket_1786661008_897067` already depends on this Hub ticket. After this run merges, that consumer can pin `@trybotster/hub-test-support@0.1.36`.

Parent runtime work stays out of this run:

- Hub `ticket_1786724303_284888` — emit `TerminalSubscriptionClosed` on WebRTC after adapter close (closed and merged)

## Deviations from plan

Publish authentication path only. `script/publish-npm-packages --dry-run` ran from clean SHA `924ea97` and packed the intended tarball. The live `script/publish-npm-packages` invocation packed the same tarball, skipped matching `@trybotster/ui-contract@0.3.2`, then failed at `npm publish` with `EOTP`.

`npm whoami` succeeded as `tonksthebear`. Blocking question `question_1786735791_717881` asked for an authenticator OTP. The human answer reported a successful public publish of the same coordinate and integrity. Independent `npm view` confirmed `@trybotster/hub-test-support@0.1.36` with `dist.integrity` equal to the committed tarball.

This is not a scope change. Package bytes, version, protocol, revision, and acceptance checks are unchanged. The committed plan was not rewritten.

## Published artifact

| Field | Value |
| --- | --- |
| Coordinate | `@trybotster/hub-test-support@0.1.36` |
| Registry latest after publish | `0.1.36` |
| `dist.integrity` | `sha512-ar867LILkxUEbbZeoJb4Tcw8iMFIDURCMY54z0r28DTxZh+FSkmYNx+Zt3eRzcX+hB+iOchsd1n1PMFUg9bpCA==` |
| `dist.tarball` | `https://registry.npmjs.org/@trybotster/hub-test-support/-/hub-test-support-0.1.36.tgz` |
| Hub SHA packed and published | `924ea9790ec42c1ea2ed18622bcdbd489a532c34` |
| `protocol_version` | 7 |
| `conformance_fixture_revision` | 41 |
| UI contract | `@trybotster/ui-contract@0.3.2` |
| ui-contract published integrity | `sha512-lWzx8j2Z+OQhRAhURWpqUSLQyAMFycOOV6MgFAfMHRgU/27bkyc0v95komBGdJjv5IWjMRWJHlgmX7GlXGWR0Q==` |

`script/publish-npm-packages` skipped `@trybotster/ui-contract@0.3.2` after the packed tarball integrity matched the already-published coordinate.

## Tests and downstream proof run

```sh
node packages/hub-test-support/scripts/sync-assets.mjs --check
# hub test-support package assets are current

node packages/hub-test-support/test.mjs
# hub test-support package import and fixture materialization passed

git status --porcelain
# empty after commit 924ea9790ec42c1ea2ed18622bcdbd489a532c34

script/publish-npm-packages --dry-run
# Dry run passed for @trybotster/ui-contract@0.3.2 and @trybotster/hub-test-support@0.1.36
# Packed hub-test-support integrity:
# sha512-ar867LILkxUEbbZeoJb4Tcw8iMFIDURCMY54z0r28DTxZh+FSkmYNx+Zt3eRzcX+hB+iOchsd1n1PMFUg9bpCA==

npm view @trybotster/hub-test-support versions --json --prefer-online
# latest published before publish: 0.1.35; 0.1.36 unused

npm whoami
# tonksthebear

script/publish-npm-packages
# ui-contract 0.3.2 skipped on matching integrity
# npm publish failed with EOTP
# human completed public publish of the same packed tarball

npm view @trybotster/hub-test-support version --prefer-online
# 0.1.36

npm view @trybotster/hub-test-support@0.1.36 dist.integrity --prefer-online
# sha512-ar867LILkxUEbbZeoJb4Tcw8iMFIDURCMY54z0r28DTxZh+FSkmYNx+Zt3eRzcX+hB+iOchsd1n1PMFUg9bpCA==
```

External smoke: clean temporary npm project, then
`npm install @trybotster/hub-test-support@0.1.36 --prefer-online`.

Installed-package assertions:

- `metadata.package_version === "0.1.36"`
- `metadata.protocol_version === 7`
- `metadata.conformance_fixture_revision === 41`
- `metadata.ui_contract.package_version === "0.3.2"`
- `verifyPackageAssets()` succeeded
- `readDaemonProtocolTypescript()` contains `DaemonLocalWebrtcDeliveryKind` and `| "daemon_event"`
- installed package files contain `terminal_subscription_closed`
- installed `first-party-client-support-matrix.json` lists `terminal_subscription_closed` under `supported_features` and not under `required_features`
- installed README contains `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` and `for_webrtc_terminal_subscription_closed`
- installed README states that protocol-7 Hello must require `terminal_subscription_closed` before Hub sends `daemon_event`
- installed README names `0.1.35` as the prior published coordinate at protocol 7 / revision 40
- installed README does not claim `0.1.33` is the prior coordinate at the same protocol and revision

Did not run Hub `./test.sh`. No Rust or daemon source changed.

## Unverified behavior or residual risk

- `0.1.36` is now immutable. Recovery from a later content defect requires a new unused patch.
- Downstream Web still has to pin and vendor this coordinate. That remains `ticket_1786661008_897067`.
- This ticket did not re-prove live WebRTC `TerminalSubscriptionClosed` emission. That remains closed parent `ticket_1786724303_284888`.
- The live `npm publish` step required human OTP. Registry `npm view` after that answer matched the committed tarball, so the published bytes are verified even though this session did not complete the OTP itself.
- Registry visibility was confirmed immediately after the human publish answer. Later CDN replica lag is not expected after `npm view` already returned the coordinate.

## Missing vault guidance discovered

None required to implement this ticket. The four-site chain, unpublished-version cutover, protocol-7 Hello gate, README-as-contract, and external-smoke notes already covered the work.

Durable capture after publish: inbox note `daemon event close negotiation cutover uses Hub test support version 0.1.36`.
