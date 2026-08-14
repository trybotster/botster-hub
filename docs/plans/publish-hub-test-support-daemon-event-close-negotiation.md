# Plan: publish hub-test-support with daemon_event close negotiation

## Target repository and target_id

| Field | Value |
| --- | --- |
| Target repository | `trybotster/botster-hub` |
| Target id | `tgt_7e208a0c76a44980a83b63af976b1f22` |
| Spawn target name | `botster-hub` |
| Ticket | `ticket_1786730686_674642` |
| Run | `run_1786734357_656172` |
| Pipeline | `botster_stack_delivery` / step `botster_stack_plan` |
| Authoritative HEAD at plan time | `b11ff3d0aeac5d5a46ea069067be7d945a0f8798` (`origin/main`) |
| Worktree | pipeline-provided ticket worktree, currently equal to `origin/main` |

Resolved from `project_pipelines_current_context` plus `list_spawn_targets`. Not inferred from the ambient process directory.

Parent `ticket_1786724303_284888` is closed and merged. Its runtime commits are already on this HEAD:

- `5bfa308` Emit TerminalSubscriptionClosed on WebRTC after adapter close
- `6591794` Record the WebRTC close-event implement commit SHA
- `b11ff3d` Fix hub-test-support revision 41 asserts and report path PII

## Repository playbook loaded

- [[botster-hub-playbook]] — ownership charter for `botster-hub`

## Other role/surface playbooks and atomic notes loaded

### Role and stack entrypoints

- [[planner-playbook]]
- [[botster-planner-playbook]]
- [[botster playbooks compose role with changed surface overlays]] — this ticket is a packages/release surface, not a runtime or web surface
- [[botster-architecture]]
- [[cli-patterns]]
- [[spa-patterns]] — loaded as planner overlay; no SPA code changes in this ticket
- [[project pipeline orchestration belongs in a device-level botster plugin]]
- [[project pipelines needs an operator workbench not more primitives]]
- [[project pipelines ui contract belongs in the plugin readme]]
- [[botster orchestration should spawn agents with explicit target ids]]
- [[botster orchestration prompts must bind agents to explicit worktrees]]

`[[project-pipelines-playbook]]` is not loaded. This ticket does not change Project Pipelines package or plugin paths.

`[[botster runtime teardown lenses]]` was read only to decide class membership. `teardown_class_applies`: no. This ticket publishes already-merged host-plane tokens. It does not change WebRTC peer lifecycle, SessionIo/ClientWorker teardown, multi-peer ownership, resource spin, or adapter close policy. The ticket forbids adapter close policy changes. Parent `ticket_1786724303_284888` already owns the runtime emission path.

### Targeted atomic notes

- [[hub generated protocol changes are a four site release chain]] — this ticket is site 3
- [[closed dependency tickets signal merged source not a consumable release]]
- [[hub test support npm releases need external consumer smoke]]
- [[Hub test support capability cutovers use a new unpublished package version]]
- [[WebRTC adapter host DTO cutover uses Hub test support version 0.1.35]] — published sibling coordinate, protocol 7 / revision 40, no `daemon_event`
- [[Protocol 7 gates WebRTC daemon events on close-event Hello negotiation]]
- [[WebRTC host events use unsolicited daemon-event delivery]]
- [[additive daemon capabilities do not raise the default client requirement]]
- [[adding a hub client feature constant is a three site change]] — superseded shared-list history; current rule is the additive split
- [[conformance fixture revisions must be unique per published content]]
- [[daemon event shape changes bump conformance fixture revision not protocol version]]
- [[published fixture readmes are part of the shipped contract]]
- [[botster first party client support matrices belong in hub test support]]
- [[published capability matrices must derive enumerations from source]]
- [[public protocol versions host control and Core terminal planes independently]]
- [[generated typescript dtos must encode serde field optionality]]
- [[botster hub client crate is the external client boundary]]
- [[botster hub is a first party host profile over core]]
- [[cross repo dependency registration must use dependency repo target]]
- [[vault example paths are not repository placement conventions]]
- [[plan agents must author vault context as wikilinks not home paths]]
- [[pipeline vault checklists must cite exact resolvable note titles]]
- [[plan steps need reviewable plan artifacts]]
- [[plan review must verify a plan artifact exists before trusting gate summaries]]
- [[implementation steps must persist report artifacts for review]]
- [[pipeline artifacts should use path neutral worktree references]]
- [[project pipelines mcp create calls can time out after committing]]

This ticket is not a consumer of Hub session-type eligibility work. Do not inject parent session-type pins.

## Context loaded

- Project: Terminal Transport North Star · Hub Close Negotiation Package. Direct-merge. Do not create a pull request.
- Authoritative spawn target `tgt_7e208a0c76a44980a83b63af976b1f22` is `trybotster/botster-hub`.
- Assigned worktree is at `origin/main` `b11ff3d`. Tracked `.gitignore` is present and non-empty. The worktree path has no colon.
- Hub source `packages/hub-test-support` is already `@trybotster/hub-test-support@0.1.36`, protocol 7, conformance fixture revision 41, ui-contract `0.3.2`.
- `node packages/hub-test-support/scripts/sync-assets.mjs --check` passed on that tree.
- Public npm latest at Plan time is `0.1.35` (protocol 7, revision 40, `dist.integrity` `sha512-KIUC4JKaeZHGtUz6wq0P1vS1ByepFShIETVdiSDvvVeYpBbLihCCb41I4ptRmDLMu3nmRqWIMQYCLcA59SXKOQ==`). Version `0.1.36` is unpublished. Re-check at Implement time after packing the intended tree.
- Published `0.1.35` `daemon-protocol.ts` has `DaemonLocalWebrtcDeliveryKind` and `daemon_terminal_frame`. It does not contain `daemon_event`.
- Source `0.1.36` `daemon-protocol.ts` has `DaemonLocalWebrtcDeliveryKind` including `daemon_event`, and `DaemonEvent` variant `{ type: "terminal_subscription_closed"; ... }`.
- Source `first-party-client-support-matrix.json` lists optional `terminal_subscription_closed` under `supported_features`, not `required_features`. That matches [[additive daemon capabilities do not raise the default client requirement]] and [[Protocol 7 gates WebRTC daemon events on close-event Hello negotiation]].
- Rust already owns `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` and `DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed()`. The TypeScript emitter does not export those helper names. The shipped README mentions negotiated `daemon_event` close delivery but does not name `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` or the Hello `required_features` gate.
- Shipped README still says `0.1.33` is the prior published coordinate at protocol 7 / revision 38. The actual prior published coordinate is now `0.1.35` at protocol 7 / revision 40. That sentence is part of the published contract.
- `npm whoami` returned `tonksthebear` in this Plan visit. Re-check at Implement time. If publish is unauthorized, ask a human. Do not invent a file, git, or `/tmp` coordinate.
- Downstream Web ticket `ticket_1786661008_897067` already depends on this ticket (`dependency_1786730694_799927`) against this Hub target. Do not edit `botster-web`.
- Parent runtime ticket `ticket_1786724303_284888` is closed. Sibling publish ticket `ticket_1786723348_522242` is closed and published `0.1.35` without `daemon_event`.
- Vault checklist `checklist_1786734746_785303` is the one Plan-visit checklist. Create-vault-checklist timed out after commit; the owner list was read before any retry.

## Scope

Publish a new unused `@trybotster/hub-test-support` coordinate from the already-synced Hub main assets plus the required README contract corrections.

Preferred coordinate: `0.1.36`, only when that version is unused or already equals the corrected tarball.

1. Build the complete intended publish tree first.
   - Correct the shipped README sentence that still calls `0.1.33` the prior published coordinate at protocol 7 / revision 38. The prior published coordinate is `0.1.35` at protocol 7 / revision 40. This tree is revision 41.
   - Document the negotiated-close admission contract in the shipped README: protocol 7 Hello must require `terminal_subscription_closed` (`FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` / `DaemonCompatibilityRequirement::for_webrtc_terminal_subscription_closed()`) before Hub sends `DaemonLocalWebrtcDeliveryKind` `daemon_event`. Keep the feature optional in default `required_features`.
2. Re-run package tests on that corrected tree. Do not make the occupancy decision against the stale-README bytes.
3. Pack the corrected tree. Record that local tarball integrity. Use `script/publish-npm-packages --dry-run` or an equivalent pack of the same tree.
4. Then re-check registry occupancy.
5. If `0.1.36` is unused, publish the corrected tarball as `0.1.36`.
6. If `0.1.36` is occupied and `dist.integrity` equals the corrected tarball, treat publication as done. Still run the external content smoke, including the README contract checks.
7. If `0.1.36` is occupied and `dist.integrity` differs from the corrected tarball, allocate the next unused patch (`0.1.37` or later). Bump `package.json`, regenerate `metadata.json`, update README pin sites and `test.mjs`, pack again, and publish that unused coordinate. A stale-README `0.1.36` is a mismatch. Do not accept it as done.
8. Keep `protocol_version` 7 and `@trybotster/ui-contract@0.3.2`. Keep `CONFORMANCE_FIXTURE_REVISION` at the parent-merge value `41` unless a later main commit allocated a newer unused revision. Do not reuse published revision 40 bytes.
9. Publish with `script/publish-npm-packages`. Do not skip an already-published coordinate on version-exists alone.
10. If `npm whoami` fails, ask a blocking human. Do not invent a file, git, or `/tmp` coordinate.
11. After publish, prove a clean external install of the registry coordinate.
12. Persist a report under `docs/reports/` with coordinate, integrity, Hub SHA, and smoke evidence.
13. Merge directly into `main`. Do not create a pull request.

## Non-scope

- Do not change terminal adapter close policy, admission, grants, framing, encryption, or teardown.
- Do not inspect READY, PAGE, FINISH, Snapshot, or other terminal bodies.
- Do not edit `botster-web`, `botster-tui`, `botster-core`, or `@trybotster/terminal-protocol`.
- Do not republish or mutate `0.1.35` or any other published version.
- Do not bump `PROTOCOL_VERSION`.
- Do not change `@trybotster/ui-contract` away from `0.3.2`.
- Do not move `terminal_subscription_closed` into default `required_features`.
- Do not emit or re-prove WebRTC `TerminalSubscriptionClosed` runtime behavior. That is closed parent `ticket_1786724303_284888`.
- Do not vendor or pin this coordinate in `botster-web`. That is downstream `ticket_1786661008_897067` site 4.

## Repository ownership boundaries and cross-repo dependencies

| Boundary | Owner | This ticket |
| --- | --- | --- |
| Host control DTOs and npm `@trybotster/hub-test-support` | botster-hub | publish site 3 |
| Generated TypeScript emitter | botster-hub-client inside this repo | consume already-synced bytes |
| WebRTC close-event runtime emission | botster-hub runtime | closed parent `ticket_1786724303_284888`; do not absorb |
| Web pin and vendored protocol | botster-web `tgt_40abcf71ccf049f4ac0c99953a799869` | downstream `ticket_1786661008_897067`; already registered |

No new cross-repo dependency is required on this ticket. The Web consumer already depends on this Hub target. Do not register a dependency against this ticket's target for Web-owned site 4 work.

## Assumptions and unknowns

- Assumption: `0.1.36` on `origin/main` is the intended publish tree. Sites 1 and 2 of the four-site chain already landed with parent merge.
- Assumption: the ticket phrase `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` is the Rust feature constant whose wire token is `terminal_subscription_closed`. The npm package does not currently export the Rust identifier. Shipping both the wire token and the documented constant/helper names in the README satisfies the tarball and Hello-contract acceptance checks without changing the TypeScript emitter.
- Assumption: `for_webrtc_terminal_subscription_closed()` is the helper the ticket allows documenting. Adding that name to generated TypeScript would be a site-1/2 emitter change and is out of scope.
- Assumption: public npm remains the approved distribution path.
- Assumption: parent merge revision 41 remains the allocated unused revision. If Implement finds a newer published meaning of 41, stop and allocate above every published meaning.
- Unknown: whether `0.1.36` remains unpublished at Implement time. Re-read the registry only after packing the corrected tree.
- Unknown: npm authentication at Implement time. Plan observed `tonksthebear`. Implement must ask a human if publish is unauthorized.

## Affected surfaces/files

Botster layers touched: Hub-owned Node package distribution and release docs.

Likely files:

- `packages/hub-test-support/README.md` — prior-coordinate sentence plus Hello `required_features` / `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` / `for_webrtc_terminal_subscription_closed()` contract sentence
- `docs/plans/publish-hub-test-support-daemon-event-close-negotiation.md` — this plan
- `docs/reports/publish-hub-test-support-daemon-event-close-negotiation-implement-report.md` — Implement report
- optional evidence JSON beside that report

Touch these files only if occupied `0.1.36` differs from the corrected tarball:

- `packages/hub-test-support/package.json`
- `packages/hub-test-support/metadata.json`
- `packages/hub-test-support/test.mjs`

Do not touch Rust adapter, daemon, or emitter files unless `node packages/hub-test-support/scripts/sync-assets.mjs --check` fails. If that check fails, stop and ask a human. This ticket is not a source-sync ticket.

## Risks

- Publishing without an external content smoke can ship a self-consistent stale tarball. Mitigation: clean-dir install and token grep after registry visibility.
- Skipping because the version exists can hide a bad publish. Mitigation: integrity compare against the corrected tarball only, then extra unused version on mismatch.
- npm auth can block publish. Mitigation: blocking human question. No unapproved fallback coordinate.
- Another publisher can occupy `0.1.36` with stale-README bytes between Plan and Implement. Mitigation: always pack the README-corrected tree first. Treat any integrity mismatch, including a stale-README `0.1.36`, as a new unused patch.
- A later main commit can change package bytes before publish. Mitigation: publish from a recorded SHA and pack that tree.
- README-only contract documentation can miss the ticket's `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` grep. Mitigation: the shipped README must contain `daemon_event`, `terminal_subscription_closed`, `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED`, and `for_webrtc_terminal_subscription_closed`.
- Moving the feature into default `required_features` would break old protocol-7 clients. Mitigation: keep it optional in the matrix and document the request-specific Hello requirement only.

## Acceptance checks/tests

`teardown_class_applies`: no.

Production path: botster-web and other external clients install the public npm coordinate. Local Hub source is not the consumable artifact. This ticket proves site 3 only. Site 4 remains the already-registered Web ticket.

1. `node packages/hub-test-support/scripts/sync-assets.mjs --check` passes on the publish tree.
2. `node packages/hub-test-support/test.mjs` passes.
3. Occupancy is decided only after the README-corrected tree is packed. Registry `0.1.36` is accepted only when its `dist.integrity` equals that corrected tarball.
4. `script/publish-npm-packages --dry-run` packs `@trybotster/hub-test-support` at the chosen unused version, or confirms occupied `0.1.36` matches the corrected tarball, and packs `@trybotster/ui-contract@0.3.2` with matching integrity if that ui-contract version is already published.
5. `npm view @trybotster/hub-test-support version` reports a version newer than any `0.1.35` published by sibling `ticket_1786723348_522242`.
6. `npm view @trybotster/hub-test-support@<published>` reports `dist.integrity` equal to the locally packed corrected tarball.
7. A clean temporary directory can `npm install @trybotster/hub-test-support@<published> --prefer-online` and then:
   - `metadata.package_version` equals the published version
   - `metadata.protocol_version === 7`
   - `metadata.conformance_fixture_revision === 41`
   - `metadata.ui_contract.package_version === "0.3.2"`
   - `verifyPackageAssets()` succeeds
   - `readDaemonProtocolTypescript()` contains `DaemonLocalWebrtcDeliveryKind` and `| "daemon_event"`
   - installed package files contain `terminal_subscription_closed`
   - installed `first-party-client-support-matrix.json` lists `terminal_subscription_closed` under `supported_features` and not under `required_features`
   - installed README contains `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` and `for_webrtc_terminal_subscription_closed`
   - installed README states that protocol-7 Hello must require `terminal_subscription_closed` before Hub sends `daemon_event`
   - installed README names `0.1.35` as the prior published coordinate at protocol 7 / revision 40 and does not claim `0.1.33` is the prior coordinate at the same protocol and revision
8. Do not require a full Hub `./test.sh` unless Implement changes Rust or daemon source.
9. Merge the plan, report, and any README or version files directly into `main`. Do not open a pull request.

## Vault gaps worth capturing

- None required before publish. The four-site chain, unpublished-version cutover, protocol-7 Hello gate, and external-smoke rules already cover this shape.
- After Implement records the published coordinate, integrity, and Hub SHA, capture that `@trybotster/hub-test-support@<published>` / revision 41 is the first published `daemon_event` close-negotiation coordinate. Do not capture that coordinate before publish.
- Optional later amendment to [[WebRTC adapter host DTO cutover uses Hub test support version 0.1.35]]: keep `0.1.35` as the host-DTO baseline without `daemon_event`, and point the close-negotiation coordinate at the new note.

## Implement sequence

1. Confirm the assigned worktree is based on current `origin/main`. Restore tracked `.gitignore` from HEAD if it is empty. Set a colon-free `CARGO_TARGET_DIR` only if the path contains `:`.
2. Confirm `node packages/hub-test-support/scripts/sync-assets.mjs --check` still passes. If it fails, stop and ask a human.
3. Edit the shipped README first: prior coordinate `0.1.35` / protocol 7 / revision 40, plus the Hello `required_features` / `FEATURE_TERMINAL_SUBSCRIPTION_CLOSED` / `for_webrtc_terminal_subscription_closed()` contract sentence.
4. Run `node packages/hub-test-support/test.mjs`.
5. Pack the corrected tree and record tarball integrity.
6. Re-read `npm view @trybotster/hub-test-support versions` and `0.1.36` occupancy only after that pack.
7. Publish the unused matching coordinate with `script/publish-npm-packages`.
8. Prove the clean external install checks above.
9. Write `docs/reports/publish-hub-test-support-daemon-event-close-negotiation-implement-report.md` and optional evidence JSON.
10. Commit on the ticket branch and merge directly to `main`. Do not open a pull request.
